"""
Weather edge bot for Kalshi. Uses Open-Meteo forecasts to find mispriced
weather markets (temperature, precip, snow). Buys when forecast-implied
probability diverges significantly from market price.
"""

import os, time, logging, traceback, math, re
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template_string
from threading import Thread
import psycopg2
from psycopg2.extras import RealDictCursor
from kalshi_auth import KalshiAuth
from weather import (
    fetch_all_forecasts, estimate_prob_above, estimate_prob_below,
    estimate_precip_prob, estimate_snow_prob, CITIES,
)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIG ===
KALSHI_HOST = os.environ.get('KALSHI_API_HOST', 'https://api.elections.kalshi.com')
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://kalshi:kalshi@localhost:5432/kalshi_weather')
PORT = int(os.environ.get('PORT', 8080))
ENABLE_TRADING = os.environ.get('ENABLE_TRADING', 'false').lower() == 'true'

# === STRATEGY ===
MIN_EDGE = 0.15          # need 15% edge (forecast prob - market price) to buy
MAX_PRICE = 0.85         # don't buy above $0.85
MIN_PRICE = 0.05         # don't buy below $0.05
CONTRACTS = 1             # contracts per trade
MAX_POSITIONS = 50        # max total open positions
MAX_PER_MARKET = 3        # max positions on same market
CYCLE_SECONDS = 60        # weather markets move slower than crypto
STARTING_BALANCE = 10000.00
CASH_RESERVE = 0.40       # keep 40% cash
TAKER_FEE_RATE = 0.07
SELL_PROFIT_PCT = 0.50    # take profit at +50%

# Weather series on Kalshi — high temp markets
# Format: series_prefix -> city_key (matches weather.py CITIES)
WEATHER_SERIES = {
    'KXHIGHNY': 'NYC',
    'KXHIGHCHI': 'CHI',
    'KXHIGHLA': 'LA',
    'KXHIGHMIA': 'MIA',
    'KXHIGHDEN': 'DEN',
    'KXHIGHATL': 'ATL',
    'KXHIGHDAL': 'DAL',
    'KXHIGHSEA': 'SEA',
    'KXHIGHPHX': 'PHX',
    'KXHIGHDCA': 'DCA',
}

# Also track precip/snow series
PRECIP_SERIES = {
    'KXRAINY': 'NYC',
    'KXRAINCHI': 'CHI',
    'KXRAINLA': 'LA',
}

SNOW_SERIES = {
    'KXSNOWNY': 'NYC',
    'KXSNOWCHI': 'CHI',
}

ALL_SERIES = list(WEATHER_SERIES.keys()) + list(PRECIP_SERIES.keys()) + list(SNOW_SERIES.keys())

# === DATABASE ===

def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT,
                    side TEXT,
                    action TEXT,
                    price NUMERIC,
                    count INTEGER,
                    current_bid NUMERIC,
                    pnl NUMERIC,
                    series TEXT,
                    market_type TEXT,
                    city TEXT,
                    threshold NUMERIC,
                    forecast_value NUMERIC,
                    forecast_prob NUMERIC,
                    edge NUMERIC,
                    target_date TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # Add columns if missing (for existing tables)
            for col, typ in [
                ('market_type', 'TEXT'), ('city', 'TEXT'), ('threshold', 'NUMERIC'),
                ('forecast_value', 'NUMERIC'), ('forecast_prob', 'NUMERIC'),
                ('edge', 'NUMERIC'), ('target_date', 'TEXT'),
            ]:
                try:
                    cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
                except:
                    pass

            cur.execute("""
                CREATE TABLE IF NOT EXISTS forecasts (
                    id SERIAL PRIMARY KEY,
                    city TEXT,
                    target_date TEXT,
                    high_f NUMERIC,
                    low_f NUMERIC,
                    precip_mm NUMERIC,
                    snow_cm NUMERIC,
                    wind_mph NUMERIC,
                    fetched_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
    finally:
        conn.close()


# === INIT ===
init_db()
auth = KalshiAuth()
app = Flask(__name__)

current_forecasts = {}
current_hot_markets = []


def sf(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0


def kalshi_fee(price, count):
    """Kalshi taker fee: 7% of P*(1-P) per contract, max $0.02/contract."""
    return min(math.ceil(TAKER_FEE_RATE * count * price * (1 - price) * 100) / 100, 0.02 * count)


# === KALSHI API ===

def _make_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

session = _make_session()


def kalshi_get(path):
    url = f"{KALSHI_HOST}/trade-api/v2{path}"
    headers = auth.get_headers("GET", f"/trade-api/v2{path}")
    resp = session.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def kalshi_post(path, data):
    url = f"{KALSHI_HOST}/trade-api/v2{path}"
    headers = auth.get_headers("POST", f"/trade-api/v2{path}")
    headers['Content-Type'] = 'application/json'
    resp = session.post(url, headers=headers, json=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_market(ticker):
    try:
        resp = kalshi_get(f"/markets/{ticker}")
        return resp.get('market', resp)
    except:
        return None


def place_order(ticker, side, action, price, count):
    if not ENABLE_TRADING:
        logger.info(f"PAPER {action.upper()}: {ticker} {side} x{count} @ ${price:.2f}")
        return ('paper', count)

    price_cents = int(round(price * 100))
    try:
        resp = kalshi_post('/portfolio/orders', {
            'ticker': ticker, 'action': action, 'side': side,
            'type': 'limit', 'count': count,
            'yes_price' if side == 'yes' else 'no_price': price_cents,
        })
        order = resp.get('order', {})
        order_id = order.get('order_id', '')
        status = order.get('status', '')
        filled = order.get('place_count', 0) - order.get('remaining_count', 0)
        if filled <= 0:
            filled = count if status in ('executed', 'filled') else 0
        logger.info(f"ORDER {action.upper()}: {ticker} status={status} filled={filled}/{count} id={order_id}")
        return (order_id, filled) if filled > 0 else None
    except Exception as e:
        logger.error(f"ORDER FAILED: {action.upper()} {ticker} -- {e}")
        return None


# === BALANCE ===

def get_kalshi_balance():
    try:
        resp = kalshi_get('/portfolio/balance')
        return resp.get('balance', 0) / 100.0
    except Exception as e:
        logger.error(f"Kalshi balance fetch failed: {e}")
        return None


def get_balance():
    if ENABLE_TRADING:
        real = get_kalshi_balance()
        if real is not None:
            return real
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT price, count FROM trades WHERE action = 'buy'")
            buys = cur.fetchall()
            buy_cost = sum(sf(t['price']) * (t.get('count') or 1) for t in buys)
            cur.execute("SELECT pnl FROM trades WHERE pnl IS NOT NULL")
            pnl_data = cur.fetchall()
            total_pnl = sum(sf(t['pnl']) for t in pnl_data)
            return max(0, STARTING_BALANCE - buy_cost + total_pnl)
    except Exception as e:
        logger.error(f"Balance calc failed: {e}")
        return 0.0
    finally:
        conn.close()


def get_open_positions():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM trades WHERE action = 'buy' AND pnl IS NULL")
            return cur.fetchall()
    except Exception as e:
        logger.error(f"get_open_positions failed: {e}")
        return []
    finally:
        conn.close()


# === MARKET PARSING ===

def parse_weather_market(ticker, subtitle):
    """Parse a weather market ticker/subtitle to extract threshold and type.

    Returns (market_type, city_key, threshold, comparison) or None.
    market_type: 'high_temp', 'precip', 'snow'
    comparison: 'above' or 'below'
    """
    # Try high temp series
    for series_prefix, city_key in WEATHER_SERIES.items():
        if ticker.startswith(series_prefix):
            # Extract threshold from ticker or subtitle
            # Tickers like KXHIGHNY-25MAR28-T55 (temp >= 55F)
            match = re.search(r'T(\d+)', ticker)
            if match:
                threshold = int(match.group(1))
                return ('high_temp', city_key, threshold, 'above')

            # Try subtitle parsing
            match = re.search(r'(\d+)', subtitle or '')
            if match:
                threshold = int(match.group(1))
                # Determine direction from subtitle
                if 'above' in (subtitle or '').lower() or 'higher' in (subtitle or '').lower() or '>=' in (subtitle or ''):
                    return ('high_temp', city_key, threshold, 'above')
                elif 'below' in (subtitle or '').lower() or 'lower' in (subtitle or '').lower() or '<' in (subtitle or ''):
                    return ('high_temp', city_key, threshold, 'below')
                else:
                    return ('high_temp', city_key, threshold, 'above')

    # Try precip series
    for series_prefix, city_key in PRECIP_SERIES.items():
        if ticker.startswith(series_prefix):
            return ('precip', city_key, 0.01, 'above')

    # Try snow series
    for series_prefix, city_key in SNOW_SERIES.items():
        if ticker.startswith(series_prefix):
            return ('snow', city_key, 0.1, 'above')

    return None


def extract_target_date(market):
    """Extract the target date for a weather market from close_time or ticker."""
    close_time = market.get('close_time') or market.get('expected_expiration_time', '')
    if close_time:
        try:
            dt = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d'), dt
        except:
            pass

    # Try from ticker: KXHIGHNY-25MAR28-T55
    match = re.search(r'-(\d{2})([A-Z]{3})(\d{2})-', market.get('ticker', ''))
    if match:
        day, mon, yr = match.groups()
        months = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                  'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
        if mon in months:
            dt = datetime(2000 + int(yr), months[mon], int(day), tzinfo=timezone.utc)
            return dt.strftime('%Y-%m-%d'), dt

    return None, None


def get_forecast_for_market(city_key, target_date, market_type):
    """Look up forecast data for a specific city and date."""
    if city_key not in current_forecasts:
        return None

    for day in current_forecasts[city_key]:
        if day['date'] == target_date:
            return day

    return None


def compute_edge(market_type, comparison, forecast_day, threshold, days_out):
    """Compute our probability estimate and the edge vs market.

    Returns (our_prob, forecast_value) where our_prob is for 'yes' outcome.
    """
    if not forecast_day:
        return None, None

    if market_type == 'high_temp':
        forecast_val = forecast_day.get('high_f')
        if forecast_val is None:
            return None, None

        if comparison == 'above':
            prob = estimate_prob_above(forecast_val, threshold, days_out)
        else:
            prob = estimate_prob_below(forecast_val, threshold, days_out)
        return prob, forecast_val

    elif market_type == 'precip':
        forecast_val = forecast_day.get('precip_mm', 0)
        prob = estimate_precip_prob(forecast_val, threshold)
        return prob, forecast_val

    elif market_type == 'snow':
        forecast_val = forecast_day.get('snow_cm', 0)
        prob = estimate_snow_prob(forecast_val, threshold)
        return prob, forecast_val

    return None, None


# === SELL LOGIC ===

def check_sells():
    logger.info("--- SELL CHECK ---")
    open_positions = get_open_positions()

    if not open_positions:
        logger.info("No open positions")
        return

    logger.info(f"Checking {len(open_positions)} open positions")
    sold = 0
    expired = 0

    for trade in open_positions:
        ticker = trade['ticker']
        side = trade['side']
        entry_price = sf(trade['price'])
        count = trade.get('count') or 1

        if entry_price <= 0:
            continue

        market = get_market(ticker)
        if not market:
            continue

        status = market.get('status', '')
        result_val = market.get('result', '')

        # === SETTLED ===
        if result_val:
            buy_fee = kalshi_fee(entry_price, count)
            if result_val == side:
                pnl = round((1.0 - entry_price) * count - buy_fee, 4)
                reason = "WIN settled @$1.00"
            else:
                pnl = round(-entry_price * count - buy_fee, 4)
                reason = "LOSS settled"
            logger.info(f"SETTLED: {ticker} {side} | {reason} | pnl=${pnl:.4f}")
            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET pnl = %s WHERE id = %s", (float(pnl), trade['id']))
            except Exception as e:
                logger.error(f"Settle DB error: {e}")
            finally:
                conn.close()
            expired += 1
            continue

        # === CLOSED but no result ===
        if status in ('closed', 'settled', 'finalized'):
            logger.info(f"WAITING: {ticker} status={status}, no result yet")
            continue

        # Get current bid
        if side == 'yes':
            current_bid = sf(market.get('yes_bid_dollars', '0'))
        else:
            current_bid = sf(market.get('no_bid_dollars', '0'))

        # Update bid in DB
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE trades SET current_bid = %s WHERE id = %s", (float(current_bid), trade['id']))
        except:
            pass
        finally:
            conn.close()

        if current_bid <= 0:
            continue

        gain = (current_bid - entry_price) / entry_price

        logger.info(f"  POS: {ticker} {side} entry=${entry_price:.2f} bid=${current_bid:.2f} {gain*100:+.0f}% x{count}")

        # Take profit
        if gain >= SELL_PROFIT_PCT:
            buy_fee = kalshi_fee(entry_price, count)
            sell_fee = kalshi_fee(current_bid, count)
            gross = round((current_bid - entry_price) * count, 4)
            pnl = round(gross - buy_fee - sell_fee, 4)

            logger.info(f"SELL: {ticker} {side} x{count} @ ${current_bid:.2f} | pnl=${pnl:.4f}")

            result = place_order(ticker, side, 'sell', current_bid, count)
            if not result:
                continue

            order_id, filled = result
            if filled < count:
                pnl = round((current_bid - entry_price) * filled - kalshi_fee(entry_price, filled) - kalshi_fee(current_bid, filled), 4)

            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET pnl = %s, current_bid = %s WHERE id = %s",
                                (float(pnl), float(current_bid), trade['id']))
            except Exception as e:
                logger.error(f"Sell DB error: {e}")
            finally:
                conn.close()
            sold += 1

    logger.info(f"SELL SUMMARY: sold={sold} expired={expired}")


# === BUY LOGIC ===

def fetch_weather_markets():
    """Fetch all open weather markets from Kalshi."""
    all_markets = []
    for series in ALL_SERIES:
        cursor = None
        try:
            while True:
                url = f'/markets?series_ticker={series}&status=open&limit=200'
                if cursor:
                    url += f'&cursor={cursor}'
                resp = kalshi_get(url)
                batch = resp.get('markets', [])
                all_markets.extend(batch)
                cursor = resp.get('cursor')
                if not cursor or not batch:
                    break
        except Exception as e:
            logger.error(f"Fetch {series} failed: {e}")
    logger.info(f"Fetched {len(all_markets)} weather markets from {len(ALL_SERIES)} series")
    return all_markets


def buy_candidates(markets):
    """Find and buy mispriced weather markets using forecast edge."""
    balance = get_balance()
    open_positions = get_open_positions()
    logger.info(f"Balance: ${balance:.2f} | {len(open_positions)} positions open")

    if len(open_positions) >= MAX_POSITIONS:
        logger.info(f"At max positions ({MAX_POSITIONS}), skipping buys")
        return

    deployable = balance * (1.0 - CASH_RESERVE)
    if deployable <= 1.0:
        logger.info(f"Deployable ${deployable:.2f} too low")
        return

    now = datetime.now(timezone.utc)
    today = now.strftime('%Y-%m-%d')

    # Count positions per ticker
    ticker_counts = {}
    for t in open_positions:
        tk = t.get('ticker', '')
        ticker_counts[tk] = ticker_counts.get(tk, 0) + 1

    candidates = []

    for market in markets:
        ticker = market.get('ticker', '')
        subtitle = market.get('subtitle', '') or market.get('title', '')

        # Parse market type
        parsed = parse_weather_market(ticker, subtitle)
        if not parsed:
            continue

        market_type, city_key, threshold, comparison = parsed

        # Position cap per ticker
        if ticker_counts.get(ticker, 0) >= MAX_PER_MARKET:
            continue

        # Get target date
        target_date, target_dt = extract_target_date(market)
        if not target_date or not target_dt:
            continue

        # How many days out
        days_out = max(0, (target_dt.date() - now.date()).days)
        if days_out > 6:
            continue  # forecast unreliable beyond 7 days

        # Get forecast
        forecast_day = get_forecast_for_market(city_key, target_date, market_type)
        if not forecast_day:
            continue

        # Compute our probability
        our_prob, forecast_val = compute_edge(market_type, comparison, forecast_day, threshold, days_out)
        if our_prob is None:
            continue

        # Get market prices
        yes_ask = float(market.get('yes_ask_dollars') or '999')
        no_ask = float(market.get('no_ask_dollars') or '999')

        # Decide which side to buy
        # If we think yes prob is high and yes is cheap -> buy yes
        # If we think yes prob is low (no prob is high) and no is cheap -> buy no
        best_side = None
        best_price = None
        best_edge = 0

        if MIN_PRICE <= yes_ask <= MAX_PRICE:
            edge_yes = our_prob - yes_ask  # yes_ask is like the market's yes probability
            if edge_yes >= MIN_EDGE:
                best_side = 'yes'
                best_price = yes_ask
                best_edge = edge_yes

        if MIN_PRICE <= no_ask <= MAX_PRICE:
            no_prob = 1.0 - our_prob
            edge_no = no_prob - no_ask
            if edge_no >= MIN_EDGE and edge_no > best_edge:
                best_side = 'no'
                best_price = no_ask
                best_edge = edge_no

        if not best_side:
            continue

        candidates.append({
            'ticker': ticker,
            'side': best_side,
            'price': best_price,
            'edge': best_edge,
            'our_prob': our_prob if best_side == 'yes' else 1.0 - our_prob,
            'market_type': market_type,
            'city': city_key,
            'threshold': threshold,
            'forecast_value': forecast_val,
            'target_date': target_date,
            'days_out': days_out,
        })

    # Sort by edge (highest first)
    candidates.sort(key=lambda x: x['edge'], reverse=True)
    logger.info(f"Found {len(candidates)} edge candidates (min edge {MIN_EDGE*100:.0f}%)")

    bought = 0
    for c in candidates:
        if len(open_positions) + bought >= MAX_POSITIONS:
            break

        cost = c['price'] * CONTRACTS
        if cost > deployable:
            continue

        logger.info(
            f"EDGE: {c['ticker']} {c['side']} @ ${c['price']:.2f} | "
            f"forecast={c['forecast_value']:.1f} prob={c['our_prob']:.2f} edge={c['edge']:.2f} | "
            f"{c['city']} {c['market_type']} thresh={c['threshold']} date={c['target_date']}"
        )

        result = place_order(c['ticker'], c['side'], 'buy', c['price'], CONTRACTS)
        if not result:
            continue

        order_id, filled = result
        if filled <= 0:
            continue

        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO trades
                       (ticker, side, action, price, count, current_bid, series, market_type, city,
                        threshold, forecast_value, forecast_prob, edge, target_date)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (c['ticker'], c['side'], 'buy', float(c['price']), filled, float(c['price']),
                     '', c['market_type'], c['city'], float(c['threshold']),
                     float(c['forecast_value']), float(c['our_prob']), float(c['edge']),
                     c['target_date'])
                )
        except Exception as e:
            logger.error(f"Buy DB insert failed: {e}")
        finally:
            conn.close()

        deployable -= cost
        bought += 1

    logger.info(f"Bought {bought} positions")


def update_hot_markets(markets):
    """Track most active weather markets."""
    global current_hot_markets
    active = [m for m in markets if sf(m.get('yes_ask_dollars', '0')) < 0.99]
    by_vol = sorted(active, key=lambda m: int(m.get('volume', 0) or 0), reverse=True)[:10]
    current_hot_markets = [
        {
            'ticker': m.get('ticker', ''),
            'title': (m.get('subtitle', '') or m.get('title', ''))[:60],
            'yes_ask': sf(m.get('yes_ask_dollars', '0')),
            'no_ask': sf(m.get('no_ask_dollars', '0')),
            'volume': int(m.get('volume', 0) or 0),
        }
        for m in by_vol
    ]


# === MAIN CYCLE ===

_cycle_count = 0

def run_cycle():
    global current_forecasts, _cycle_count
    _cycle_count += 1

    mode = "PAPER" if not ENABLE_TRADING else "LIVE"
    balance = get_balance()
    logger.info(f"=== CYCLE {_cycle_count} [{mode}] === Balance: ${balance:.2f}")

    # Refresh forecasts every 10 cycles (~10 min)
    if _cycle_count % 10 == 1 or not current_forecasts:
        logger.info("Fetching weather forecasts...")
        current_forecasts = fetch_all_forecasts()
        for city, days in current_forecasts.items():
            if days:
                d = days[0]
                logger.info(f"  {city}: high={d.get('high_f', '?')}F low={d.get('low_f', '?')}F precip={d.get('precip_mm', 0):.1f}mm")

        # Save forecasts to DB
        conn = get_db()
        try:
            with conn.cursor() as cur:
                for city, days in current_forecasts.items():
                    for d in days:
                        cur.execute(
                            """INSERT INTO forecasts (city, target_date, high_f, low_f, precip_mm, snow_cm, wind_mph)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                            (city, d['date'], d.get('high_f'), d.get('low_f'),
                             d.get('precip_mm', 0), d.get('snow_cm', 0), d.get('wind_mph', 0))
                        )
        except Exception as e:
            logger.error(f"Save forecasts failed: {e}")
        finally:
            conn.close()

    check_sells()
    markets = fetch_weather_markets()
    update_hot_markets(markets)
    buy_candidates(markets)

    balance = get_balance()
    logger.info(f"=== CYCLE END [{mode}] === Balance: ${balance:.2f}")


# === DASHBOARD API ===

@app.route('/')
def health():
    return 'OK'


@app.route('/api/status')
def api_status():
    try:
        cash = get_balance()
        open_positions = get_open_positions()
        positions_value = sum(sf(t.get('current_bid', 0)) * (t.get('count') or 1) for t in open_positions)
        portfolio = cash + positions_value

        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT pnl, count FROM trades WHERE action = 'buy' AND pnl IS NOT NULL")
                resolved_data = cur.fetchall()

                cur.execute("SELECT count, price FROM trades WHERE action = 'buy'")
                all_buys = cur.fetchall()
        finally:
            conn.close()

        total_pnl = sum(sf(t['pnl']) for t in resolved_data)
        wins = sum(1 for t in resolved_data if sf(t['pnl']) > 0)
        losses = sum(1 for t in resolved_data if sf(t['pnl']) <= 0)
        avg_win = round(sum(sf(t['pnl']) for t in resolved_data if sf(t['pnl']) > 0) / max(wins, 1), 4)
        avg_loss = round(sum(sf(t['pnl']) for t in resolved_data if sf(t['pnl']) <= 0) / max(losses, 1), 4)

        total_contracts = sum((t.get('count') or 1) for t in all_buys)
        total_fees = sum(kalshi_fee(sf(t.get('price')), t.get('count') or 1) for t in all_buys)

        # Unrealized P&L
        round_cost = sum(sf(t.get('price')) * (t.get('count') or 1) for t in open_positions)
        round_pnl = round(positions_value - round_cost, 4)
        round_pct = round((round_pnl / round_cost * 100), 1) if round_cost > 0 else 0

        overall_pnl = round((cash + positions_value) - STARTING_BALANCE, 2)

        mode = "PAPER" if not ENABLE_TRADING else "LIVE"

        return jsonify({
            'portfolio': round(portfolio, 2),
            'cash': round(cash, 2),
            'positions_value': round(positions_value, 2),
            'overall_pnl': overall_pnl,
            'round_pnl': round_pnl,
            'round_pct': round_pct,
            'net_pnl': round(total_pnl, 4),
            'total_fees': round(total_fees, 4),
            'total_contracts': total_contracts,
            'wins': wins,
            'losses': losses,
            'open_count': len(open_positions),
            'mode': mode,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'cities_tracked': len(current_forecasts),
        })
    except Exception as e:
        logger.error(f"API status error: {e}")
        return jsonify({'portfolio': 0, 'cash': 0, 'positions_value': 0, 'net_pnl': 0,
                        'wins': 0, 'losses': 0, 'open_count': 0, 'mode': 'PAPER'})


@app.route('/api/open')
def api_open():
    try:
        positions = []
        for t in get_open_positions():
            price = sf(t.get('price'))
            current = sf(t.get('current_bid'))
            count = int(t.get('count') or 1)
            unrealized = round((current - price) * count, 4) if price > 0 and current > 0 else 0
            gain_pct = round(((current - price) / price) * 100, 1) if price > 0 and current > 0 else 0
            positions.append({
                'ticker': t.get('ticker', ''),
                'side': t.get('side', ''),
                'city': t.get('city', ''),
                'market_type': t.get('market_type', ''),
                'threshold': sf(t.get('threshold')),
                'forecast_value': sf(t.get('forecast_value')),
                'edge': sf(t.get('edge')),
                'target_date': t.get('target_date', ''),
                'count': count,
                'entry': price,
                'current_bid': current,
                'unrealized': unrealized,
                'gain_pct': gain_pct,
            })
        positions.sort(key=lambda x: x['gain_pct'], reverse=True)
        return jsonify(positions)
    except Exception as e:
        logger.error(f"API open error: {e}")
        return jsonify([])


@app.route('/api/trades')
def api_trades():
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM trades WHERE action = 'buy' AND pnl IS NOT NULL ORDER BY created_at DESC LIMIT 50")
                result_data = cur.fetchall()
        finally:
            conn.close()
        trades = []
        for t in result_data:
            entry = sf(t.get('price'))
            exit_price = sf(t.get('current_bid'))
            gain_pct = round(((exit_price - entry) / entry) * 100, 1) if entry > 0 else 0
            trades.append({
                'created_at': str(t.get('created_at', '')),
                'ticker': t.get('ticker', ''),
                'side': t.get('side', ''),
                'city': t.get('city', ''),
                'market_type': t.get('market_type', ''),
                'edge': sf(t.get('edge')),
                'count': t.get('count', 1),
                'entry': entry,
                'exit': exit_price,
                'pnl': sf(t.get('pnl')),
                'gain_pct': gain_pct,
            })
        return jsonify(trades)
    except Exception as e:
        logger.error(f"API trades error: {e}")
        return jsonify([])


@app.route('/api/hot')
def api_hot():
    return jsonify(current_hot_markets)


@app.route('/api/forecasts')
def api_forecasts():
    """Return current forecast data for all cities."""
    result = {}
    for city_key, days in current_forecasts.items():
        city_name = CITIES.get(city_key, (0, 0, city_key))[2]
        result[city_key] = {
            'name': city_name,
            'forecasts': days[:3],  # next 3 days
        }
    return jsonify(result)


@app.route('/api/edge_history')
def api_edge_history():
    """Show historical accuracy of edge calls."""
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT city, market_type, edge, forecast_prob, pnl,
                           CASE WHEN pnl > 0 THEN 1 ELSE 0 END as won
                    FROM trades
                    WHERE action = 'buy' AND pnl IS NOT NULL AND edge IS NOT NULL
                    ORDER BY created_at DESC LIMIT 200
                """)
                trades = cur.fetchall()
        finally:
            conn.close()

        # Bucket by edge size
        buckets = {}
        for t in trades:
            edge = sf(t.get('edge'))
            if edge < 0.20:
                b = '15-20%'
            elif edge < 0.30:
                b = '20-30%'
            elif edge < 0.50:
                b = '30-50%'
            else:
                b = '50%+'
            if b not in buckets:
                buckets[b] = {'total': 0, 'wins': 0, 'pnl': 0}
            buckets[b]['total'] += 1
            buckets[b]['wins'] += int(sf(t.get('pnl')) > 0)
            buckets[b]['pnl'] += sf(t.get('pnl'))

        return jsonify({
            'buckets': {k: {**v, 'win_rate': round(v['wins'] / v['total'] * 100, 1) if v['total'] > 0 else 0}
                        for k, v in buckets.items()},
            'total_trades': len(trades),
        })
    except Exception as e:
        logger.error(f"API edge history error: {e}")
        return jsonify({'buckets': {}, 'total_trades': 0})


@app.route('/dashboard')
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Kalshi Weather Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700;800&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#06080d;--bg1:#0c1017;--bg2:#111820;--bg3:#1a2130;
  --border:#1a2235;--border2:#243050;
  --text:#c8d0e0;--text2:#6a7490;--text3:#3a4260;
  --green:#00e68a;--green2:#00cc7a;--green-bg:rgba(0,230,138,.06);
  --red:#ff4466;--red2:#ee3355;--red-bg:rgba(255,68,102,.06);
  --gold:#f0b040;--gold2:#e0a030;
  --blue:#4488ff;--cyan:#40d0e0;
  --sky:#60b0ff;
}
body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.5;min-height:100vh;display:flex;flex-direction:column}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}

.header-bar{background:var(--bg1);border-bottom:1px solid var(--border);padding:10px 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.header-left{display:flex;align-items:center;gap:16px}
.brand{font-size:13px;font-weight:700;color:var(--sky);letter-spacing:2px;text-transform:uppercase}
.live-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;animation:pulse 1.8s ease-in-out infinite}
.dot-paper{background:var(--gold);box-shadow:0 0 8px var(--gold)}
.dot-live{background:var(--green);box-shadow:0 0 8px var(--green)}
.mode-badge{font-size:10px;padding:3px 10px;border-radius:3px;font-weight:600;letter-spacing:1px}
.mode-paper{background:rgba(240,176,64,.15);color:var(--gold);border:1px solid rgba(240,176,64,.3)}
.mode-live{background:rgba(0,230,138,.15);color:var(--green);border:1px solid rgba(0,230,138,.3)}
.header-right{display:flex;align-items:center;gap:16px;font-size:10px;color:var(--text2)}

.main-wrap{flex:1;padding:16px 20px;max-width:1600px;margin:0 auto;width:100%}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:1100px){.grid{grid-template-columns:1fr}}
.full{grid-column:1/-1}

.hero-card{background:var(--bg1);border:1px solid var(--border);border-radius:8px;padding:24px 32px;text-align:center;position:relative;overflow:hidden;margin-bottom:14px}
.hero-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--sky),transparent)}
.hero-label{font-size:10px;color:var(--text2);text-transform:uppercase;letter-spacing:2px;margin-bottom:6px}
.hero-value{font-size:40px;font-weight:800;letter-spacing:-1px}
.hero-sub{display:flex;justify-content:center;gap:32px;margin-top:14px;flex-wrap:wrap}
.hero-sub-item{text-align:center}
.hero-sub-label{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px}
.hero-sub-value{font-size:15px;font-weight:600;margin-top:2px}

.stats-bar{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:14px}
.stat-card{background:var(--bg1);border:1px solid var(--border);border-radius:6px;padding:12px 14px;position:relative;overflow:hidden}
.stat-card::after{content:'';position:absolute;bottom:0;left:0;right:0;height:1px}
.stat-card.a-green::after{background:var(--green)}.stat-card.a-red::after{background:var(--red)}.stat-card.a-sky::after{background:var(--sky)}.stat-card.a-gold::after{background:var(--gold)}
.stat-label{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-bottom:4px}
.stat-value{font-size:15px;font-weight:700}

.panel{background:var(--bg1);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.panel-header{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;background:var(--bg2)}
.panel-header h2{color:var(--sky);font-size:11px;text-transform:uppercase;letter-spacing:1.5px;font-weight:600}
.panel-header .count{color:var(--text2);font-size:10px}
.panel-body{max-height:380px;overflow-y:auto}
.panel-body::-webkit-scrollbar{width:4px}
.panel-body::-webkit-scrollbar-track{background:var(--bg1)}
.panel-body::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}

table{width:100%;border-collapse:collapse;font-size:11px}
th{color:var(--text3);text-align:left;padding:8px 10px;border-bottom:1px solid var(--border);text-transform:uppercase;font-size:9px;letter-spacing:.8px;font-weight:600;position:sticky;top:0;background:var(--bg1)}
td{padding:7px 10px;border-bottom:1px solid rgba(26,34,53,.5)}
tr:hover{background:var(--bg3)}
.green{color:var(--green)}.red{color:var(--red)}.gray{color:var(--text3)}.gold{color:var(--gold)}.sky{color:var(--sky)}

.forecast-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;padding:14px}
.fc-card{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px}
.fc-city{font-size:11px;font-weight:700;color:var(--sky);margin-bottom:6px}
.fc-row{display:flex;justify-content:space-between;font-size:10px;margin-bottom:3px}
.fc-temp{font-weight:600}

.empty{color:var(--text3);text-align:center;padding:24px;font-size:11px;font-style:italic}
.status-bar{background:var(--bg1);border-top:1px solid var(--border);padding:8px 24px;font-size:10px;color:var(--text3)}
</style>
</head>
<body>

<div class="header-bar">
  <div class="header-left">
    <span class="brand">Weather Edge</span>
    <span class="live-dot dot-paper" id="mode-dot"></span>
    <span class="mode-badge mode-paper" id="mode-badge">PAPER MODE</span>
  </div>
  <div class="header-right">
    <span>Min Edge: 15%</span>
    <span>|</span>
    <span>Sell: +50%</span>
    <span>|</span>
    <span id="cycle-info">--</span>
  </div>
</div>

<div class="main-wrap">

<div class="hero-card full">
  <div class="hero-label">Overall Profit & Loss</div>
  <div class="hero-value" id="tb-overall">...</div>
  <div class="hero-sub">
    <div class="hero-sub-item"><div class="hero-sub-label">Unrealized</div><div class="hero-sub-value" id="tb-unreal">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Realized</div><div class="hero-sub-value" id="tb-real">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Win Rate</div><div class="hero-sub-value" id="tb-winrate">...</div></div>
    <div class="hero-sub-item"><div class="hero-sub-label">Cities</div><div class="hero-sub-value" id="tb-cities">...</div></div>
  </div>
</div>

<div class="stats-bar full">
  <div class="stat-card a-sky"><div class="stat-label">Portfolio</div><div class="stat-value" id="st-port">--</div></div>
  <div class="stat-card a-green"><div class="stat-label">Cash</div><div class="stat-value" id="st-cash">--</div></div>
  <div class="stat-card a-gold"><div class="stat-label">Open</div><div class="stat-value" id="st-open">--</div></div>
  <div class="stat-card a-green"><div class="stat-label">Wins</div><div class="stat-value green" id="st-wins">--</div></div>
  <div class="stat-card a-red"><div class="stat-label">Losses</div><div class="stat-value red" id="st-losses">--</div></div>
  <div class="stat-card a-sky"><div class="stat-label">Avg Win</div><div class="stat-value green" id="st-avgw">--</div></div>
  <div class="stat-card a-red"><div class="stat-label">Avg Loss</div><div class="stat-value red" id="st-avgl">--</div></div>
</div>

<div class="grid">

<div class="panel">
  <div class="panel-header"><h2>Forecasts</h2></div>
  <div class="panel-body" id="fc-body"><div class="empty">Loading forecasts...</div></div>
</div>

<div class="panel">
  <div class="panel-header"><h2>Hot Markets</h2><span class="count" id="hot-count"></span></div>
  <div class="panel-body"><table><thead><tr><th>Market</th><th>Yes</th><th>No</th><th>Vol</th></tr></thead><tbody id="hot-tbody"></tbody></table></div>
</div>

<div class="panel full">
  <div class="panel-header"><h2>Open Positions</h2><span class="count" id="open-count"></span></div>
  <div class="panel-body"><table><thead><tr><th>Ticker</th><th>Side</th><th>City</th><th>Type</th><th>Forecast</th><th>Edge</th><th>Entry</th><th>Bid</th><th>P&L</th></tr></thead><tbody id="open-tbody"></tbody></table></div>
</div>

<div class="panel full">
  <div class="panel-header"><h2>Trade History</h2></div>
  <div class="panel-body"><table><thead><tr><th>Time</th><th>Ticker</th><th>Side</th><th>City</th><th>Edge</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead><tbody id="trades-tbody"></tbody></table></div>
</div>

</div>
</div>

<div class="status-bar">
  <span>Kalshi Weather Edge Terminal | Cycle: <span id="sb-cycle">60s</span> | Forecasts: Open-Meteo</span>
</div>

<script>
const $=s=>document.getElementById(s);
const pnlClass=v=>v>0?'green':v<0?'red':'gray';
const fmt=v=>'$'+Math.abs(v).toFixed(2);
const pnlFmt=v=>(v>=0?'+':'-')+fmt(v);

async function refresh(){
  try{
    const [status,open,trades,hot,fc]=await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/open').then(r=>r.json()),
      fetch('/api/trades').then(r=>r.json()),
      fetch('/api/hot').then(r=>r.json()),
      fetch('/api/forecasts').then(r=>r.json()),
    ]);

    // Mode
    if(status.mode==='LIVE'){
      $('mode-dot').className='live-dot dot-live';
      $('mode-badge').className='mode-badge mode-live';
      $('mode-badge').textContent='LIVE';
    }

    // Hero
    const ov=status.overall_pnl||0;
    $('tb-overall').innerHTML=`<span class="${pnlClass(ov)}">${pnlFmt(ov)}</span>`;
    const ur=status.round_pnl||0;
    $('tb-unreal').innerHTML=`<span class="${pnlClass(ur)}">${pnlFmt(ur)}</span>`;
    const re=status.net_pnl||0;
    $('tb-real').innerHTML=`<span class="${pnlClass(re)}">${pnlFmt(re)}</span>`;
    const wr=status.wins+status.losses>0?((status.wins/(status.wins+status.losses))*100).toFixed(1)+'%':'--';
    $('tb-winrate').textContent=wr;
    $('tb-cities').textContent=status.cities_tracked||0;

    // Stats
    $('st-port').textContent=fmt(status.portfolio||0);
    $('st-cash').textContent=fmt(status.cash||0);
    $('st-open').textContent=status.open_count||0;
    $('st-wins').textContent=status.wins||0;
    $('st-losses').textContent=status.losses||0;
    $('st-avgw').textContent=status.avg_win?pnlFmt(status.avg_win):'--';
    $('st-avgl').textContent=status.avg_loss?pnlFmt(status.avg_loss):'--';

    // Forecasts
    let fcHtml='<div class="forecast-grid">';
    for(const [key,data] of Object.entries(fc)){
      fcHtml+=`<div class="fc-card"><div class="fc-city">${data.name}</div>`;
      for(const d of (data.forecasts||[])){
        fcHtml+=`<div class="fc-row"><span>${d.date}</span><span class="fc-temp red">${d.high_f?.toFixed(0)||'?'}F</span><span class="fc-temp sky">${d.low_f?.toFixed(0)||'?'}F</span><span>${d.precip_mm?.toFixed(1)||0}mm</span></div>`;
      }
      fcHtml+='</div>';
    }
    fcHtml+='</div>';
    $('fc-body').innerHTML=fcHtml||'<div class="empty">No forecasts</div>';

    // Hot markets
    $('hot-count').textContent=hot.length;
    $('hot-tbody').innerHTML=hot.map(m=>`<tr><td title="${m.title}">${m.ticker}</td><td>$${m.yes_ask?.toFixed(2)}</td><td>$${m.no_ask?.toFixed(2)}</td><td>${m.volume}</td></tr>`).join('')||'<tr><td colspan=4 class="empty">No markets</td></tr>';

    // Open positions
    $('open-count').textContent=open.length;
    $('open-tbody').innerHTML=open.map(p=>{
      const cls=pnlClass(p.unrealized);
      return `<tr><td>${p.ticker}</td><td>${p.side}</td><td>${p.city}</td><td>${p.market_type}</td><td>${p.forecast_value?.toFixed(1)}</td><td class="sky">${(p.edge*100).toFixed(0)}%</td><td>$${p.entry?.toFixed(2)}</td><td>$${p.current_bid?.toFixed(2)}</td><td class="${cls}">${pnlFmt(p.unrealized)} (${p.gain_pct>0?'+':''}${p.gain_pct}%)</td></tr>`;
    }).join('')||'<tr><td colspan=9 class="empty">No positions</td></tr>';

    // Trade history
    $('trades-tbody').innerHTML=trades.map(t=>{
      const cls=pnlClass(t.pnl);
      return `<tr><td>${new Date(t.created_at).toLocaleString()}</td><td>${t.ticker}</td><td>${t.side}</td><td>${t.city}</td><td class="sky">${(t.edge*100).toFixed(0)}%</td><td>$${t.entry?.toFixed(2)}</td><td>$${t.exit?.toFixed(2)}</td><td class="${cls}">${pnlFmt(t.pnl)}</td></tr>`;
    }).join('')||'<tr><td colspan=8 class="empty">No trades yet</td></tr>';

  }catch(e){console.error('Refresh error:',e)}
}

refresh();
setInterval(refresh,10000);
</script>
</body>
</html>"""


def bot_loop():
    mode = "PAPER" if not ENABLE_TRADING else "LIVE"
    logger.info(f"Weather Edge Bot starting [{mode}] -- min edge {MIN_EDGE*100:.0f}%, sell +{SELL_PROFIT_PCT*100:.0f}%, {CONTRACTS} contracts, {CASH_RESERVE*100:.0f}% reserve")
    logger.info(f"Tracking {len(ALL_SERIES)} weather series across {len(CITIES)} cities")

    while True:
        try:
            run_cycle()
        except Exception as e:
            logger.error(f"Cycle error: {e}")
            traceback.print_exc()
        time.sleep(CYCLE_SECONDS)


if __name__ == '__main__':
    bot_thread = Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    app.run(host='0.0.0.0', port=PORT)
