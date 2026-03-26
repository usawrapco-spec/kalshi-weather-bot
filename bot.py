"""
Weather Edge Bot for Kalshi. Uses GFS 31-member ensemble forecasts
targeted at exact NWS station coordinates to find mispriced weather
markets. Fractional Kelly sizing. Circuit breakers.
"""

import os, time, logging, traceback, math, re
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template_string
from threading import Thread
import psycopg2
from psycopg2.extras import RealDictCursor
from kalshi_auth import KalshiAuth
from weather import (
    fetch_all_forecasts, ensemble_prob_above, ensemble_prob_below,
    gaussian_prob_above, estimate_precip_prob, estimate_snow_prob,
    kelly_size, STATIONS,
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

# === STRATEGY (proven params from research) ===
MIN_EDGE = 0.08           # 8% minimum edge (proven threshold)
MAX_PRICE = 0.70          # don't buy above $0.70
MIN_PRICE = 0.03          # don't buy below $0.03
MAX_BET = 100.0           # $100 max per trade (Kelly capped)
MAX_POSITIONS = 30        # max simultaneous positions
MAX_PER_MARKET = 3        # max 3 repeating buys on same market
CYCLE_SECONDS = 60        # 60s cycle
STARTING_BALANCE = 10000.00
CASH_RESERVE = 0.30       # keep 30% cash
TAKER_FEE_RATE = 0.07
SELL_PROFIT_PCT = 0.50    # take profit at +50%

# Circuit breakers
DAILY_LOSS_LIMIT = 300.0  # stop trading if down $300 today
MAX_DAILY_TRADES = 50     # max trades per day

# Kalshi weather series prefixes -> station key
# These map Kalshi ticker prefixes to the NWS station they settle on
TEMP_SERIES = {
    'NHIGHNY':  'NYC',   'NHIGHLGA': 'LGA',
    'NHIGHCHI': 'ORD',   'NHIGHMDW': 'CHI',
    'NHIGHLA':  'LA',    'NHIGHMIA': 'MIA',
    'NHIGHDEN': 'DEN',   'NHIGHATL': 'ATL',
    'NHIGHDFW': 'DFW',   'NHIGHDAL': 'DAL',
    'NHIGHSEA': 'SEA',   'NHIGHPHX': 'PHX',
    'NHIGHDCA': 'DCA',   'NHIGHBOS': 'BOS',
    'NHIGHCLT': 'CLT',   'NHIGHDTW': 'DTW',
    'NHIGHHOU': 'HOU',   'NHIGHJAX': 'JAX',
    'NHIGHLAS': 'LAS',   'NHIGHMSP': 'MSP',
    'NHIGHBNA': 'BNA',   'NHIGHMSY': 'MSY',
    'NHIGHOKC': 'OKC',   'NHIGHPHL': 'PHL',
    'NHIGHAUS': 'AUS',   'NHIGHSAT': 'SAT',
    'NHIGHSFO': 'SFO',   'NHIGHTPA': 'TPA',
    'NHIGHBKF': 'BKF',
    # Also try KXHIGH variants
    'KXHIGHNY':  'NYC',  'KXHIGHLGA': 'LGA',
    'KXHIGHCHI': 'ORD',  'KXHIGHMDW': 'CHI',
    'KXHIGHLA':  'LA',   'KXHIGHMIA': 'MIA',
    'KXHIGHDEN': 'DEN',  'KXHIGHATL': 'ATL',
    'KXHIGHDFW': 'DFW',  'KXHIGHDAL': 'DAL',
    'KXHIGHSEA': 'SEA',  'KXHIGHPHX': 'PHX',
    'KXHIGHDCA': 'DCA',  'KXHIGHBOS': 'BOS',
    'KXHIGHCLT': 'CLT',  'KXHIGHDTW': 'DTW',
    'KXHIGHHOU': 'HOU',  'KXHIGHJAX': 'JAX',
    'KXHIGHLAS': 'LAS',  'KXHIGHMSP': 'MSP',
    'KXHIGHBNA': 'BNA',  'KXHIGHMSY': 'MSY',
    'KXHIGHOKC': 'OKC',  'KXHIGHPHL': 'PHL',
    'KXHIGHAUS': 'AUS',  'KXHIGHSAT': 'SAT',
    'KXHIGHSFO': 'SFO',  'KXHIGHTPA': 'TPA',
    'KXHIGHBKF': 'BKF',
}

PRECIP_SERIES = {
    'KXRAINY': 'NYC', 'KXRAINCHI': 'ORD', 'KXRAINLA': 'LA',
    'KXRAINMIA': 'MIA', 'KXRAINDEN': 'DEN', 'KXRAINATL': 'ATL',
    'RAINMNY': 'NYC', 'RAINMCHI': 'ORD', 'RAINMLA': 'LA',
}

SNOW_SERIES = {
    'KXSNOWNY': 'NYC', 'KXSNOWCHI': 'ORD', 'KXSNOWBOS': 'BOS',
    'NOWDATASNOWNY': 'NYC', 'NOWDATASNOWCHI': 'ORD',
}

ALL_SERIES_KEYS = set(list(TEMP_SERIES.keys()) + list(PRECIP_SERIES.keys()) + list(SNOW_SERIES.keys()))

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
                    station TEXT,
                    threshold NUMERIC,
                    forecast_value NUMERIC,
                    forecast_prob NUMERIC,
                    edge NUMERIC,
                    ensemble_size INTEGER,
                    target_date TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            for col, typ in [
                ('market_type', 'TEXT'), ('city', 'TEXT'), ('station', 'TEXT'),
                ('threshold', 'NUMERIC'), ('forecast_value', 'NUMERIC'),
                ('forecast_prob', 'NUMERIC'), ('edge', 'NUMERIC'),
                ('ensemble_size', 'INTEGER'), ('target_date', 'TEXT'),
            ]:
                try:
                    cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
                except:
                    pass

            cur.execute("""
                CREATE TABLE IF NOT EXISTS forecasts (
                    id SERIAL PRIMARY KEY,
                    city TEXT,
                    station TEXT,
                    target_date TEXT,
                    high_f NUMERIC,
                    low_f NUMERIC,
                    precip_mm NUMERIC,
                    snow_cm NUMERIC,
                    ensemble_size INTEGER,
                    fetched_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            for col, typ in [('station', 'TEXT'), ('ensemble_size', 'INTEGER')]:
                try:
                    cur.execute(f"ALTER TABLE forecasts ADD COLUMN {col} {typ}")
                except:
                    pass

            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id SERIAL PRIMARY KEY,
                    trade_date DATE DEFAULT CURRENT_DATE,
                    trades_count INTEGER DEFAULT 0,
                    daily_pnl NUMERIC DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0
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
_daily_trades = 0
_daily_pnl = 0.0
_last_trade_date = None


def sf(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0


def kalshi_fee(price, count):
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


# === CIRCUIT BREAKERS ===

def check_circuit_breakers():
    """Check if we should stop trading for the day."""
    global _daily_trades, _daily_pnl, _last_trade_date

    today = datetime.now(timezone.utc).date()
    if _last_trade_date != today:
        _daily_trades = 0
        _daily_pnl = 0.0
        _last_trade_date = today

    if _daily_pnl <= -DAILY_LOSS_LIMIT:
        logger.warning(f"CIRCUIT BREAKER: Daily loss ${_daily_pnl:.2f} exceeds limit ${DAILY_LOSS_LIMIT}")
        return False

    if _daily_trades >= MAX_DAILY_TRADES:
        logger.warning(f"CIRCUIT BREAKER: {_daily_trades} trades today, limit is {MAX_DAILY_TRADES}")
        return False

    return True


# === MARKET PARSING ===

def parse_weather_market(ticker, subtitle):
    """Parse market ticker to extract type, station, threshold, direction.

    Returns (market_type, city_key, threshold, comparison) or None.
    """
    ticker_upper = ticker.upper()

    # Temperature markets: NHIGHNY-26MAR25-B58 or KXHIGHNY-26MAR25-T55
    for series_prefix, city_key in TEMP_SERIES.items():
        if ticker_upper.startswith(series_prefix):
            # Extract threshold from ticker
            # B58 = bracket at 58, T55 = threshold at 55
            match = re.search(r'[BT](\d+)', ticker_upper)
            if match:
                threshold = int(match.group(1))
                return ('high_temp', city_key, threshold, 'above')

            # Try from subtitle
            match = re.search(r'(\d+)\s*[°]?\s*[Ff]', subtitle or '')
            if match:
                threshold = int(match.group(1))
                if any(w in (subtitle or '').lower() for w in ['above', 'higher', 'or more', '>=']):
                    return ('high_temp', city_key, threshold, 'above')
                elif any(w in (subtitle or '').lower() for w in ['below', 'lower', 'under', '<']):
                    return ('high_temp', city_key, threshold, 'below')
                return ('high_temp', city_key, threshold, 'above')

    # Precip markets
    for series_prefix, city_key in PRECIP_SERIES.items():
        if ticker_upper.startswith(series_prefix):
            return ('precip', city_key, 0.254, 'above')  # 0.01 inch = 0.254mm

    # Snow markets
    for series_prefix, city_key in SNOW_SERIES.items():
        if ticker_upper.startswith(series_prefix):
            match = re.search(r'(\d+\.?\d*)', subtitle or '')
            threshold = float(match.group(1)) * 2.54 if match else 2.54  # inches to cm
            return ('snow', city_key, threshold, 'above')

    return None


def extract_target_date(market):
    """Extract target date from market close_time or ticker."""
    close_time = market.get('close_time') or market.get('expected_expiration_time', '')
    if close_time:
        try:
            dt = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d'), dt
        except:
            pass

    # From ticker: NHIGHNY-26MAR25-B58
    match = re.search(r'-(\d{2})([A-Z]{3})(\d{2})-', market.get('ticker', ''))
    if match:
        day, mon, yr = match.groups()
        months = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
                  'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
        if mon in months:
            dt = datetime(2000+int(yr), months[mon], int(day), tzinfo=timezone.utc)
            return dt.strftime('%Y-%m-%d'), dt

    return None, None


def get_ensemble_for_date(city_key, target_date):
    """Get ensemble data for a specific city and date."""
    if city_key not in current_forecasts:
        return None

    fc = current_forecasts[city_key]
    ensemble = fc.get('ensemble')
    if not ensemble:
        return None

    for day in ensemble.get('daily', []):
        if day['date'] == target_date:
            return day

    return None


def get_deterministic_for_date(city_key, target_date):
    """Get deterministic forecast for a specific city and date."""
    if city_key not in current_forecasts:
        return None

    for day in current_forecasts[city_key].get('deterministic', []):
        if day['date'] == target_date:
            return day

    return None


def compute_probability(market_type, comparison, city_key, target_date, threshold, days_out):
    """Compute probability using ensemble (preferred) or Gaussian fallback.

    Returns (probability_for_yes, forecast_value, ensemble_size)
    """
    ensemble_day = get_ensemble_for_date(city_key, target_date)
    det_day = get_deterministic_for_date(city_key, target_date)

    if market_type == 'high_temp':
        if ensemble_day and len(ensemble_day.get('highs', [])) > 1:
            highs = ensemble_day['highs']
            if comparison == 'above':
                prob = ensemble_prob_above(highs, threshold)
            else:
                prob = ensemble_prob_below(highs, threshold)
            mean_high = sum(highs) / len(highs)
            return prob, mean_high, len(highs)

        # Gaussian fallback
        if det_day and det_day.get('high_f') is not None:
            forecast = det_day['high_f']
            if comparison == 'above':
                prob = gaussian_prob_above(forecast, threshold, days_out)
            else:
                prob = 1.0 - gaussian_prob_above(forecast, threshold, days_out)
            return prob, forecast, 1

    elif market_type == 'precip':
        if ensemble_day and len(ensemble_day.get('precip_sums', [])) > 1:
            precips = ensemble_day['precip_sums']
            prob = estimate_precip_prob(precips, threshold)
            mean_precip = sum(precips) / len(precips)
            return prob, mean_precip, len(precips)

        if det_day:
            val = det_day.get('precip_mm', 0)
            prob = estimate_precip_prob([val], threshold)
            return prob, val, 1

    elif market_type == 'snow':
        if ensemble_day and len(ensemble_day.get('snow_sums', [])) > 1:
            snows = ensemble_day['snow_sums']
            prob = estimate_snow_prob(snows, threshold)
            mean_snow = sum(snows) / len(snows)
            return prob, mean_snow, len(snows)

        if det_day:
            val = det_day.get('snow_cm', 0)
            prob = estimate_snow_prob([val], threshold)
            return prob, val, 1

    return None, None, 0


# === SELL LOGIC ===

def check_sells():
    global _daily_pnl
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
                reason = "WIN"
            else:
                pnl = round(-entry_price * count - buy_fee, 4)
                reason = "LOSS"
            logger.info(f"SETTLED: {ticker} {side} | {reason} | pnl=${pnl:.4f}")
            _daily_pnl += pnl
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

        if status in ('closed', 'settled', 'finalized'):
            continue

        # Get current bid
        if side == 'yes':
            current_bid = sf(market.get('yes_bid_dollars', '0'))
        else:
            current_bid = sf(market.get('no_bid_dollars', '0'))

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

        if gain >= SELL_PROFIT_PCT:
            buy_fee = kalshi_fee(entry_price, count)
            sell_fee = kalshi_fee(current_bid, count)
            pnl = round((current_bid - entry_price) * count - buy_fee - sell_fee, 4)

            result = place_order(ticker, side, 'sell', current_bid, count)
            if not result:
                continue

            order_id, filled = result
            if filled < count:
                pnl = round((current_bid - entry_price) * filled - kalshi_fee(entry_price, filled) - kalshi_fee(current_bid, filled), 4)

            _daily_pnl += pnl
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

    logger.info(f"SELL SUMMARY: sold={sold} settled={expired}")


# === BUY LOGIC ===

def fetch_weather_markets():
    """Fetch all open weather markets from Kalshi."""
    all_markets = []
    fetched_series = set()

    for series in ALL_SERIES_KEYS:
        if series in fetched_series:
            continue
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
            fetched_series.add(series)
        except Exception as e:
            if '404' not in str(e) and 'not found' not in str(e).lower():
                logger.error(f"Fetch {series} failed: {e}")

    logger.info(f"Fetched {len(all_markets)} weather markets from {len(fetched_series)} series")
    return all_markets


def buy_candidates(markets):
    """Find and buy mispriced weather markets using ensemble edge."""
    global _daily_trades

    if not check_circuit_breakers():
        return

    balance = get_balance()
    open_positions = get_open_positions()
    logger.info(f"Balance: ${balance:.2f} | {len(open_positions)} positions open")

    if len(open_positions) >= MAX_POSITIONS:
        logger.info(f"At max positions ({MAX_POSITIONS})")
        return

    deployable = balance * (1.0 - CASH_RESERVE)
    if deployable <= 1.0:
        logger.info(f"Deployable ${deployable:.2f} too low")
        return

    now = datetime.now(timezone.utc)

    ticker_counts = {}
    for t in open_positions:
        tk = t.get('ticker', '')
        ticker_counts[tk] = ticker_counts.get(tk, 0) + 1

    candidates = []

    for market in markets:
        ticker = market.get('ticker', '')
        subtitle = market.get('subtitle', '') or market.get('title', '')

        parsed = parse_weather_market(ticker, subtitle)
        if not parsed:
            continue

        market_type, city_key, threshold, comparison = parsed

        if ticker_counts.get(ticker, 0) >= MAX_PER_MARKET:
            continue

        target_date, target_dt = extract_target_date(market)
        if not target_date or not target_dt:
            continue

        days_out = max(0, (target_dt.date() - now.date()).days)
        if days_out > 6:
            continue

        our_prob, forecast_val, ensemble_size = compute_probability(
            market_type, comparison, city_key, target_date, threshold, days_out
        )
        if our_prob is None:
            continue

        yes_ask = float(market.get('yes_ask_dollars') or '999')
        no_ask = float(market.get('no_ask_dollars') or '999')

        best_side = None
        best_price = None
        best_edge = 0
        best_prob = 0

        if MIN_PRICE <= yes_ask <= MAX_PRICE:
            edge_yes = our_prob - yes_ask
            if edge_yes >= MIN_EDGE:
                best_side = 'yes'
                best_price = yes_ask
                best_edge = edge_yes
                best_prob = our_prob

        if MIN_PRICE <= no_ask <= MAX_PRICE:
            no_prob = 1.0 - our_prob
            edge_no = no_prob - no_ask
            if edge_no >= MIN_EDGE and edge_no > best_edge:
                best_side = 'no'
                best_price = no_ask
                best_edge = edge_no
                best_prob = no_prob

        if not best_side:
            continue

        # Kelly sizing
        contracts = kelly_size(best_prob, best_price, balance, MAX_BET)
        if contracts <= 0:
            continue

        station_info = STATIONS.get(city_key, (0, 0, city_key, '', ''))

        candidates.append({
            'ticker': ticker,
            'side': best_side,
            'price': best_price,
            'edge': best_edge,
            'our_prob': best_prob,
            'contracts': contracts,
            'market_type': market_type,
            'city': city_key,
            'station': station_info[3] if len(station_info) > 3 else '',
            'threshold': threshold,
            'forecast_value': forecast_val,
            'target_date': target_date,
            'days_out': days_out,
            'ensemble_size': ensemble_size,
        })

    # Sort by edge * ensemble confidence
    candidates.sort(key=lambda x: x['edge'] * (1.0 if x['ensemble_size'] > 1 else 0.7), reverse=True)
    logger.info(f"Found {len(candidates)} edge candidates (>={MIN_EDGE*100:.0f}%)")

    bought = 0
    for c in candidates:
        if len(open_positions) + bought >= MAX_POSITIONS:
            break
        if not check_circuit_breakers():
            break

        cost = c['price'] * c['contracts']
        if cost > deployable:
            c['contracts'] = max(1, int(deployable / c['price']))
            cost = c['price'] * c['contracts']
            if cost > deployable:
                continue

        logger.info(
            f"EDGE: {c['ticker']} {c['side']} x{c['contracts']} @ ${c['price']:.2f} | "
            f"prob={c['our_prob']:.2f} edge={c['edge']:.2f} ensemble={c['ensemble_size']} | "
            f"{c['city']}({c['station']}) {c['market_type']} thresh={c['threshold']} "
            f"forecast={c['forecast_value']:.1f} date={c['target_date']}"
        )

        result = place_order(c['ticker'], c['side'], 'buy', c['price'], c['contracts'])
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
                       (ticker, side, action, price, count, current_bid, series,
                        market_type, city, station, threshold, forecast_value,
                        forecast_prob, edge, ensemble_size, target_date)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (c['ticker'], c['side'], 'buy', float(c['price']), filled,
                     float(c['price']), '', c['market_type'], c['city'], c['station'],
                     float(c['threshold']), float(c['forecast_value']),
                     float(c['our_prob']), float(c['edge']), c['ensemble_size'],
                     c['target_date'])
                )
        except Exception as e:
            logger.error(f"Buy DB insert failed: {e}")
        finally:
            conn.close()

        deployable -= cost
        bought += 1
        _daily_trades += 1

    logger.info(f"Bought {bought} positions (Kelly sized)")


def update_hot_markets(markets):
    global current_hot_markets
    active = [m for m in markets if sf(m.get('yes_ask_dollars', '0')) < 0.99]
    by_vol = sorted(active, key=lambda m: int(m.get('volume', 0) or 0), reverse=True)[:15]
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
        logger.info("Fetching GFS ensemble forecasts for all stations...")
        current_forecasts = fetch_all_forecasts()
        ensemble_count = sum(1 for v in current_forecasts.values()
                           if v.get('ensemble') and v['ensemble'].get('daily')
                           and v['ensemble']['daily'][0].get('highs')
                           and len(v['ensemble']['daily'][0]['highs']) > 1)
        logger.info(f"Ensemble data for {ensemble_count}/{len(current_forecasts)} stations")
        for city, fc in current_forecasts.items():
            det = fc.get('deterministic', [{}])
            if det:
                d = det[0]
                station = STATIONS.get(city, (0, 0, city, '', ''))[3]
                ens_size = d.get('ensemble_size', 0)
                logger.info(f"  {city}({station}): high={d.get('high_f', '?')}F "
                          f"low={d.get('low_f', '?')}F precip={d.get('precip_mm', 0):.1f}mm "
                          f"ensemble={ens_size}")

        # Save to DB
        conn = get_db()
        try:
            with conn.cursor() as cur:
                for city, fc in current_forecasts.items():
                    station = STATIONS.get(city, (0, 0, city, '', ''))[3]
                    for d in fc.get('deterministic', []):
                        cur.execute(
                            """INSERT INTO forecasts (city, station, target_date, high_f, low_f, precip_mm, snow_cm, ensemble_size)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (city, station, d['date'], d.get('high_f'), d.get('low_f'),
                             d.get('precip_mm', 0), d.get('snow_cm', 0), d.get('ensemble_size', 0))
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
    logger.info(f"=== CYCLE END [{mode}] === Balance: ${balance:.2f} | Daily P&L: ${_daily_pnl:.2f}")


# === DASHBOARD ===

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
                resolved = cur.fetchall()
                cur.execute("SELECT count, price FROM trades WHERE action = 'buy'")
                all_buys = cur.fetchall()
        finally:
            conn.close()

        total_pnl = sum(sf(t['pnl']) for t in resolved)
        wins = sum(1 for t in resolved if sf(t['pnl']) > 0)
        losses = sum(1 for t in resolved if sf(t['pnl']) <= 0)
        avg_win = round(sum(sf(t['pnl']) for t in resolved if sf(t['pnl']) > 0) / max(wins, 1), 4)
        avg_loss = round(sum(sf(t['pnl']) for t in resolved if sf(t['pnl']) <= 0) / max(losses, 1), 4)
        total_contracts = sum((t.get('count') or 1) for t in all_buys)
        total_fees = round(sum(kalshi_fee(sf(t.get('price')), t.get('count') or 1) for t in all_buys), 4)

        round_cost = sum(sf(t.get('price')) * (t.get('count') or 1) for t in open_positions)
        round_pnl = round(positions_value - round_cost, 4)
        round_pct = round((round_pnl / round_cost * 100), 1) if round_cost > 0 else 0
        overall_pnl = round((cash + positions_value) - STARTING_BALANCE, 2)

        mode = "PAPER" if not ENABLE_TRADING else "LIVE"

        # Edge accuracy
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""SELECT edge, ensemble_size, pnl FROM trades
                              WHERE action='buy' AND pnl IS NOT NULL AND edge IS NOT NULL
                              ORDER BY created_at DESC LIMIT 100""")
                edge_trades = cur.fetchall()
        finally:
            conn.close()

        edge_wins = sum(1 for t in edge_trades if sf(t['pnl']) > 0)
        edge_total = len(edge_trades)
        avg_edge = round(sum(sf(t['edge']) for t in edge_trades) / max(edge_total, 1) * 100, 1)

        return jsonify({
            'portfolio': round(portfolio, 2),
            'cash': round(cash, 2),
            'positions_value': round(positions_value, 2),
            'overall_pnl': overall_pnl,
            'round_pnl': round_pnl,
            'round_pct': round_pct,
            'net_pnl': round(total_pnl, 4),
            'total_fees': total_fees,
            'total_contracts': total_contracts,
            'wins': wins,
            'losses': losses,
            'open_count': len(open_positions),
            'mode': mode,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'cities_tracked': len(current_forecasts),
            'stations_total': len(STATIONS),
            'daily_pnl': round(_daily_pnl, 2),
            'daily_trades': _daily_trades,
            'edge_accuracy': round(edge_wins / max(edge_total, 1) * 100, 1),
            'avg_edge': avg_edge,
            'edge_sample': edge_total,
            'cycle': _cycle_count,
        })
    except Exception as e:
        logger.error(f"API status error: {e}")
        return jsonify({'portfolio': 0, 'cash': 0, 'mode': 'PAPER'})


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
                'station': t.get('station', ''),
                'market_type': t.get('market_type', ''),
                'threshold': sf(t.get('threshold')),
                'forecast_value': sf(t.get('forecast_value')),
                'edge': sf(t.get('edge')),
                'ensemble_size': t.get('ensemble_size', 0),
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
                cur.execute("""SELECT * FROM trades WHERE action='buy' AND pnl IS NOT NULL
                              ORDER BY created_at DESC LIMIT 50""")
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
                'station': t.get('station', ''),
                'market_type': t.get('market_type', ''),
                'edge': sf(t.get('edge')),
                'ensemble_size': t.get('ensemble_size', 0),
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
    result = {}
    for city_key, fc in current_forecasts.items():
        info = STATIONS.get(city_key, (0, 0, city_key, '', ''))
        det = fc.get('deterministic', [])[:3]
        ensemble = fc.get('ensemble')
        ens_size = 0
        if ensemble and ensemble.get('daily'):
            ens_size = len(ensemble['daily'][0].get('highs', []))
        result[city_key] = {
            'name': info[2],
            'station': info[3] if len(info) > 3 else '',
            'station_name': info[4] if len(info) > 4 else '',
            'lat': info[0],
            'lon': info[1],
            'ensemble_size': ens_size,
            'forecasts': det,
        }
    return jsonify(result)


@app.route('/api/edge_stats')
def api_edge_stats():
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT city, station, market_type, edge, ensemble_size, forecast_prob, pnl
                    FROM trades WHERE action='buy' AND pnl IS NOT NULL AND edge IS NOT NULL
                    ORDER BY created_at DESC LIMIT 200
                """)
                trades = cur.fetchall()
        finally:
            conn.close()

        # By edge bucket
        edge_buckets = {}
        for t in trades:
            edge = sf(t.get('edge'))
            if edge < 0.10: b = '8-10%'
            elif edge < 0.15: b = '10-15%'
            elif edge < 0.25: b = '15-25%'
            elif edge < 0.40: b = '25-40%'
            else: b = '40%+'
            if b not in edge_buckets:
                edge_buckets[b] = {'total': 0, 'wins': 0, 'pnl': 0}
            edge_buckets[b]['total'] += 1
            edge_buckets[b]['wins'] += int(sf(t.get('pnl')) > 0)
            edge_buckets[b]['pnl'] += sf(t.get('pnl'))

        # By city
        city_stats = {}
        for t in trades:
            city = t.get('city', '?')
            if city not in city_stats:
                city_stats[city] = {'total': 0, 'wins': 0, 'pnl': 0}
            city_stats[city]['total'] += 1
            city_stats[city]['wins'] += int(sf(t.get('pnl')) > 0)
            city_stats[city]['pnl'] += sf(t.get('pnl'))

        # Ensemble vs deterministic
        ens_trades = [t for t in trades if (t.get('ensemble_size') or 0) > 1]
        det_trades = [t for t in trades if (t.get('ensemble_size') or 0) <= 1]
        ens_wr = round(sum(1 for t in ens_trades if sf(t['pnl']) > 0) / max(len(ens_trades), 1) * 100, 1)
        det_wr = round(sum(1 for t in det_trades if sf(t['pnl']) > 0) / max(len(det_trades), 1) * 100, 1)

        return jsonify({
            'edge_buckets': {k: {**v, 'wr': round(v['wins']/max(v['total'],1)*100, 1),
                                  'pnl': round(v['pnl'], 2)} for k, v in edge_buckets.items()},
            'city_stats': {k: {**v, 'wr': round(v['wins']/max(v['total'],1)*100, 1),
                                'pnl': round(v['pnl'], 2)} for k, v in city_stats.items()},
            'ensemble_wr': ens_wr, 'ensemble_n': len(ens_trades),
            'deterministic_wr': det_wr, 'deterministic_n': len(det_trades),
            'total': len(trades),
        })
    except Exception as e:
        logger.error(f"API edge stats error: {e}")
        return jsonify({})


@app.route('/dashboard')
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Weather Edge Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700;800&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#f4f6fb;--bg1:#ffffff;--bg2:#f0f2f8;--bg3:#e8ecf4;
  --border:#d8dde8;--border2:#c8d0e0;
  --text:#1a2030;--text2:#5a6478;--text3:#8890a4;
  --green:#0d9f5f;--green2:#0b8a52;--green-glow:rgba(13,159,95,.1);
  --red:#d63050;--red2:#c02845;--red-glow:rgba(214,48,80,.1);
  --gold:#c08000;--gold-glow:rgba(192,128,0,.08);
  --blue:#2a6fd6;--cyan:#1898b0;--purple:#6050c8;
  --sky:#2878d0;--sky-glow:rgba(40,120,208,.08);
  --orange:#d06820;
}
body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.5;min-height:100vh;display:flex;flex-direction:column;overflow-x:hidden}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:var(--bg2)}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:var(--text3)}

@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
@keyframes glow{0%,100%{box-shadow:0 0 15px var(--sky-glow)}50%{box-shadow:0 0 30px var(--sky-glow),0 0 60px rgba(80,184,255,.05)}}
@keyframes slideUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
@keyframes shimmer{0%{background-position:-200% 0}100%{background-position:200% 0}}

/* === HEADER === */
.header{background:var(--bg1);border-bottom:1px solid var(--border);padding:12px 28px;display:flex;align-items:center;justify-content:space-between;position:relative;box-shadow:0 1px 4px rgba(0,0,0,.06)}
.header::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--sky),var(--purple),var(--sky),transparent);opacity:.5}
.h-left{display:flex;align-items:center;gap:14px}
.brand{font-size:14px;font-weight:800;color:var(--sky);letter-spacing:2px;text-transform:uppercase}
.brand-sub{font-size:9px;color:var(--text3);letter-spacing:3px;text-transform:uppercase;margin-top:-2px}
.live-dot{width:8px;height:8px;border-radius:50%;animation:pulse 2s ease infinite}
.dot-paper{background:var(--gold);box-shadow:0 0 10px var(--gold)}
.dot-live{background:var(--green);box-shadow:0 0 10px var(--green)}
.mode-badge{font-size:9px;padding:3px 10px;border-radius:3px;font-weight:700;letter-spacing:1.5px}
.mode-paper{background:#fef3d0;color:var(--gold);border:1px solid #e8d090}
.mode-live{background:#d4f5e4;color:var(--green);border:1px solid #a0dbb8}
.h-right{display:flex;align-items:center;gap:20px;font-size:10px;color:var(--text2)}
.h-right .sep{color:var(--border2)}
.h-pill{background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:3px 8px;font-size:9px;color:var(--text2)}

/* === MAIN === */
.main{flex:1;padding:16px 24px;max-width:1800px;margin:0 auto;width:100%}

/* === HERO === */
.hero{background:var(--bg1);border:1px solid var(--border);border-radius:10px;padding:28px 36px;text-align:center;margin-bottom:16px;position:relative;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.04)}
.hero::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,var(--sky),var(--purple),var(--sky))}
.hero-label{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:3px;margin-bottom:8px}
.hero-val{font-size:48px;font-weight:800;letter-spacing:-2px;line-height:1}
.hero-row{display:flex;justify-content:center;gap:40px;margin-top:18px;flex-wrap:wrap}
.hero-item{text-align:center}
.hero-item-label{font-size:8px;color:var(--text3);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:3px}
.hero-item-val{font-size:16px;font-weight:700}

/* === STATS GRID === */
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:16px}
.stat{background:var(--bg1);border:1px solid var(--border);border-radius:8px;padding:14px 16px;position:relative;overflow:hidden;transition:border-color .3s;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.stat:hover{border-color:var(--sky)}
.stat::after{content:'';position:absolute;bottom:0;left:0;right:0;height:3px;border-radius:0 0 8px 8px}
.stat.a-green::after{background:var(--green)}
.stat.a-red::after{background:var(--red)}
.stat.a-sky::after{background:var(--sky)}
.stat.a-gold::after{background:var(--gold)}
.stat.a-purple::after{background:var(--purple)}
.stat.a-cyan::after{background:var(--cyan)}
.stat.a-orange::after{background:var(--orange)}
.s-label{font-size:8px;color:var(--text3);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:5px}
.s-val{font-size:17px;font-weight:700}

/* === GRID === */
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:1200px){.grid{grid-template-columns:1fr}}
.full{grid-column:1/-1}
.triple{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px}
@media(max-width:1200px){.triple{grid-template-columns:1fr}}

/* === PANEL === */
.panel{background:var(--bg1);border:1px solid var(--border);border-radius:10px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.04)}
.p-head{padding:12px 18px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;background:var(--bg2)}
.p-head h2{font-size:11px;text-transform:uppercase;letter-spacing:2px;font-weight:700;display:flex;align-items:center;gap:8px}
.p-head h2 .icon{font-size:14px}
.p-head .badge{font-size:9px;color:var(--text2);background:var(--bg);padding:2px 8px;border-radius:3px;border:1px solid var(--border)}
.p-body{max-height:420px;overflow-y:auto}

/* === TABLES === */
table{width:100%;border-collapse:collapse;font-size:11px}
th{color:var(--text2);text-align:left;padding:9px 12px;border-bottom:2px solid var(--border);text-transform:uppercase;font-size:8px;letter-spacing:1px;font-weight:700;position:sticky;top:0;background:var(--bg1);z-index:1}
td{padding:8px 12px;border-bottom:1px solid var(--border)}
tr{transition:all .15s}
tr:hover{background:var(--bg2)}
.green{color:var(--green)}.red{color:var(--red)}.gray{color:var(--text3)}.gold{color:var(--gold)}.sky{color:var(--sky)}.cyan{color:var(--cyan)}.purple{color:var(--purple)}.orange{color:var(--orange)}

/* === FORECAST CARDS === */
.fc-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:8px;padding:12px}
.fc-card{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:10px 12px;transition:all .2s;box-shadow:0 1px 3px rgba(0,0,0,.03)}
.fc-card:hover{border-color:var(--sky);background:var(--bg1)}
.fc-city{font-size:11px;font-weight:700;color:var(--sky);margin-bottom:2px}
.fc-station{font-size:8px;color:var(--text3);letter-spacing:.5px;margin-bottom:6px}
.fc-row{display:flex;justify-content:space-between;align-items:center;font-size:10px;margin-bottom:2px;padding:2px 0}
.fc-temp-hi{color:var(--red);font-weight:700}
.fc-temp-lo{color:var(--cyan);font-weight:700}
.fc-precip{color:var(--blue);font-size:9px}
.fc-ens{font-size:8px;color:var(--purple);margin-top:4px;padding-top:4px;border-top:1px solid var(--border);font-weight:600}

/* === EDGE CARDS === */
.edge-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;padding:12px}
.edge-card{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:12px;box-shadow:0 1px 3px rgba(0,0,0,.03)}
.edge-card-title{font-size:9px;color:var(--gold);text-transform:uppercase;letter-spacing:1.5px;font-weight:700;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid var(--border)}
.edge-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px;font-size:10px}
.edge-bar-wrap{flex:1;height:6px;background:var(--border);border-radius:3px;overflow:hidden;margin:0 8px}
.edge-bar{height:100%;border-radius:3px;transition:width .5s ease}
.edge-badge{font-size:8px;font-weight:700;padding:1px 5px;border-radius:2px;min-width:32px;text-align:center}

/* === CITY HEAT MAP === */
.city-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(90px,1fr));gap:6px;padding:12px}
.city-chip{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:8px;text-align:center;transition:all .2s;cursor:default}
.city-chip:hover{border-color:var(--sky)}
.city-name{font-size:9px;font-weight:700;color:var(--sky)}
.city-stat{font-size:14px;font-weight:800;margin:3px 0}
.city-sub{font-size:8px;color:var(--text3)}

/* === FOOTER === */
.footer{background:var(--bg1);border-top:1px solid var(--border);padding:8px 28px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;font-size:9px;color:var(--text2)}
.footer span{display:flex;align-items:center;gap:6px}

.empty{color:var(--text3);text-align:center;padding:28px;font-size:11px;font-style:italic}
.tag{display:inline-block;font-size:8px;padding:1px 5px;border-radius:2px;font-weight:600;letter-spacing:.5px}
.tag-ens{background:#ece8ff;color:var(--purple)}
.tag-det{background:var(--bg2);color:var(--text2)}
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <div class="h-left">
    <div>
      <div class="brand">Weather Edge</div>
      <div class="brand-sub">Kalshi Trading Terminal</div>
    </div>
    <span class="live-dot dot-paper" id="mode-dot"></span>
    <span class="mode-badge mode-paper" id="mode-badge">PAPER</span>
  </div>
  <div class="h-right">
    <span class="h-pill">Edge >= 8%</span>
    <span class="sep">|</span>
    <span class="h-pill">Kelly 15%</span>
    <span class="sep">|</span>
    <span class="h-pill">GFS 31-Member Ensemble</span>
    <span class="sep">|</span>
    <span>Cycle <span id="hd-cycle">--</span></span>
  </div>
</div>

<!-- Main -->
<div class="main">

<!-- Hero P&L -->
<div class="hero">
  <div class="hero-label">Overall Profit & Loss</div>
  <div class="hero-val" id="hero-pnl">...</div>
  <div class="hero-row">
    <div class="hero-item"><div class="hero-item-label">Unrealized</div><div class="hero-item-val" id="h-unreal">--</div></div>
    <div class="hero-item"><div class="hero-item-label">Realized</div><div class="hero-item-val" id="h-real">--</div></div>
    <div class="hero-item"><div class="hero-item-label">Today</div><div class="hero-item-val" id="h-today">--</div></div>
    <div class="hero-item"><div class="hero-item-label">Win Rate</div><div class="hero-item-val" id="h-wr">--</div></div>
    <div class="hero-item"><div class="hero-item-label">Edge Accuracy</div><div class="hero-item-val" id="h-edge">--</div></div>
    <div class="hero-item"><div class="hero-item-label">Stations</div><div class="hero-item-val cyan" id="h-stations">--</div></div>
  </div>
</div>

<!-- Stats -->
<div class="stats" id="stats-bar">
  <div class="stat a-sky"><div class="s-label">Portfolio</div><div class="s-val" id="s-port">--</div></div>
  <div class="stat a-green"><div class="s-label">Cash</div><div class="s-val" id="s-cash">--</div></div>
  <div class="stat a-gold"><div class="s-label">Open Positions</div><div class="s-val" id="s-open">--</div></div>
  <div class="stat a-green"><div class="s-label">Wins</div><div class="s-val green" id="s-wins">--</div></div>
  <div class="stat a-red"><div class="s-label">Losses</div><div class="s-val red" id="s-losses">--</div></div>
  <div class="stat a-purple"><div class="s-label">Avg Edge</div><div class="s-val purple" id="s-avgedge">--</div></div>
  <div class="stat a-cyan"><div class="s-label">Contracts</div><div class="s-val cyan" id="s-contracts">--</div></div>
  <div class="stat a-orange"><div class="s-label">Today Trades</div><div class="s-val orange" id="s-dtrades">--</div></div>
</div>

<!-- Forecasts + Edge Analysis -->
<div class="grid" style="margin-bottom:14px">
  <div class="panel">
    <div class="p-head"><h2><span class="icon">&#9729;</span> <span style="color:var(--sky)">Station Forecasts</span></h2><span class="badge" id="fc-count">--</span></div>
    <div class="p-body" style="max-height:500px" id="fc-body"><div class="empty">Loading forecasts...</div></div>
  </div>
  <div class="panel">
    <div class="p-head"><h2><span class="icon">&#9889;</span> <span style="color:var(--gold)">Edge Analysis</span></h2><span class="badge" id="edge-count">--</span></div>
    <div class="p-body" style="max-height:500px" id="edge-body"><div class="empty">Loading edge stats...</div></div>
  </div>
</div>

<!-- Open Positions -->
<div class="panel full" style="margin-bottom:14px">
  <div class="p-head"><h2><span class="icon">&#9679;</span> <span style="color:var(--green)">Open Positions</span></h2><span class="badge" id="op-count">0</span></div>
  <div class="p-body">
    <table><thead><tr>
      <th>Ticker</th><th>Side</th><th>City</th><th>Station</th><th>Forecast</th><th>Edge</th><th>Ens</th><th>Qty</th><th>Entry</th><th>Bid</th><th>P&L</th>
    </tr></thead><tbody id="op-tbody"></tbody></table>
  </div>
</div>

<!-- Hot Markets + History -->
<div class="grid">
  <div class="panel">
    <div class="p-head"><h2><span class="icon">&#128293;</span> <span style="color:var(--orange)">Hot Markets</span></h2><span class="badge" id="hot-count">0</span></div>
    <div class="p-body">
      <table><thead><tr><th>Market</th><th>Yes</th><th>No</th><th>Vol</th></tr></thead><tbody id="hot-tbody"></tbody></table>
    </div>
  </div>
  <div class="panel">
    <div class="p-head"><h2><span class="icon">&#128203;</span> <span style="color:var(--cyan)">Trade History</span></h2></div>
    <div class="p-body">
      <table><thead><tr><th>Time</th><th>Ticker</th><th>City</th><th>Edge</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead><tbody id="tr-tbody"></tbody></table>
    </div>
  </div>
</div>

</div>

<!-- Footer -->
<div class="footer">
  <span>Weather Edge Terminal v2 | NWS Station-Targeted | GFS Ensemble | Kelly Criterion</span>
  <span>Sell +50% | Circuit: $300/day | Max 20 positions | Forecast: Open-Meteo</span>
</div>

<script>
const $=s=>document.getElementById(s);
const pc=v=>v>0?'green':v<0?'red':'gray';
const fmt=v=>'$'+Math.abs(v).toFixed(2);
const pf=v=>(v>=0?'+':'')+fmt(v);

async function refresh(){
  try{
    const [st,op,tr,hot,fc,es]=await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/open').then(r=>r.json()),
      fetch('/api/trades').then(r=>r.json()),
      fetch('/api/hot').then(r=>r.json()),
      fetch('/api/forecasts').then(r=>r.json()),
      fetch('/api/edge_stats').then(r=>r.json()),
    ]);

    // Mode
    if(st.mode==='LIVE'){$('mode-dot').className='live-dot dot-live';$('mode-badge').className='mode-badge mode-live';$('mode-badge').textContent='LIVE';}

    // Hero
    const ov=st.overall_pnl||0;
    $('hero-pnl').innerHTML=`<span class="${pc(ov)}">${pf(ov)}</span>`;
    const ur=st.round_pnl||0;$('h-unreal').innerHTML=`<span class="${pc(ur)}">${pf(ur)}</span>`;
    const re=st.net_pnl||0;$('h-real').innerHTML=`<span class="${pc(re)}">${pf(re)}</span>`;
    const td=st.daily_pnl||0;$('h-today').innerHTML=`<span class="${pc(td)}">${pf(td)}</span>`;
    const tot=st.wins+st.losses;
    $('h-wr').innerHTML=tot>0?`<span class="${st.wins/tot>.5?'green':'red'}">${(st.wins/tot*100).toFixed(1)}%</span>`:'--';
    $('h-edge').innerHTML=st.edge_sample>0?`<span class="${st.edge_accuracy>50?'green':'red'}">${st.edge_accuracy}%</span> <span style="font-size:9px;color:var(--text3)">(${st.edge_sample})</span>`:'--';
    $('h-stations').textContent=`${st.cities_tracked}/${st.stations_total}`;
    $('hd-cycle').textContent=st.cycle||'--';

    // Stats
    $('s-port').textContent=fmt(st.portfolio||0);
    $('s-cash').textContent=fmt(st.cash||0);
    $('s-open').textContent=st.open_count||0;
    $('s-wins').textContent=st.wins||0;
    $('s-losses').textContent=st.losses||0;
    $('s-avgedge').textContent=st.avg_edge?st.avg_edge+'%':'--';
    $('s-contracts').textContent=st.total_contracts||0;
    $('s-dtrades').textContent=st.daily_trades||0;

    // Forecasts
    const fcKeys=Object.keys(fc).sort();
    $('fc-count').textContent=fcKeys.length+' stations';
    let fcH='<div class="fc-grid">';
    for(const k of fcKeys){
      const c=fc[k];
      fcH+=`<div class="fc-card"><div class="fc-city">${c.name}</div><div class="fc-station">${c.station} - ${c.station_name}</div>`;
      for(const d of (c.forecasts||[])){
        const hi=d.high_f!=null?d.high_f.toFixed(0):'?';
        const lo=d.low_f!=null?d.low_f.toFixed(0):'?';
        const pr=d.precip_mm!=null?d.precip_mm.toFixed(1):'0';
        fcH+=`<div class="fc-row"><span style="color:var(--text2)">${d.date?.slice(5)||'?'}</span><span class="fc-temp-hi">${hi}°</span><span class="fc-temp-lo">${lo}°</span><span class="fc-precip">${pr}mm</span></div>`;
      }
      fcH+=`<div class="fc-ens">${c.ensemble_size>1?'&#9733; '+c.ensemble_size+' ensemble members':'&#9675; deterministic only'}</div>`;
      fcH+='</div>';
    }
    fcH+='</div>';
    $('fc-body').innerHTML=fcH;

    // Edge analysis
    $('edge-count').textContent=(es.total||0)+' trades analyzed';
    let eH='<div class="edge-grid">';

    // Edge buckets
    eH+='<div class="edge-card"><div class="edge-card-title">Win Rate by Edge Size</div>';
    for(const [k,v] of Object.entries(es.edge_buckets||{})){
      const wr=v.wr||0;const col=wr>=60?'var(--green)':wr>=45?'var(--gold)':'var(--red)';
      eH+=`<div class="edge-row"><span style="min-width:50px">${k}</span><div class="edge-bar-wrap"><div class="edge-bar" style="width:${wr}%;background:${col}"></div></div><span class="edge-badge" style="color:${col}">${wr}%</span><span style="color:var(--text3);font-size:9px;margin-left:4px">(${v.total})</span></div>`;
    }
    eH+='</div>';

    // Ensemble vs deterministic
    eH+='<div class="edge-card"><div class="edge-card-title">Ensemble vs Deterministic</div>';
    eH+=`<div class="edge-row"><span>Ensemble (31)</span><div class="edge-bar-wrap"><div class="edge-bar" style="width:${es.ensemble_wr||0}%;background:var(--purple)"></div></div><span class="edge-badge" style="color:var(--purple)">${es.ensemble_wr||0}%</span><span style="color:var(--text3);font-size:9px;margin-left:4px">(${es.ensemble_n||0})</span></div>`;
    eH+=`<div class="edge-row"><span>Deterministic</span><div class="edge-bar-wrap"><div class="edge-bar" style="width:${es.deterministic_wr||0}%;background:var(--text2)"></div></div><span class="edge-badge" style="color:var(--text2)">${es.deterministic_wr||0}%</span><span style="color:var(--text3);font-size:9px;margin-left:4px">(${es.deterministic_n||0})</span></div>`;
    eH+='</div>';

    // City performance
    eH+='<div class="edge-card"><div class="edge-card-title">Performance by City</div>';
    for(const [k,v] of Object.entries(es.city_stats||{}).sort((a,b)=>b[1].pnl-a[1].pnl)){
      const col=v.pnl>=0?'var(--green)':'var(--red)';
      eH+=`<div class="edge-row"><span class="sky" style="min-width:35px">${k}</span><span style="color:${col};min-width:55px">${v.pnl>=0?'+':''}$${v.pnl.toFixed(2)}</span><span style="color:var(--text2)">${v.wr}% (${v.total})</span></div>`;
    }
    eH+='</div>';

    eH+='</div>';
    $('edge-body').innerHTML=eH;

    // Open positions
    $('op-count').textContent=op.length;
    $('op-tbody').innerHTML=op.map(p=>{
      const cls=pc(p.unrealized);
      const ensTag=p.ensemble_size>1?'<span class="tag tag-ens">ENS '+p.ensemble_size+'</span>':'<span class="tag tag-det">DET</span>';
      return `<tr><td>${p.ticker}</td><td>${p.side}</td><td class="sky">${p.city}</td><td style="font-size:9px;color:var(--text2)">${p.station}</td><td>${p.forecast_value?.toFixed(1)||'?'}</td><td class="purple">${(p.edge*100).toFixed(0)}%</td><td>${ensTag}</td><td>${p.count}</td><td>$${p.entry?.toFixed(2)}</td><td>$${p.current_bid?.toFixed(2)}</td><td class="${cls}">${pf(p.unrealized)} (${p.gain_pct>0?'+':''}${p.gain_pct}%)</td></tr>`;
    }).join('')||'<tr><td colspan=11 class="empty">No open positions</td></tr>';

    // Hot markets
    $('hot-count').textContent=hot.length;
    $('hot-tbody').innerHTML=hot.map(m=>`<tr><td title="${m.title}">${m.ticker}</td><td>$${m.yes_ask?.toFixed(2)}</td><td>$${m.no_ask?.toFixed(2)}</td><td>${m.volume?.toLocaleString()}</td></tr>`).join('')||'<tr><td colspan=4 class="empty">No markets</td></tr>';

    // Trade history
    $('tr-tbody').innerHTML=tr.map(t=>{
      const cls=pc(t.pnl);
      return `<tr><td style="font-size:9px">${new Date(t.created_at).toLocaleString()}</td><td>${t.ticker}</td><td class="sky">${t.city}</td><td class="purple">${(t.edge*100).toFixed(0)}%</td><td>$${t.entry?.toFixed(2)}</td><td>$${t.exit?.toFixed(2)}</td><td class="${cls}">${pf(t.pnl)}</td></tr>`;
    }).join('')||'<tr><td colspan=7 class="empty">No trades yet</td></tr>';

  }catch(e){console.error('Refresh error:',e)}
}

refresh();
setInterval(refresh,10000);
</script>
</body>
</html>"""


def bot_loop():
    mode = "PAPER" if not ENABLE_TRADING else "LIVE"
    logger.info(f"=== WEATHER EDGE BOT [{mode}] ===")
    logger.info(f"Strategy: min edge {MIN_EDGE*100:.0f}%, Kelly {15}%, max ${MAX_BET}/trade")
    logger.info(f"Circuit breakers: ${DAILY_LOSS_LIMIT}/day loss limit, {MAX_DAILY_TRADES} trades/day")
    logger.info(f"Tracking {len(STATIONS)} NWS stations across {len(set(TEMP_SERIES.values()))} cities")
    logger.info(f"Settlement source: NWS Daily Climate Report (CLI)")

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
