"""
Weather forecast engine using Open-Meteo GFS Ensemble API.
Uses exact NWS station coordinates that Kalshi settles on.
31-member GFS ensemble for calibrated probability estimates.
"""

import logging
import requests
import math
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# === EXACT NWS STATION COORDINATES ===
# Kalshi settles on NWS Daily Climate Report (CLI) from these specific stations.
# Using generic city coords = wrong forecasts = losing money.
# Format: city_key -> (lat, lon, display_name, ICAO, station_name)
STATIONS = {
    'NYC':  (40.7790, -73.9692, 'New York',       'KNYC', 'Central Park'),
    'LGA':  (40.7772, -73.8726, 'LaGuardia',      'KLGA', 'LaGuardia Airport'),
    'CHI':  (41.7841, -87.7551, 'Chicago Midway',  'KMDW', 'Midway Airport'),
    'ORD':  (41.9602, -87.9316, 'Chicago OHare',  'KORD', 'OHare International'),
    'LA':   (33.9382, -118.3870, 'Los Angeles',    'KLAX', 'LAX Airport'),
    'MIA':  (25.7881, -80.3169, 'Miami',           'KMIA', 'Miami International'),
    'DEN':  (39.8466, -104.6560, 'Denver',         'KDEN', 'Denver International'),
    'ATL':  (33.6304, -84.4221, 'Atlanta',         'KATL', 'Hartsfield-Jackson'),
    'DFW':  (32.8998, -97.0403, 'Dallas/FW',       'KDFW', 'DFW International'),
    'DAL':  (32.8471, -96.8518, 'Dallas Love',     'KDAL', 'Love Field'),
    'SEA':  (47.4502, -122.3088, 'Seattle',        'KSEA', 'Sea-Tac Airport'),
    'PHX':  (33.4373, -112.0078, 'Phoenix',        'KPHX', 'Sky Harbor'),
    'DCA':  (38.8512, -77.0402, 'Washington DC',   'KDCA', 'Reagan National'),
    'BOS':  (42.3656, -71.0096, 'Boston',           'KBOS', 'Logan Airport'),
    'CLT':  (35.2144, -80.9473, 'Charlotte',       'KCLT', 'Douglas International'),
    'DTW':  (42.2124, -83.3534, 'Detroit',          'KDTW', 'Detroit Metro'),
    'HOU':  (29.6454, -95.2789, 'Houston',         'KHOU', 'Hobby Airport'),
    'JAX':  (30.4941, -81.6879, 'Jacksonville',    'KJAX', 'Jacksonville Intl'),
    'LAS':  (36.0840, -115.1537, 'Las Vegas',      'KLAS', 'Harry Reid Intl'),
    'MSP':  (44.8848, -93.2223, 'Minneapolis',     'KMSP', 'MSP Airport'),
    'BNA':  (36.1245, -86.6782, 'Nashville',       'KBNA', 'Nashville Intl'),
    'MSY':  (29.9934, -90.2580, 'New Orleans',     'KMSY', 'Armstrong Intl'),
    'OKC':  (35.3931, -97.6007, 'Oklahoma City',   'KOKC', 'Will Rogers'),
    'PHL':  (39.8721, -75.2407, 'Philadelphia',    'KPHL', 'Philadelphia Intl'),
    'AUS':  (30.2099, -97.6806, 'Austin',          'KAUS', 'Bergstrom Intl'),
    'SAT':  (29.5337, -98.4698, 'San Antonio',     'KSAT', 'San Antonio Intl'),
    'SFO':  (37.6213, -122.3790, 'San Francisco',  'KSFO', 'SFO Airport'),
    'TPA':  (27.9755, -82.5332, 'Tampa',           'KTPA', 'Tampa Intl'),
    'BKF':  (39.7017, -104.7517, 'Aurora/Boulder',  'KBKF', 'Buckley SFB'),
}

_session = requests.Session()


def fetch_ensemble_forecast(city_key):
    """Fetch GFS 31-member ensemble forecast from Open-Meteo for a station.

    Returns dict with:
      - daily: [{date, highs: [31 values], lows: [31 values], precip_sums: [31], snow_sums: [31]}]
      - deterministic: [{date, high_f, low_f, precip_mm, snow_cm, wind_mph}]
    """
    if city_key not in STATIONS:
        return None

    lat, lon, name, icao, station = STATIONS[city_key]

    # Fetch ensemble (31 members) for temperature
    try:
        resp = _session.get(
            'https://ensemble-api.open-meteo.com/v1/ensemble',
            params={
                'latitude': lat,
                'longitude': lon,
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum',
                'temperature_unit': 'fahrenheit',
                'precipitation_unit': 'mm',
                'timezone': 'America/New_York',
                'forecast_days': 7,
                'models': 'gfs_seamless',
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Ensemble fetch failed for {city_key} ({icao}): {e}")
        return None

    daily = data.get('daily', {})
    dates = daily.get('time', [])

    # Ensemble members are returned as arrays of arrays
    # temperature_2m_max_member01, temperature_2m_max_member02, etc.
    # Or as a single array if aggregated differently
    # Open-Meteo ensemble returns: temperature_2m_max as array with all members interleaved

    result = {'daily': [], 'deterministic': []}

    # Parse ensemble members for each day
    num_days = len(dates)

    for i in range(num_days):
        day_data = {'date': dates[i], 'highs': [], 'lows': [], 'precip_sums': [], 'snow_sums': []}

        # Collect all ensemble member values for this day
        for key_prefix, target_list in [
            ('temperature_2m_max', 'highs'),
            ('temperature_2m_min', 'lows'),
            ('precipitation_sum', 'precip_sums'),
            ('snowfall_sum', 'snow_sums'),
        ]:
            # Try member keys: key_member01, key_member02, ... key_member31
            for m in range(31):
                member_key = f"{key_prefix}_member{m:02d}"
                vals = daily.get(member_key, [])
                if i < len(vals) and vals[i] is not None:
                    day_data[target_list].append(vals[i])

            # Fallback: if no member keys, try the base key (deterministic)
            if not day_data[target_list]:
                base_vals = daily.get(key_prefix, [])
                if i < len(base_vals) and base_vals[i] is not None:
                    day_data[target_list] = [base_vals[i]]

        result['daily'].append(day_data)

        # Deterministic = mean of ensemble
        highs = day_data['highs']
        lows = day_data['lows']
        precip = day_data['precip_sums']
        snow = day_data['snow_sums']

        result['deterministic'].append({
            'date': dates[i],
            'high_f': sum(highs) / len(highs) if highs else None,
            'low_f': sum(lows) / len(lows) if lows else None,
            'precip_mm': sum(precip) / len(precip) if precip else 0,
            'snow_cm': sum(snow) / len(snow) if snow else 0,
            'ensemble_size': len(highs),
        })

    return result


def fetch_deterministic_forecast(city_key):
    """Fallback: fetch standard deterministic forecast from Open-Meteo."""
    if city_key not in STATIONS:
        return []

    lat, lon, name, icao, station = STATIONS[city_key]

    try:
        resp = _session.get(
            'https://api.open-meteo.com/v1/forecast',
            params={
                'latitude': lat,
                'longitude': lon,
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,wind_speed_10m_max',
                'temperature_unit': 'fahrenheit',
                'wind_speed_unit': 'mph',
                'precipitation_unit': 'mm',
                'timezone': 'America/New_York',
                'forecast_days': 7,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Deterministic forecast fetch failed for {city_key}: {e}")
        return []

    daily = data.get('daily', {})
    dates = daily.get('time', [])
    highs = daily.get('temperature_2m_max', [])
    lows = daily.get('temperature_2m_min', [])
    precip = daily.get('precipitation_sum', [])
    snow = daily.get('snowfall_sum', [])
    wind = daily.get('wind_speed_10m_max', [])

    forecasts = []
    for i in range(len(dates)):
        forecasts.append({
            'date': dates[i],
            'high_f': highs[i] if i < len(highs) else None,
            'low_f': lows[i] if i < len(lows) else None,
            'precip_mm': precip[i] if i < len(precip) else 0,
            'snow_cm': snow[i] if i < len(snow) else 0,
            'wind_mph': wind[i] if i < len(wind) else 0,
        })

    return forecasts


def fetch_all_forecasts():
    """Fetch ensemble + deterministic forecasts for all stations.

    Returns dict: {city_key: {ensemble: {...}, deterministic: [...]}}
    """
    results = {}
    for city_key in STATIONS:
        ensemble = fetch_ensemble_forecast(city_key)
        if ensemble:
            results[city_key] = {
                'ensemble': ensemble,
                'deterministic': ensemble['deterministic'],
            }
        else:
            # Fallback to deterministic only
            det = fetch_deterministic_forecast(city_key)
            if det:
                results[city_key] = {
                    'ensemble': None,
                    'deterministic': det,
                }

    logger.info(f"Fetched forecasts for {len(results)}/{len(STATIONS)} stations")
    return results


# === PROBABILITY ESTIMATION ===

def ensemble_prob_above(ensemble_highs, threshold):
    """Count fraction of ensemble members with high >= threshold.

    This is the proven approach: 28/31 members above 70F = 90.3% probability.
    """
    if not ensemble_highs:
        return None
    above = sum(1 for h in ensemble_highs if h >= threshold)
    return above / len(ensemble_highs)


def ensemble_prob_below(ensemble_highs, threshold):
    """Count fraction of ensemble members with high < threshold."""
    if not ensemble_highs:
        return None
    below = sum(1 for h in ensemble_highs if h < threshold)
    return below / len(ensemble_highs)


def _normal_cdf(x):
    """Approximate standard normal CDF (Abramowitz and Stegun)."""
    if x < -6:
        return 0.0
    if x > 6:
        return 1.0
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x / 2.0)
    return 0.5 * (1.0 + sign * y)


# Lead-time dependent RMSE for Gaussian fallback (when ensemble unavailable)
TEMP_RMSE = {
    0: 2.0,   # today
    1: 2.5,   # tomorrow
    2: 3.5,   # 2 days
    3: 4.5,   # 3 days
    4: 5.5,   # 4+ days
}


def gaussian_prob_above(forecast_temp, threshold, days_out):
    """Fallback: Gaussian CDF probability when ensemble unavailable."""
    rmse = TEMP_RMSE.get(min(days_out, 4), 5.5)
    z = (threshold - forecast_temp) / rmse
    return 1.0 - _normal_cdf(z)


def estimate_precip_prob(ensemble_precips, threshold_mm=0.254):
    """Estimate probability of measurable precipitation (>= 0.01 inch = 0.254mm).

    Uses ensemble if available, otherwise logistic fallback.
    """
    if ensemble_precips and len(ensemble_precips) > 1:
        above = sum(1 for p in ensemble_precips if p >= threshold_mm)
        return above / len(ensemble_precips)

    # Single value fallback
    val = ensemble_precips[0] if ensemble_precips else 0
    if val <= 0:
        return 0.05
    if val >= threshold_mm * 5:
        return 0.95
    x = (val - threshold_mm) / max(threshold_mm, 0.5)
    return 1.0 / (1.0 + math.exp(-3.0 * x))


def estimate_snow_prob(ensemble_snows, threshold_cm=0.1):
    """Estimate probability of snowfall exceeding threshold."""
    if ensemble_snows and len(ensemble_snows) > 1:
        above = sum(1 for s in ensemble_snows if s >= threshold_cm)
        return above / len(ensemble_snows)

    val = ensemble_snows[0] if ensemble_snows else 0
    if val <= 0:
        return 0.05
    if val >= threshold_cm * 5:
        return 0.95
    x = (val - threshold_cm) / max(threshold_cm, 0.5)
    return 1.0 / (1.0 + math.exp(-3.0 * x))


# === KELLY CRITERION ===

KELLY_FRACTION = 0.15  # use 15% of Kelly (conservative fractional Kelly)

def kelly_size(prob, price, bankroll, max_bet=100.0, max_pct=0.05):
    """Fractional Kelly criterion for position sizing.

    Args:
        prob: Our estimated probability of winning
        price: Market price (cost per contract)
        bankroll: Total available balance
        max_bet: Hard dollar cap per trade
        max_pct: Max percentage of bankroll per trade

    Returns: number of contracts to buy (0 if no edge)
    """
    if price <= 0 or price >= 1 or prob <= price:
        return 0

    # Kelly formula for binary outcome
    # f* = (bp - q) / b where b = (1-price)/price, p = prob, q = 1-prob
    b = (1.0 - price) / price
    q = 1.0 - prob
    f_star = (b * prob - q) / b

    if f_star <= 0:
        return 0

    # Apply fractional Kelly
    bet_size = bankroll * f_star * KELLY_FRACTION

    # Apply caps
    bet_size = min(bet_size, max_bet, bankroll * max_pct)

    contracts = max(1, int(bet_size / price))
    return contracts
