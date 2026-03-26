"""
Weather forecast engine using Open-Meteo (free, no API key).
Fetches forecasts for cities with active Kalshi weather markets
and converts them to probability estimates.
"""

import logging
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# Cities with Kalshi weather markets
# Format: city_key -> (lat, lon, display_name)
CITIES = {
    'NYC': (40.7128, -74.0060, 'New York'),
    'CHI': (41.8781, -87.6298, 'Chicago'),
    'LA':  (34.0522, -118.2437, 'Los Angeles'),
    'MIA': (25.7617, -80.1918, 'Miami'),
    'DEN': (39.7392, -104.9903, 'Denver'),
    'ATL': (33.7490, -84.3880, 'Atlanta'),
    'DAL': (32.7767, -96.7970, 'Dallas'),
    'SEA': (47.6062, -122.3321, 'Seattle'),
    'PHX': (33.4484, -112.0740, 'Phoenix'),
    'DCA': (38.9072, -77.0369, 'Washington DC'),
}

# Typical forecast error margins (degrees F) — used for probability spread
TEMP_FORECAST_STDEV = {
    0: 1.5,    # today: very accurate
    1: 2.5,    # tomorrow
    2: 3.5,    # 2 days out
    3: 4.5,    # 3 days out
    4: 5.5,    # 4+ days
}

_session = requests.Session()


def _c_to_f(c):
    """Celsius to Fahrenheit."""
    return c * 9.0 / 5.0 + 32.0


def fetch_forecast(city_key):
    """Fetch 7-day forecast from Open-Meteo for a city.

    Returns list of dicts: [{date, high_f, low_f, precip_mm, snow_cm, wind_mph}, ...]
    """
    if city_key not in CITIES:
        return []

    lat, lon, name = CITIES[city_key]

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
        logger.error(f"Forecast fetch failed for {city_key}: {e}")
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
    """Fetch forecasts for all tracked cities.

    Returns dict: {city_key: [forecast_days]}
    """
    results = {}
    for city_key in CITIES:
        forecasts = fetch_forecast(city_key)
        if forecasts:
            results[city_key] = forecasts
    return results


def _normal_cdf(x):
    """Approximate standard normal CDF using Abramowitz and Stegun."""
    import math
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


def estimate_prob_above(forecast_temp, threshold, days_out):
    """Estimate probability that actual temp will be >= threshold.

    Uses forecast temp as mean and day-dependent standard deviation.
    """
    stdev = TEMP_FORECAST_STDEV.get(min(days_out, 4), 5.5)
    if stdev <= 0:
        return 1.0 if forecast_temp >= threshold else 0.0

    z = (threshold - forecast_temp) / stdev
    return 1.0 - _normal_cdf(z)


def estimate_prob_below(forecast_temp, threshold, days_out):
    """Estimate probability that actual temp will be < threshold."""
    return 1.0 - estimate_prob_above(forecast_temp, threshold, days_out)


def estimate_precip_prob(forecast_mm, threshold_mm=0.01):
    """Estimate probability of precipitation exceeding threshold.

    Simple logistic based on forecast amount.
    """
    import math
    if forecast_mm <= 0:
        return 0.05  # small chance even with 0 forecast
    if forecast_mm >= threshold_mm * 5:
        return 0.95
    # Sigmoid centered around threshold
    x = (forecast_mm - threshold_mm) / max(threshold_mm, 0.5)
    return 1.0 / (1.0 + math.exp(-3.0 * x))


def estimate_snow_prob(forecast_cm, threshold_cm=0.1):
    """Estimate probability of snowfall exceeding threshold."""
    import math
    if forecast_cm <= 0:
        return 0.05
    if forecast_cm >= threshold_cm * 5:
        return 0.95
    x = (forecast_cm - threshold_cm) / max(threshold_cm, 0.5)
    return 1.0 / (1.0 + math.exp(-3.0 * x))
