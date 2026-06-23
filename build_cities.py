"""
build_cities.py — CityCompare Data Pipeline
============================================
Produces cities_free.json and cities_max.json for GitHub Releases.

Data sources:
  Phase 1 (Free tier):
    - Census ACS 5-Year: population, medianHomePrice, medianMonthlyRent,
      medianHouseholdIncome, avgCommuteMinutes
    - BLS LAUS: unemploymentRate
    - Static: colIndex (MERIC placeholder until meric_col.json is available)
    - Static data files: sales taxes, state income taxes, energy, utilities, transit

  Phase 2 (Max tier additions):
    - Census ACS: effectivePropertyTaxRate (derived from owner costs)
    - BLS QCEW: yoyWageGrowthRate
    - FBI UCR: violentCrimeRatePer100k, propertyCrimeRatePer100k
    - NOAA CDO: heatingDegreeDays, coolingDegreeDays
    - Walk Score API: walkabilityScore, transitAccessibilityIndex

  Phase 3 (Enrichment — from static reference files):
    - MERIC: colIndex, groceryIndex (loaded from data/meric_col.json when available)
    - EIA: gasPricePerGallon, monthlyElectricityBill
    - Static: transitRoundTripFare, transitMonthlyPass, rideShare5MileCost
    - Static: stateIncomeTaxTopRate, effectiveSalesTaxRate, localSalesTaxRate
    - Static: monthlyWaterSewerTrash, monthlyBroadband

Usage:
  python build_cities.py

  Set env vars (or edit CONFIG below):
    CENSUS_API_KEY   — from api.census.gov (free)
    BLS_API_KEY      — from bls.gov/developers (free)
    FBI_API_KEY      — from api.data.gov (free)
    NOAA_API_KEY     — from ncei.noaa.gov/cdo-web (free)
    WALKSCORE_API_KEY— from walkscore.com/professional/api.php (free hobbyist)
"""

import json
import os
import re
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
import requests

# ── Configuration ────────────────────────────────────────────────────────────

CONFIG = {
    "census_key":    os.environ.get("CENSUS_API_KEY",    "47461a7b7cf5dae414c9803fc90423ea74eda0be"),
    "bls_key":       os.environ.get("BLS_API_KEY",       ""),
    "fbi_key":       os.environ.get("FBI_API_KEY",       ""),
    "noaa_key":      os.environ.get("NOAA_API_KEY",      ""),
    "walkscore_key": os.environ.get("WALKSCORE_API_KEY", ""),
    "min_population": 25000,
    "cities_csv":    Path("data/american_cities_expanded_clean.csv"),
    "output_dir":    Path("output"),
    "data_dir":      Path("data"),
    "request_delay": 0.25,   # seconds between API calls (be polite)
    "bls_batch_size": 50,    # BLS allows up to 50 series per request
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── State lookup tables ───────────────────────────────────────────────────────

STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
    "DE":"10","DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17",
    "IN":"18","IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24",
    "MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31",
    "NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38",
    "OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46",
    "TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56"
}

STATE_NAMES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","DC":"District of Columbia",
    "FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois",
    "IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana",
    "ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota",
    "MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada",
    "NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York",
    "NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont",
    "VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"
}

# ── Load static reference data ────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    if not path.exists():
        log.warning(f"Static file not found: {path} — using empty dict")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def load_static_data():
    d = CONFIG["data_dir"]
    return {
        "taxes":    load_json(d / "state_taxes.json"),
        "sales":    load_json(d / "sales_taxes.json"),
        "energy":   load_json(d / "energy_prices.json"),
        "utils":    load_json(d / "utilities.json"),
        "transit":  load_json(d / "transit_fares.json"),
        "meric":    load_json(d / "meric_col.json"),   # optional — Phase 3
    }

# ── Load city list ────────────────────────────────────────────────────────────

def load_cities() -> list[tuple[str, str]]:
    """Load (city_name, state_abbr) pairs from the clean CSV."""
    path = CONFIG["cities_csv"]
    if not path.exists():
        # Fallback: look in data/ subdirectory
        path = CONFIG["data_dir"] / "american_cities_expanded_clean.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"City list not found. Expected at {CONFIG['cities_csv']} "
            f"or {CONFIG['data_dir']}/american_cities_expanded_clean.csv"
        )
    cities = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip().rstrip(",").strip('"')
            if not line:
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 2:
                city = parts[0].strip().strip('"')
                state = parts[1].strip().strip('"')
                if len(state) == 2 and state.isalpha():
                    cities.append((city, state))
    log.info(f"Loaded {len(cities)} cities from {path}")
    return cities

# ── Census ACS ────────────────────────────────────────────────────────────────

def fetch_census_acs(state_abbr: str) -> dict:
    """
    Fetch ACS 5-Year 2023 place-level data for one state.
    Returns dict keyed by city name (lower) → data dict.

    Variables fetched:
      B01003_001E  Total population
      B25077_001E  Median home value (owner-occupied)
      B25064_001E  Median gross rent
      B19013_001E  Median household income
      B08303_001E  Aggregate travel time to work (sum)
      B08301_001E  Workers 16+ (for avg commute calc)
      B25097_001E  Mortgage status: with a mortgage, median owner cost (monthly)
      B25105_001E  Median monthly housing costs
    """
    fips = STATE_FIPS.get(state_abbr)
    if not fips:
        return {}

    variables = ",".join([
        "NAME",
        "B01003_001E",   # population
        "B25077_001E",   # median home value
        "B25064_001E",   # median gross rent
        "B19013_001E",   # median household income
        "B08303_001E",   # aggregate commute time
        "B08301_001E",   # workers 16+ (denominator for avg commute)
        "B25105_001E",   # median monthly housing costs (for property tax proxy)
    ])

    url = (
        f"https://api.census.gov/data/2023/acs/acs5"
        f"?get={variables}"
        f"&for=place:*"
        f"&in=state:{fips}"
        f"&key={CONFIG['census_key']}"
    )

    try:
        resp = requests.get(url, timeout=30,
                           headers={"User-Agent": "CityCompare-Pipeline/1.0"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Census ACS fetch failed for {state_abbr}: {e}")
        return {}

    result = {}
    headers = data[0]
    for row in data[1:]:
        r = dict(zip(headers, row))
        full_name = r.get("NAME", "")
        city_raw = full_name.split(",")[0].strip()

        # Strip Census suffixes
        suffixes = [
            " city and borough", " metro township", " unified government (balance)",
            " consolidated government (balance)", " metro government (balance)",
            " metropolitan government (balance)", " city (balance)", " city",
            " town", " village", " borough", " CDPD", " CDP", " municipality",
        ]
        city_clean = city_raw
        for sfx in suffixes:
            if city_clean.lower().endswith(sfx.lower()):
                city_clean = city_clean[:-len(sfx)].strip()
                break
        city_clean = re.sub(r'\s*\(.*?\)\s*$', '', city_clean).strip()

        def safe_float(val, fallback=0.0):
            try:
                v = float(val)
                return v if v > 0 else fallback
            except (TypeError, ValueError):
                return fallback

        pop = safe_float(r.get("B01003_001E"))
        if pop < CONFIG["min_population"]:
            continue

        # Avg commute: aggregate_time / workers (minutes)
        agg_time = safe_float(r.get("B08303_001E"))
        workers  = safe_float(r.get("B08301_001E"), 1)
        avg_commute = round(agg_time / workers, 1) if workers > 0 else 27.0

        # Property tax rate proxy: not directly in ACS.
        # We use median monthly housing cost vs median home value as a rough
        # annual cost rate. This is a Phase 3 field — will be overridden by
        # better data when available. Default to state average (~1.0%).
        home_val = safe_float(r.get("B25077_001E"))
        monthly_housing = safe_float(r.get("B25105_001E"))
        if home_val > 0 and monthly_housing > 0:
            annual_housing = monthly_housing * 12
            # Rough: mortgage P&I + insurance + taxes ≈ housing cost
            # Taxes are roughly 25-35% of total housing cost for homeowners
            prop_tax_rate = round(min((annual_housing * 0.28) / home_val * 100, 3.5), 2)
        else:
            prop_tax_rate = 1.0  # national average fallback

        result[city_clean.lower()] = {
            "census_name":          city_clean,
            "population":           int(pop),
            "medianHomePrice":      safe_float(r.get("B25077_001E")),
            "medianMonthlyRent":    safe_float(r.get("B25064_001E")),
            "medianHouseholdIncome":safe_float(r.get("B19013_001E")),
            "avgCommuteMinutes":    avg_commute if avg_commute > 5 else 27.0,
            "effectivePropertyTaxRate": prop_tax_rate,
        }

    time.sleep(CONFIG["request_delay"])
    return result

# ── BLS Unemployment ──────────────────────────────────────────────────────────

# BLS LAUS area codes: "CN" prefix for counties/cities won't cover all places.
# Best approach: use state-level unemployment as proxy for smaller cities,
# and city-level for top metros where LAUS codes are known.
# For the full pipeline this is acceptable — unemployment varies more by MSA
# than by specific city within an MSA.

BLS_STATE_UNEMPLOYMENT = {
    # 2024 annual average unemployment rates by state (BLS LAUS)
    "AL":3.2,"AK":4.2,"AZ":3.7,"AR":3.2,"CA":5.3,"CO":3.5,"CT":4.0,
    "DC":5.2,"DE":3.8,"FL":3.4,"GA":3.5,"HI":3.1,"ID":3.1,"IL":4.7,
    "IN":3.3,"IA":2.8,"KS":2.9,"KY":4.3,"LA":3.8,"ME":2.9,"MD":3.2,
    "MA":3.7,"MI":4.0,"MN":3.2,"MS":4.0,"MO":3.5,"MT":2.8,"NE":2.5,
    "NV":5.0,"NH":2.6,"NJ":4.4,"NM":4.2,"NY":4.4,"NC":3.6,"ND":2.2,
    "OH":4.0,"OK":3.2,"OR":4.5,"PA":3.9,"RI":4.2,"SC":3.5,"SD":2.2,
    "TN":3.4,"TX":3.9,"UT":3.1,"VT":2.2,"VA":3.0,"WA":4.4,"WV":4.6,
    "WI":3.0,"WY":3.5
}

BLS_STATE_WAGE_GROWTH = {
    # 2023-2024 YoY average wage growth by state (BLS QCEW, all industries)
    "AL":4.2,"AK":3.5,"AZ":5.1,"AR":4.0,"CA":3.8,"CO":4.5,"CT":4.8,
    "DC":3.2,"DE":4.1,"FL":4.8,"GA":4.9,"HI":3.6,"ID":4.8,"IL":4.2,
    "IN":4.0,"IA":3.8,"KS":3.9,"KY":4.1,"LA":3.7,"ME":4.5,"MD":3.9,
    "MA":4.3,"MI":4.0,"MN":4.1,"MS":3.8,"MO":4.0,"MT":5.2,"NE":3.9,
    "NV":4.5,"NH":4.4,"NJ":4.0,"NM":4.3,"NY":3.5,"NC":5.0,"ND":3.5,
    "OH":4.0,"OK":4.2,"OR":4.3,"PA":3.9,"RI":4.1,"SC":5.1,"SD":3.8,
    "TN":4.8,"TX":4.5,"UT":5.3,"VT":4.0,"VA":4.2,"WA":4.8,"WV":3.5,
    "WI":3.9,"WY":3.8
}

def get_bls_data(state_abbr: str) -> tuple[float, float]:
    """Return (unemployment_rate, yoy_wage_growth) for a state."""
    unemp = BLS_STATE_UNEMPLOYMENT.get(state_abbr, 4.0)
    wage  = BLS_STATE_WAGE_GROWTH.get(state_abbr, 4.0)
    return unemp, wage

# ── FBI Crime Data ────────────────────────────────────────────────────────────

# FBI UCR API requires an api.data.gov key.
# When no key is available, we use state-level crime rate estimates.
# Cities under 50k will always be DataConfidence.limited per PRD.

FBI_STATE_VIOLENT_CRIME = {
    # Violent crime rate per 100,000 (FBI UCR 2022, latest full year)
    "AL":465,"AK":838,"AZ":476,"AR":595,"CA":499,"CO":413,"CT":199,
    "DC":992,"DE":410,"FL":421,"GA":348,"HI":265,"ID":229,"IL":431,
    "IN":325,"IA":272,"KS":383,"KY":218,"LA":659,"ME":108,"MD":469,
    "MA":328,"MI":427,"MN":252,"MS":273,"MO":498,"MT":360,"NE":298,
    "NV":598,"NH":151,"NJ":248,"NM":780,"NY":379,"NC":365,"ND":290,
    "OH":326,"OK":437,"OR":292,"PA":296,"RI":218,"SC":490,"SD":399,
    "TN":617,"TX":411,"UT":229,"VT":160,"VA":203,"WA":291,"WV":314,
    "WI":295,"WY":234
}

FBI_STATE_PROPERTY_CRIME = {
    # Property crime rate per 100,000 (FBI UCR 2022)
    "AL":2318,"AK":2676,"AZ":2516,"AR":2619,"CA":2213,"CO":2648,"CT":1351,
    "DC":3578,"DE":1837,"FL":2150,"GA":2147,"HI":2765,"ID":1602,"IL":1691,
    "IN":1787,"IA":1539,"KS":2235,"KY":1778,"LA":2572,"ME":1308,"MD":1668,
    "MA":1264,"MI":1743,"MN":2043,"MS":2098,"MO":2669,"MT":2752,"NE":2185,
    "NV":2834,"NH":1148,"NJ":1275,"NM":3548,"NY":1418,"NC":2272,"ND":1697,
    "OH":1883,"OK":2818,"OR":2866,"PA":1337,"RI":1468,"SC":2703,"SD":1876,
    "TN":2696,"TX":2175,"UT":2451,"VT":1450,"VA":1613,"WA":2933,"WV":1638,
    "WI":1724,"WY":2137
}

def get_crime_data(city: str, state: str, population: int) -> tuple[float, float, str]:
    """
    Returns (violent_per_100k, property_per_100k, confidence_level).
    confidence: 'high' | 'moderate' | 'limited'
    Cities under 50k default to limited confidence per PRD Section 24.
    """
    fbi_key = CONFIG.get("fbi_key", "")

    # If FBI key available, try city-level lookup
    if fbi_key:
        data = fetch_fbi_city(city, state, fbi_key)
        if data:
            conf = "high" if population >= 50000 else "limited"
            return data[0], data[1], conf

    # Fall back to state-level estimate
    violent  = float(FBI_STATE_VIOLENT_CRIME.get(state, 380))
    prop     = float(FBI_STATE_PROPERTY_CRIME.get(state, 1954))
    conf     = "limited" if population < 50000 else "moderate"
    return violent, prop, conf

def fetch_fbi_city(city: str, state: str, api_key: str) -> tuple | None:
    """Attempt FBI UCR API city-level lookup. Returns (violent, property) or None."""
    try:
        url = (
            f"https://api.usa.gov/crime/fbi/cde/summarized/agency/offense"
            f"?type=violent-crime&from=2022&to=2022"
            f"&api_key={api_key}"
        )
        resp = requests.get(url, timeout=15,
                           headers={"User-Agent": "CityCompare-Pipeline/1.0"})
        if resp.status_code != 200:
            return None
        # Parse response — format varies; return None if uncertain
        return None  # Placeholder — full FBI API parsing is complex
    except Exception:
        return None

# ── NOAA Climate Data ────────────────────────────────────────────────────────

# NOAA 30-year climate normals (1991-2020) — HDD and CDD by state capital
# as proxy for state-wide data. When NOAA key is available, fetch city-level.
# HDD = Heating Degree Days (base 65°F), CDD = Cooling Degree Days (base 65°F)

NOAA_STATE_CLIMATE = {
    # (HDD, CDD) 30-year normals
    "AK":(10700, 30), "AL":(2700, 2580), "AR":(3100, 2400), "AZ":(1500, 3700),
    "CA":(2800, 1200), "CO":(6000, 700),  "CT":(5500, 700),  "DC":(4200, 1400),
    "DE":(4700, 1200), "FL":(700,  3800), "GA":(2800, 2200), "HI":(0,    2800),
    "ID":(5800, 700),  "IL":(6300, 1100), "IN":(5600, 1100), "IA":(6600, 1100),
    "KS":(5100, 1500), "KY":(4600, 1400), "LA":(1700, 3000), "ME":(7400, 400),
    "MD":(4600, 1300), "MA":(5600, 700),  "MI":(6500, 700),  "MN":(8200, 700),
    "MS":(2500, 2700), "MO":(5000, 1400), "MT":(7700, 500),  "NE":(6000, 1200),
    "NV":(4200, 1800), "NH":(7300, 400),  "NJ":(4900, 1100), "NM":(4000, 1500),
    "NY":(5700, 900),  "NC":(3300, 1700), "ND":(9000, 700),  "OH":(5700, 1000),
    "OK":(3600, 2100), "OR":(4200, 400),  "PA":(5300, 1000), "RI":(5900, 600),
    "SC":(2700, 2000), "SD":(7500, 1000), "TN":(3700, 1700), "TX":(2200, 3000),
    "UT":(5500, 1200), "VT":(7900, 400),  "VA":(4000, 1400), "WA":(4800, 300),
    "WV":(5000, 1000), "WI":(7300, 700),  "WY":(7200, 500),
}

def get_climate_data(city: str, state: str) -> tuple[float, float]:
    """Returns (heatingDegreeDays, coolingDegreeDays)."""
    hdd, cdd = NOAA_STATE_CLIMATE.get(state, (4500, 1500))
    return float(hdd), float(cdd)

# ── Walk Score ───────────────────────────────────────────────────────────────

WALKSCORE_STATE_DEFAULTS = {
    # Average walk score for cities in each state (rough estimates)
    # 0=car-dependent, 50=somewhat walkable, 70=very walkable, 90=walker's paradise
    "AK":35,"AL":32,"AR":30,"AZ":38,"CA":52,"CO":45,"CT":50,"DC":98,
    "DE":42,"FL":46,"GA":42,"HI":58,"ID":35,"IL":55,"IN":36,"IA":38,
    "KS":38,"KY":38,"LA":52,"ME":40,"MD":52,"MA":62,"MI":42,"MN":50,
    "MS":28,"MO":44,"MT":35,"NE":40,"NV":45,"NH":40,"NJ":58,"NM":38,
    "NY":68,"NC":40,"ND":38,"OH":45,"OK":35,"OR":52,"PA":52,"RI":58,
    "SC":38,"SD":36,"TN":38,"TX":42,"UT":42,"VT":40,"VA":46,"WA":52,
    "WV":28,"WI":45,"WY":30
}

WALKSCORE_TRANSIT_DEFAULTS = {
    # Average transit score by state
    "AK":15,"AL":15,"AR":10,"AZ":28,"CA":42,"CO":38,"CT":40,"DC":88,
    "DE":25,"FL":32,"GA":35,"HI":40,"ID":15,"IL":48,"IN":22,"IA":18,
    "KS":18,"KY":20,"LA":30,"ME":18,"MD":52,"MA":62,"MI":28,"MN":42,
    "MS":10,"MO":32,"MT":12,"NE":22,"NV":35,"NH":18,"NJ":52,"NM":22,
    "NY":68,"NC":25,"ND":12,"OH":32,"OK":18,"OR":42,"PA":48,"RI":38,
    "SC":20,"SD":12,"TN":22,"TX":32,"UT":38,"VT":18,"VA":40,"WA":48,
    "WV":12,"WI":32,"WY":10
}

def fetch_walkscore(city: str, state: str, lat: float = None, lon: float = None) -> tuple[float, float]:
    """
    Returns (walkabilityScore, transitAccessibilityIndex).
    Uses Walk Score API if key is set; otherwise returns state-level defaults.
    """
    ws_key = CONFIG.get("walkscore_key", "")
    if ws_key and lat and lon:
        try:
            url = (
                f"https://api.walkscore.com/score"
                f"?format=json"
                f"&address={requests.utils.quote(f'{city}, {state}')}"
                f"&lat={lat}&lon={lon}"
                f"&transit=1"
                f"&wsapikey={ws_key}"
            )
            resp = requests.get(url, timeout=10,
                               headers={"User-Agent": "CityCompare-Pipeline/1.0"})
            if resp.status_code == 200:
                data = resp.json()
                walk  = float(data.get("walkscore", 0))
                transit = float(data.get("transit", {}).get("score", 0))
                time.sleep(CONFIG["request_delay"])
                return walk, transit
        except Exception as e:
            log.warning(f"Walk Score API failed for {city}, {state}: {e}")

    return float(WALKSCORE_STATE_DEFAULTS.get(state, 40)), \
           float(WALKSCORE_TRANSIT_DEFAULTS.get(state, 25))

# ── COL Index (MERIC) ────────────────────────────────────────────────────────

# State-level COL index from MERIC Q4 2024 (national average = 100)
# Updated quarterly — replace with meric_col.json when available
MERIC_STATE_COL = {
    "AK":129,"AL":88, "AR":87, "AZ":105,"CA":149,"CO":113,"CT":122,
    "DC":150,"DE":108,"FL":103,"GA":93, "HI":186,"ID":103,"IL":98,
    "IN":90, "IA":89, "KS":89, "KY":90, "LA":93, "ME":110,"MD":120,
    "MA":135,"MI":92, "MN":100,"MS":84, "MO":90, "MT":105,"NE":92,
    "NV":103,"NH":118,"NJ":125,"NM":92, "NY":139,"NC":96, "ND":99,
    "OH":93, "OK":88, "OR":115,"PA":102,"RI":119,"SC":94, "SD":94,
    "TN":91, "TX":95, "UT":105,"VT":118,"VA":107,"WA":118,"WV":88,
    "WI":96, "WY":97
}

MERIC_STATE_GROCERY = {
    # Grocery component of MERIC COL index
    "AK":132,"AL":97, "AR":95, "AZ":101,"CA":108,"CO":100,"CT":110,
    "DC":114,"DE":101,"FL":102,"GA":97, "HI":159,"ID":96, "IL":97,
    "IN":95, "IA":95, "KS":94, "KY":94, "LA":98, "ME":107,"MD":107,
    "MA":109,"MI":97, "MN":100,"MS":93, "MO":95, "MT":104,"NE":96,
    "NV":102,"NH":112,"NJ":109,"NM":97, "NY":113,"NC":97, "ND":102,
    "OH":96, "OK":93, "OR":104,"PA":100,"RI":109,"SC":98, "SD":101,
    "TN":96, "TX":95, "UT":97, "VT":109,"VA":101,"WA":107,"WV":94,
    "WI":98, "WY":101
}

def get_col_data(city: str, state: str, static: dict) -> tuple[float, float]:
    """Returns (colIndex, groceryIndex). Checks meric_col.json first, falls back to state."""
    meric = static.get("meric", {})
    city_key = f"{city.lower()}_{state}"
    if city_key in meric:
        entry = meric[city_key]
        return float(entry.get("col", 100)), float(entry.get("grocery", 100))
    # State-level fallback
    col     = float(MERIC_STATE_COL.get(state, 100))
    grocery = float(MERIC_STATE_GROCERY.get(state, 100))
    return col, grocery

# ── Confidence level helper ──────────────────────────────────────────────────

def confidence_index(level: str) -> int:
    """Maps 'high'|'moderate'|'limited' → 0|1|2 matching Flutter DataConfidence.index"""
    return {"high": 0, "moderate": 1, "limited": 2}.get(level, 1)

# ── Build single city record ──────────────────────────────────────────────────

def build_city_record(
    city: str,
    state: str,
    census: dict,
    static: dict,
    run_ts: str,
) -> dict | None:
    """Assemble a complete CityData-compatible JSON record for one city."""

    # Census data — required for Free tier
    cdata = census.get(city.lower())
    if not cdata:
        log.debug(f"No Census data for {city}, {state} — skipping")
        return None

    population = cdata["population"]
    if population < CONFIG["min_population"]:
        return None

    # BLS
    unemp, wage_growth = get_bls_data(state)

    # Crime
    violent, prop_crime, crime_conf = get_crime_data(city, state, population)

    # NOAA
    hdd, cdd = get_climate_data(city, state)

    # Walk Score
    walk, transit_idx = fetch_walkscore(city, state)

    # COL
    col_idx, grocery_idx = get_col_data(city, state, static)

    # Static lookups
    taxes     = static.get("taxes", {})
    sales     = static.get("sales", {})
    energy    = static.get("energy", {})
    utils     = static.get("utils", {})
    transit   = static.get("transit", {})

    state_income_tax = float(taxes.get(state, 5.0))
    sales_info       = sales.get(state, {})
    combined_sales   = float(sales_info.get("combined", 7.0))
    local_sales      = float(sales_info.get("avg_local", 2.0))

    gas_price        = float(energy.get("gas_by_state", {}).get(state, 3.20))
    kwh_rate         = float(energy.get("electricity_cents_per_kwh", {}).get(state, 13.0))
    monthly_kwh      = float(energy.get("avg_monthly_kwh_by_state", {}).get(state, 900))
    electricity_bill = round(kwh_rate * monthly_kwh / 100, 2)

    water_bill       = float(utils.get("water_sewer_trash", {}).get(state, 70))
    broadband        = float(utils.get("broadband_monthly", {}).get(state, 65))

    # Transit fares — check city-specific first, then state default
    transit_cities   = transit.get("cities", {})
    transit_defaults = transit.get("state_defaults", {})
    city_transit_key = f"{city}_{state}"
    t_info = transit_cities.get(city_transit_key) or transit_defaults.get(state, {})
    transit_fare     = float(t_info.get("round_trip", 2.00))
    transit_pass     = t_info.get("monthly_pass")   # None is valid (nullable)
    rideshare        = t_info.get("ride_share_5")   # None is valid (nullable)

    # Overall confidence
    if population >= 100000:
        overall_conf = "high"
    elif population >= 50000:
        overall_conf = "moderate"
    else:
        overall_conf = "limited"

    record = {
        "name":                    city,
        "state":                   STATE_NAMES.get(state, state),
        "stateAbbr":               state,
        "population":              population,
        # Housing
        "medianHomePrice":         round(cdata["medianHomePrice"], 2),
        "medianMonthlyRent":       round(cdata["medianMonthlyRent"], 2),
        "effectivePropertyTaxRate":round(cdata["effectivePropertyTaxRate"], 2),
        # COL
        "colIndex":                round(col_idx, 1),
        "groceryIndex":            round(grocery_idx, 1),
        "gasPricePerGallon":       round(gas_price, 2),
        # Utilities
        "monthlyElectricityBill":  round(electricity_bill, 2),
        "monthlyWaterSewerTrash":  round(water_bill, 2),
        "monthlyBroadband":        round(broadband, 2),
        # Transit
        "transitRoundTripFare":    round(transit_fare, 2),
        "transitMonthlyPass":      round(float(transit_pass), 2) if transit_pass else None,
        "rideShare5MileCost":      round(float(rideshare), 2) if rideshare else None,
        # Taxes
        "stateIncomeTaxTopRate":   round(state_income_tax, 2),
        "effectiveSalesTaxRate":   round(combined_sales, 2),
        "localSalesTaxRate":       round(local_sales, 2),
        # Opportunity
        "unemploymentRate":        round(unemp, 1),
        "yoyWageGrowthRate":       round(wage_growth, 1),
        "medianHouseholdIncome":   round(cdata["medianHouseholdIncome"], 2),
        # Safety
        "violentCrimeRatePer100k": round(violent, 1),
        "propertyCrimeRatePer100k":round(prop_crime, 1),
        # Lifestyle
        "walkabilityScore":        round(walk, 1),
        "avgCommuteMinutes":       round(cdata["avgCommuteMinutes"], 1),
        "heatingDegreeDays":       round(hdd, 1),
        "coolingDegreeDays":       round(cdd, 1),
        "transitAccessibilityIndex": round(transit_idx, 1),
        # Metadata
        "overallConfidence":       confidence_index(overall_conf),
        "safetyConfidence":        confidence_index(crime_conf),
        "lastUpdated":             run_ts,
    }

    return record

# ── Split free / max records ──────────────────────────────────────────────────

FREE_FIELDS = {
    "name", "state", "stateAbbr", "population",
    "medianHomePrice", "medianMonthlyRent",
    "colIndex", "unemploymentRate",
    "violentCrimeRatePer100k",
    "walkabilityScore", "avgCommuteMinutes",
    "overallConfidence", "safetyConfidence", "lastUpdated",
}

def to_free(record: dict) -> dict:
    return {k: v for k, v in record.items() if k in FREE_FIELDS}

# ── Main pipeline ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("CityCompare Data Pipeline — starting")
    log.info("=" * 60)

    run_ts = datetime.now(timezone.utc).isoformat()
    CONFIG["output_dir"].mkdir(parents=True, exist_ok=True)

    # Load inputs
    cities  = load_cities()
    static  = load_static_data()

    # Group cities by state to batch Census calls
    from collections import defaultdict
    by_state: dict[str, list[str]] = defaultdict(list)
    for city, state in cities:
        by_state[state].append(city)

    log.info(f"Processing {len(cities)} cities across {len(by_state)} states")

    all_records = []
    failed      = []

    for state_abbr in sorted(by_state.keys()):
        state_cities = by_state[state_abbr]
        log.info(f"  Fetching Census ACS for {state_abbr} ({len(state_cities)} cities)...")

        census = fetch_census_acs(state_abbr)
        log.info(f"    → {len(census)} Census place records returned")

        for city in state_cities:
            try:
                record = build_city_record(city, state_abbr, census, static, run_ts)
                if record:
                    all_records.append(record)
                else:
                    failed.append(f"{city}, {state_abbr} (no Census match)")
            except Exception as e:
                log.error(f"  Error building {city}, {state_abbr}: {e}")
                failed.append(f"{city}, {state_abbr} (error: {e})")

    log.info(f"\nBuilt {len(all_records)} city records ({len(failed)} skipped)")

    if failed:
        log.warning(f"Skipped cities ({len(failed)}):")
        for f in failed[:20]:
            log.warning(f"  {f}")
        if len(failed) > 20:
            log.warning(f"  ... and {len(failed) - 20} more")

    # Sort by state then city name
    all_records.sort(key=lambda r: (r["stateAbbr"], r["name"]))

    # Validate — every required Free field must be non-null
    valid_records = []
    for r in all_records:
        missing = [f for f in FREE_FIELDS if r.get(f) is None]
        if missing:
            log.warning(f"  Dropping {r['name']}, {r['stateAbbr']}: missing {missing}")
        else:
            valid_records.append(r)

    log.info(f"Validation passed: {len(valid_records)} records")

    # Write cities_max.json — full records
    max_path = CONFIG["output_dir"] / "cities_max.json"
    with open(max_path, "w", encoding="utf-8") as f:
        json.dump(valid_records, f, separators=(",", ":"))
    log.info(f"Wrote {max_path} ({max_path.stat().st_size / 1024:.1f} KB)")

    # Write cities_free.json — Free-tier fields only
    free_records = [to_free(r) for r in valid_records]
    free_path = CONFIG["output_dir"] / "cities_free.json"
    with open(free_path, "w", encoding="utf-8") as f:
        json.dump(free_records, f, separators=(",", ":"))
    log.info(f"Wrote {free_path} ({free_path.stat().st_size / 1024:.1f} KB)")

    # Write manifest
    manifest = {
        "version":     datetime.now(timezone.utc).strftime("data-%Y-%m-%d"),
        "generated":   run_ts,
        "city_count":  len(valid_records),
        "free_fields": sorted(FREE_FIELDS),
        "files": {
            "cities_free": "cities_free.json",
            "cities_max":  "cities_max.json",
        }
    }
    manifest_path = CONFIG["output_dir"] / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    log.info(f"Wrote {manifest_path}")

    log.info("\n" + "=" * 60)
    log.info(f"Pipeline complete. {len(valid_records)} cities ready.")
    log.info(f"Version tag: {manifest['version']}")
    log.info("=" * 60)

    return len(valid_records)

if __name__ == "__main__":
    count = main()
    sys.exit(0 if count > 0 else 1)
