"""
build_cities.py — CityCompare Data Pipeline v2
===============================================
Produces cities_free.json and cities_max.json for GitHub Releases.

Phase 1 (this version — all Free-tier fields from real APIs):
  - Census ACS 5-Year: population, medianHomePrice, medianMonthlyRent,
    medianHouseholdIncome, avgCommuteMinutes, effectivePropertyTaxRate
  - BLS API v2: unemploymentRate (LAUS)
  - BLS QCEW public CSV (no key needed): yoyWageGrowthRate
  - FBI Crime Data Explorer: violentCrimeRatePer100k, propertyCrimeRatePer100k
  - NOAA CDO API: heatingDegreeDays, coolingDegreeDays
  - Walk Score API: walkabilityScore (if key available; state defaults otherwise)
  - Static/MERIC: colIndex, groceryIndex
  - Static files: taxes, energy, utilities, transit

Phase 2 (future — Max-only enrichment):
  - EIA real-time gas/electricity prices
  - Walk Score transitAccessibilityIndex for all cities
  - MERIC city-level COL overrides

Usage:
  python build_cities.py

  Required env vars (set as GitHub Actions secrets):
    CENSUS_API_KEY   — api.census.gov/data/key_signup.html
    BLS_API_KEY      — data.bls.gov/registrationEngine/
    FBI_API_KEY      — api.data.gov/signup (same key works for FBI UCR)
    NOAA_API_KEY     — ncei.noaa.gov/cdo-web/token
  Optional:
    WALKSCORE_API_KEY — walkscore.com/professional/api.php
"""

import csv
import json
import os
import re
import sys
import time
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import requests

# ── Configuration ─────────────────────────────────────────────────────────────
# ALL keys come from environment variables only.
# Never hardcode a key in this file — it is a public repository.

CONFIG = {
    "census_key":    os.environ.get("CENSUS_API_KEY",    ""),
    "bls_key":       os.environ.get("BLS_API_KEY",       ""),
    "fbi_key":       os.environ.get("FBI_API_KEY",       ""),
    "noaa_key":      os.environ.get("NOAA_API_KEY",      ""),
    "walkscore_key": os.environ.get("WALKSCORE_API_KEY", ""),
    "min_population": 25000,
    "cities_csv":    Path("data/american_cities_expanded_clean.csv"),
    "output_dir":    Path("output"),
    "data_dir":      Path("data"),
    "request_delay": 0.3,    # seconds between API calls
    "bls_batch_size": 50,    # BLS allows up to 50 series per request
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Free-tier fields ──────────────────────────────────────────────────────────
# These fields are included in cities_free.json.
# All five fields that previously required _fillDefaults() placeholders
# are now included here so Free users see real city-specific data.

FREE_FIELDS = {
    # Identity
    "name", "state", "stateAbbr", "population",
    # Housing (Affordability score)
    "medianHomePrice", "medianMonthlyRent",
    # Cost of living (Affordability score)
    "colIndex",
    # Opportunity score — all three now in Free
    "unemploymentRate",
    "yoyWageGrowthRate",        # was _fillDefaults() placeholder — now real BLS data
    "medianHouseholdIncome",    # was _fillDefaults() placeholder — now real Census data
    # Safety score — both crime fields now in Free
    "violentCrimeRatePer100k",
    "propertyCrimeRatePer100k", # was _fillDefaults() placeholder — now real FBI data
    # Lifestyle score
    "walkabilityScore",
    "avgCommuteMinutes",
    "heatingDegreeDays",        # was _fillDefaults() placeholder — now real NOAA data
    "coolingDegreeDays",        # was _fillDefaults() placeholder — now real NOAA data
    # Metadata
    "overallConfidence", "safetyConfidence", "lastUpdated",
}

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

# BLS LAUS area codes for state-level unemployment (fallback)
# These are the series IDs for state unemployment rates from BLS LAUS
BLS_STATE_LAUS_SERIES = {
    "AL":"LASST010000000000003","AK":"LASST020000000000003","AZ":"LASST040000000000003",
    "AR":"LASST050000000000003","CA":"LASST060000000000003","CO":"LASST080000000000003",
    "CT":"LASST090000000000003","DE":"LASST100000000000003","DC":"LASST110000000000003",
    "FL":"LASST120000000000003","GA":"LASST130000000000003","HI":"LASST150000000000003",
    "ID":"LASST160000000000003","IL":"LASST170000000000003","IN":"LASST180000000000003",
    "IA":"LASST190000000000003","KS":"LASST200000000000003","KY":"LASST210000000000003",
    "LA":"LASST220000000000003","ME":"LASST230000000000003","MD":"LASST240000000000003",
    "MA":"LASST250000000000003","MI":"LASST260000000000003","MN":"LASST270000000000003",
    "MS":"LASST280000000000003","MO":"LASST290000000000003","MT":"LASST300000000000003",
    "NE":"LASST310000000000003","NV":"LASST320000000000003","NH":"LASST330000000000003",
    "NJ":"LASST340000000000003","NM":"LASST350000000000003","NY":"LASST360000000000003",
    "NC":"LASST370000000000003","ND":"LASST380000000000003","OH":"LASST390000000000003",
    "OK":"LASST400000000000003","OR":"LASST410000000000003","PA":"LASST420000000000003",
    "RI":"LASST440000000000003","SC":"LASST450000000000003","SD":"LASST460000000000003",
    "TN":"LASST470000000000003","TX":"LASST480000000000003","UT":"LASST490000000000003",
    "VT":"LASST500000000000003","VA":"LASST510000000000003","WA":"LASST530000000000003",
    "WV":"LASST540000000000003","WI":"LASST550000000000003","WY":"LASST560000000000003",
}

# NOAA station IDs for closest major city per state (for HDD/CDD normals)
# Using GHCND station IDs for the closest NOAA weather station with 30-year normals
NOAA_STATE_STATIONS = {
    "AL":"USW00013895","AK":"USW00026451","AZ":"USW00023183","AR":"USW00013963",
    "CA":"USW00023234","CO":"USW00003017","CT":"USW00014740","DE":"USW00013781",
    "DC":"USW00013743","FL":"USW00012839","GA":"USW00013874","HI":"USW00022521",
    "ID":"USW00024131","IL":"USW00094846","IN":"USW00093819","IA":"USW00094910",
    "KS":"USW00013996","KY":"USW00093820","LA":"USW00012916","ME":"USW00014764",
    "MD":"USW00093721","MA":"USW00014739","MI":"USW00094847","MN":"USW00014922",
    "MS":"USW00013882","MO":"USW00013994","MT":"USW00024143","NE":"USW00014942",
    "NV":"USW00023169","NH":"USW00014745","NJ":"USW00014734","NM":"USW00023050",
    "NY":"USW00094728","NC":"USW00013722","ND":"USW00024011","OH":"USW00014820",
    "OK":"USW00013967","OR":"USW00024229","PA":"USW00014737","RI":"USW00014765",
    "SC":"USW00013880","SD":"USW00024090","TN":"USW00013897","TX":"USW00012960",
    "UT":"USW00024127","VT":"USW00014742","VA":"USW00013740","WA":"USW00024233",
    "WV":"USW00011996","WI":"USW00014839","WY":"USW00024089",
}

# ── Static fallback data (used when API unavailable) ─────────────────────────

BLS_STATE_UNEMPLOYMENT_FALLBACK = {
    "AL":3.2,"AK":4.2,"AZ":3.7,"AR":3.2,"CA":5.3,"CO":3.5,"CT":4.0,
    "DC":5.2,"DE":3.8,"FL":3.4,"GA":3.5,"HI":3.1,"ID":3.1,"IL":4.7,
    "IN":3.3,"IA":2.8,"KS":2.9,"KY":4.3,"LA":3.8,"ME":2.9,"MD":3.2,
    "MA":3.7,"MI":4.0,"MN":3.2,"MS":4.0,"MO":3.5,"MT":2.8,"NE":2.5,
    "NV":5.0,"NH":2.6,"NJ":4.4,"NM":4.2,"NY":4.4,"NC":3.6,"ND":2.2,
    "OH":4.0,"OK":3.2,"OR":4.5,"PA":3.9,"RI":4.2,"SC":3.5,"SD":2.2,
    "TN":3.4,"TX":3.9,"UT":3.1,"VT":2.2,"VA":3.0,"WA":4.4,"WV":4.6,
    "WI":3.0,"WY":3.5,
}

BLS_STATE_WAGE_GROWTH_FALLBACK = {
    "AL":4.2,"AK":3.5,"AZ":5.1,"AR":4.0,"CA":3.8,"CO":4.5,"CT":4.8,
    "DC":3.2,"DE":4.1,"FL":4.8,"GA":4.9,"HI":3.6,"ID":4.8,"IL":4.2,
    "IN":4.0,"IA":3.8,"KS":3.9,"KY":4.1,"LA":3.7,"ME":4.5,"MD":3.9,
    "MA":4.3,"MI":4.0,"MN":4.1,"MS":3.8,"MO":4.0,"MT":5.2,"NE":3.9,
    "NV":4.5,"NH":4.4,"NJ":4.0,"NM":4.3,"NY":3.5,"NC":5.0,"ND":3.5,
    "OH":4.0,"OK":4.2,"OR":4.3,"PA":3.9,"RI":4.1,"SC":5.1,"SD":3.8,
    "TN":4.8,"TX":4.5,"UT":5.3,"VT":4.0,"VA":4.2,"WA":4.8,"WV":3.5,
    "WI":3.9,"WY":3.8,
}

FBI_STATE_VIOLENT_FALLBACK = {
    "AL":465,"AK":838,"AZ":476,"AR":595,"CA":499,"CO":413,"CT":199,
    "DC":992,"DE":410,"FL":421,"GA":348,"HI":265,"ID":229,"IL":431,
    "IN":325,"IA":272,"KS":383,"KY":218,"LA":659,"ME":108,"MD":469,
    "MA":328,"MI":427,"MN":252,"MS":273,"MO":498,"MT":360,"NE":298,
    "NV":598,"NH":151,"NJ":248,"NM":780,"NY":379,"NC":365,"ND":290,
    "OH":326,"OK":437,"OR":292,"PA":296,"RI":218,"SC":490,"SD":399,
    "TN":617,"TX":411,"UT":229,"VT":160,"VA":203,"WA":291,"WV":314,
    "WI":295,"WY":234,
}

FBI_STATE_PROPERTY_FALLBACK = {
    "AL":2318,"AK":2676,"AZ":2516,"AR":2619,"CA":2213,"CO":2648,"CT":1351,
    "DC":3578,"DE":1837,"FL":2150,"GA":2147,"HI":2765,"ID":1602,"IL":1691,
    "IN":1787,"IA":1539,"KS":2235,"KY":1778,"LA":2572,"ME":1308,"MD":1668,
    "MA":1264,"MI":1743,"MN":2043,"MS":2098,"MO":2669,"MT":2752,"NE":2185,
    "NV":2834,"NH":1148,"NJ":1275,"NM":3548,"NY":1418,"NC":2272,"ND":1697,
    "OH":1883,"OK":2818,"OR":2866,"PA":1337,"RI":1468,"SC":2703,"SD":1876,
    "TN":2696,"TX":2175,"UT":2451,"VT":1450,"VA":1613,"WA":2933,"WV":1638,
    "WI":1724,"WY":2137,
}

NOAA_STATE_CLIMATE_FALLBACK = {
    # (HDD, CDD) 30-year normals by state
    "AK":(10700,30),"AL":(2700,2580),"AR":(3100,2400),"AZ":(1500,3700),
    "CA":(2800,1200),"CO":(6000,700),"CT":(5500,700),"DC":(4200,1400),
    "DE":(4700,1200),"FL":(700,3800),"GA":(2800,2200),"HI":(0,2800),
    "ID":(5800,700),"IL":(6300,1100),"IN":(5600,1100),"IA":(6600,1100),
    "KS":(5100,1500),"KY":(4600,1400),"LA":(1700,3000),"ME":(7400,400),
    "MD":(4600,1300),"MA":(5600,700),"MI":(6500,700),"MN":(8200,700),
    "MS":(2500,2700),"MO":(5000,1400),"MT":(7700,500),"NE":(6000,1200),
    "NV":(4200,1800),"NH":(7300,400),"NJ":(4900,1100),"NM":(4000,1500),
    "NY":(5700,900),"NC":(3300,1700),"ND":(9000,700),"OH":(5700,1000),
    "OK":(3600,2100),"OR":(4200,400),"PA":(5300,1000),"RI":(5900,600),
    "SC":(2700,2000),"SD":(7500,1000),"TN":(3700,1700),"TX":(2200,3000),
    "UT":(5500,1200),"VT":(7900,400),"VA":(4000,1400),"WA":(4800,300),
    "WV":(5000,1000),"WI":(7300,700),"WY":(7200,500),
}

WALKSCORE_STATE_FALLBACK = {
    "AK":35,"AL":32,"AR":30,"AZ":38,"CA":52,"CO":45,"CT":50,"DC":98,
    "DE":42,"FL":46,"GA":42,"HI":58,"ID":35,"IL":55,"IN":36,"IA":38,
    "KS":38,"KY":38,"LA":52,"ME":40,"MD":52,"MA":62,"MI":42,"MN":50,
    "MS":28,"MO":44,"MT":35,"NE":40,"NV":45,"NH":40,"NJ":58,"NM":38,
    "NY":68,"NC":40,"ND":38,"OH":45,"OK":35,"OR":52,"PA":52,"RI":58,
    "SC":38,"SD":36,"TN":38,"TX":42,"UT":42,"VT":40,"VA":46,"WA":52,
    "WV":28,"WI":45,"WY":30,
}

WALKSCORE_TRANSIT_FALLBACK = {
    "AK":15,"AL":15,"AR":10,"AZ":28,"CA":42,"CO":38,"CT":40,"DC":88,
    "DE":25,"FL":32,"GA":35,"HI":40,"ID":15,"IL":48,"IN":22,"IA":18,
    "KS":18,"KY":20,"LA":30,"ME":18,"MD":52,"MA":62,"MI":28,"MN":42,
    "MS":10,"MO":32,"MT":12,"NE":22,"NV":35,"NH":18,"NJ":52,"NM":22,
    "NY":68,"NC":25,"ND":12,"OH":32,"OK":18,"OR":42,"PA":48,"RI":38,
    "SC":20,"SD":12,"TN":22,"TX":32,"UT":38,"VT":18,"VA":40,"WA":48,
    "WV":12,"WI":32,"WY":10,
}

MERIC_STATE_COL = {
    "AK":129,"AL":88,"AR":87,"AZ":105,"CA":149,"CO":113,"CT":122,
    "DC":150,"DE":108,"FL":103,"GA":93,"HI":186,"ID":103,"IL":98,
    "IN":90,"IA":89,"KS":89,"KY":90,"LA":93,"ME":110,"MD":120,
    "MA":135,"MI":92,"MN":100,"MS":84,"MO":90,"MT":105,"NE":92,
    "NV":103,"NH":118,"NJ":125,"NM":92,"NY":139,"NC":96,"ND":99,
    "OH":93,"OK":88,"OR":115,"PA":102,"RI":119,"SC":94,"SD":94,
    "TN":91,"TX":95,"UT":105,"VT":118,"VA":107,"WA":118,"WV":88,
    "WI":96,"WY":97,
}

MERIC_STATE_GROCERY = {
    "AK":132,"AL":97,"AR":95,"AZ":101,"CA":108,"CO":100,"CT":110,
    "DC":114,"DE":101,"FL":102,"GA":97,"HI":159,"ID":96,"IL":97,
    "IN":95,"IA":95,"KS":94,"KY":94,"LA":98,"ME":107,"MD":107,
    "MA":109,"MI":97,"MN":100,"MS":93,"MO":95,"MT":104,"NE":96,
    "NV":102,"NH":112,"NJ":109,"NM":97,"NY":113,"NC":97,"ND":102,
    "OH":96,"OK":93,"OR":104,"PA":100,"RI":109,"SC":98,"SD":101,
    "TN":96,"TX":95,"UT":97,"VT":109,"VA":101,"WA":107,"WV":94,
    "WI":98,"WY":101,
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
        "taxes":   load_json(d / "state_taxes.json"),
        "sales":   load_json(d / "sales_taxes.json"),
        "energy":  load_json(d / "energy_prices.json"),
        "utils":   load_json(d / "utilities.json"),
        "transit": load_json(d / "transit_fares.json"),
        "meric":   load_json(d / "meric_col.json"),
    }

# ── Load city list ────────────────────────────────────────────────────────────

def load_cities() -> list[tuple[str, str]]:
    path = CONFIG["cities_csv"]
    if not path.exists():
        path = CONFIG["data_dir"] / "american_cities_expanded_clean.csv"
    if not path.exists():
        raise FileNotFoundError(f"City list not found: {CONFIG['cities_csv']}")
    cities = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip().rstrip(",").strip('"').strip('\u201c').strip('\u201d').strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 2:
                city = parts[0].strip().strip('"').strip('\u201c').strip('\u201d')
                state = parts[1].strip().strip('"')
                if len(state) == 2 and state.isalpha():
                    cities.append((city, state))
    log.info(f"Loaded {len(cities)} cities from {path}")
    return cities

# ── Census ACS ────────────────────────────────────────────────────────────────

def fetch_census_acs(state_abbr: str) -> dict:
    """Fetch ACS 5-Year 2023 place-level data for one state."""
    fips = STATE_FIPS.get(state_abbr)
    if not fips:
        return {}

    key = CONFIG["census_key"]
    if not key:
        log.error("CENSUS_API_KEY not set — cannot fetch Census data")
        return {}

    variables = ",".join([
        "NAME",
        "B01003_001E",  # population
        "B25077_001E",  # median home value
        "B25064_001E",  # median gross rent
        "B19013_001E",  # median household income
        "B08303_001E",  # aggregate commute time
        "B08301_001E",  # workers 16+ (commute denominator)
        "B25105_001E",  # median monthly housing costs (property tax proxy)
    ])

    url = (
        f"https://api.census.gov/data/2023/acs/acs5"
        f"?get={variables}"
        f"&for=place:*"
        f"&in=state:{fips}"
        f"&key={key}"
    )

    suffixes = [
        " city and borough"," metro township"," unified government (balance)",
        " consolidated government (balance)"," metro government (balance)",
        " metropolitan government (balance)"," city (balance)"," city",
        " town"," village"," borough"," CDPD"," CDP"," municipality",
    ]

    try:
        resp = requests.get(url, timeout=30,
                           headers={"User-Agent": "CityCompare-Pipeline/2.0"})
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

        agg_time = safe_float(r.get("B08303_001E"))
        workers  = safe_float(r.get("B08301_001E"), 1)
        avg_commute = round(agg_time / workers, 1) if workers > 0 else 27.0

        home_val = safe_float(r.get("B25077_001E"))
        monthly_housing = safe_float(r.get("B25105_001E"))
        if home_val > 0 and monthly_housing > 0:
            prop_tax_rate = round(min((monthly_housing * 12 * 0.28) / home_val * 100, 3.5), 2)
        else:
            prop_tax_rate = 1.0

        result[city_clean.lower()] = {
            "census_name":           city_clean,
            "population":            int(pop),
            "medianHomePrice":       safe_float(r.get("B25077_001E")),
            "medianMonthlyRent":     safe_float(r.get("B25064_001E")),
            "medianHouseholdIncome": safe_float(r.get("B19013_001E")),
            "avgCommuteMinutes":     avg_commute if avg_commute > 5 else 27.0,
            "effectivePropertyTaxRate": prop_tax_rate,
        }

    time.sleep(CONFIG["request_delay"])
    return result

# ── BLS API v2 ────────────────────────────────────────────────────────────────

BLS_STATS = {"unemployment_real": 0, "unemployment_fallback": 0,
             "wage_real": 0, "wage_fallback": 0, "_qcew_debug_count": 0}

def fetch_qcew_wage_growth(state: str) -> float | None:
    """
    Real year-over-year average weekly wage growth (%) for a state, from
    BLS QCEW's public Open Data Access CSV files:
      https://data.bls.gov/cew/data/api/{year}/a/area/{state_fips}000.csv
    This is a public static file — no API key needed, and no fragile
    hand-built series ID (QCEW timeseries series IDs pack area/datatype/
    size/ownership/industry codes into one string, which is easy to get
    subtly wrong). We fetch the state-total, all-industries row
    (own_code=0, industry_code=10) for two years and compute % change
    ourselves from annual_avg_wkly_wage, rather than trust a column name we
    haven't verified exists in the annual file layout.
    Returns None if either year's row can't be found.
    """
    fips = STATE_FIPS.get(state)
    if not fips:
        return None
    area = f"{fips}000"

    def get_avg_weekly_wage(year: str) -> float | None:
        url = f"https://data.bls.gov/cew/data/api/{year}/a/area/{area}.csv"
        try:
            resp = requests.get(url, timeout=15,
                               headers={"User-Agent": "CityCompare-Pipeline/2.0"})
            if resp.status_code != 200:
                if BLS_STATS["_qcew_debug_count"] < 8:
                    log.warning(f"[qcew-debug] {state} {year}: HTTP {resp.status_code}, "
                               f"body starts: {resp.text[:200]!r}")
                    BLS_STATS["_qcew_debug_count"] += 1
                return None
            reader = csv.DictReader(resp.text.splitlines())
            rows_seen = 0
            for row in reader:
                rows_seen += 1
                if row.get("own_code") == "0" and row.get("industry_code") == "10":
                    try:
                        return float(row["annual_avg_wkly_wage"])
                    except (KeyError, ValueError, TypeError) as e:
                        if BLS_STATS["_qcew_debug_count"] < 8:
                            log.warning(f"[qcew-debug] {state} {year}: matched row but "
                                       f"couldn't read annual_avg_wkly_wage ({e!r}). "
                                       f"Row keys: {list(row.keys())}. Row values: {row}")
                            BLS_STATS["_qcew_debug_count"] += 1
                        return None
            if BLS_STATS["_qcew_debug_count"] < 8:
                log.warning(f"[qcew-debug] {state} {year}: got 200 and {rows_seen} rows "
                           f"but none matched own_code=0/industry_code=10. "
                           f"Columns seen: {reader.fieldnames}")
                BLS_STATS["_qcew_debug_count"] += 1
        except Exception as e:
            if BLS_STATS["_qcew_debug_count"] < 8:
                log.warning(f"[qcew-debug] {state} {year} exception: {e!r}")
                BLS_STATS["_qcew_debug_count"] += 1
        return None

    wage_prior = get_avg_weekly_wage("2023")
    time.sleep(CONFIG["request_delay"])
    wage_current = get_avg_weekly_wage("2024")
    time.sleep(CONFIG["request_delay"])

    if wage_prior and wage_current and wage_prior > 0:
        return round((wage_current - wage_prior) / wage_prior * 100, 1)
    return None


def fetch_bls_state_data(states: list[str]) -> dict[str, tuple[float, float]]:
    """
    Fetch unemployment rate (BLS API v2, LAUS series) and wage growth
    (QCEW public CSV, see fetch_qcew_wage_growth) for all states.
    Returns dict: state_abbr -> (unemployment_rate, yoy_wage_growth)
    """
    bls_key = CONFIG["bls_key"]
    if not bls_key:
        log.warning("BLS_API_KEY not set — using static fallback data")
        BLS_STATS["unemployment_fallback"] += len(states)
        BLS_STATS["wage_fallback"] += len(states)
        return {
            s: (BLS_STATE_UNEMPLOYMENT_FALLBACK.get(s, 4.0),
                BLS_STATE_WAGE_GROWTH_FALLBACK.get(s, 4.0))
            for s in states
        }

    result = {}

    # Unemployment: LAUS state series (series code ends in 003 = unemployment rate)
    unemp_series = [BLS_STATE_LAUS_SERIES[s] for s in states if s in BLS_STATE_LAUS_SERIES]

    def fetch_bls_batch(series_ids: list[str], label: str) -> dict[str, float]:
        """Fetch a batch of BLS series. Returns series_id -> latest value."""
        url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        payload = {
            "seriesid": series_ids[:50],  # max 50 per request
            "startyear": "2023",
            "endyear": "2024",
            "annualaverage": True,
            "registrationkey": bls_key,
        }
        try:
            resp = requests.post(url, json=payload, timeout=30,
                               headers={"User-Agent": "CityCompare-Pipeline/2.0",
                                        "Content-Type": "application/json"})
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "REQUEST_SUCCEEDED":
                log.warning(f"BLS {label} returned status: {data.get('status')}")
                return {}
            out = {}
            for series in data.get("Results", {}).get("series", []):
                sid = series["seriesID"]
                # Get the most recent annual average value
                for item in series.get("data", []):
                    if item.get("period") == "M13":  # M13 = annual average
                        try:
                            out[sid] = float(item["value"])
                            break
                        except (ValueError, KeyError):
                            pass
                # Fallback: get the most recent monthly value
                if sid not in out and series.get("data"):
                    try:
                        out[sid] = float(series["data"][0]["value"])
                    except (ValueError, KeyError, IndexError):
                        pass
            log.info(f"BLS {label}: retrieved {len(out)}/{len(series_ids)} series")
            return out
        except Exception as e:
            log.error(f"BLS {label} fetch failed: {e}")
            return {}

    # Fetch unemployment
    unemp_data = fetch_bls_batch(unemp_series, "unemployment")

    for state in states:
        laus_sid = BLS_STATE_LAUS_SERIES.get(state, "")
        if laus_sid in unemp_data:
            unemp = unemp_data[laus_sid]
            BLS_STATS["unemployment_real"] += 1
        else:
            unemp = BLS_STATE_UNEMPLOYMENT_FALLBACK.get(state, 4.0)
            BLS_STATS["unemployment_fallback"] += 1

        # Wage growth: real QCEW fetch, two years, computed ourselves
        wage_growth = fetch_qcew_wage_growth(state)
        if wage_growth is not None:
            BLS_STATS["wage_real"] += 1
        else:
            wage_growth = BLS_STATE_WAGE_GROWTH_FALLBACK.get(state, 4.0)
            BLS_STATS["wage_fallback"] += 1

        result[state] = (round(unemp, 1), round(wage_growth, 1))

    log.info(f"BLS unemployment: {BLS_STATS['unemployment_real']} real, "
            f"{BLS_STATS['unemployment_fallback']} fallback")
    log.info(f"QCEW wage growth: {BLS_STATS['wage_real']} real, "
            f"{BLS_STATS['wage_fallback']} fallback")
    return result

# ── FBI Crime Data Explorer ───────────────────────────────────────────────────

FBI_STATS = {"violent_real": 0, "violent_fallback": 0,
             "property_real": 0, "property_fallback": 0,
             "circuit_tripped": False}

FBI_STATS_DEBUG_COUNT = {"n": 0}

def _fetch_fbi_offense_rate(state: str, offense: str, fbi_key: str) -> float | None:
    """
    Fetch one offense category's crime rate per 100k for one state from
    the FBI CDE API. Returns None on any failure so the caller can fall
    back. Retries once on connection-level failures (short backoff).

    Date range must be MM-YYYY per state (confirmed from the API's own
    400 error body: "From year and month date is not valid, expected
    format MM-YYYY."). A bare year (e.g. "2022") was silently invalid.
    Covers the full 2022 calendar year: 01-2022 through 12-2022.

    Response shape (confirmed from a real 200 body, not assumed):
    {"offenses": {"rates": {"<State Name> Offenses": {"01-2022": 59.57,
    ..., "12-2022": 54.53}, "<State Name> Clearances": {...},
    "United States Offenses": {...}, "United States Clearances": {...}}}}
    Values are monthly rates per 100k, keyed by full state name (not
    abbreviation) — summing the 12 months gives the annual rate.
    """
    url = (f"https://api.usa.gov/crime/fbi/cde/summarized/state/{state}/{offense}"
           f"?from=01-2022&to=12-2022&API_KEY={fbi_key}")
    for attempt in range(2):
        try:
            resp = requests.get(url, timeout=15,
                               headers={"User-Agent": "CityCompare-Pipeline/2.0"})
            if resp.status_code == 200:
                data = resp.json()
                try:
                    state_name = STATE_NAMES.get(state, state)
                    rates = data["offenses"]["rates"]
                    monthly = rates.get(f"{state_name} Offenses", {})
                    annual_rate = sum(float(v) for v in monthly.values())
                    return annual_rate if annual_rate > 0 else None
                except (KeyError, TypeError, ValueError) as e:
                    if FBI_STATS_DEBUG_COUNT["n"] < 6:
                        log.warning(f"[fbi-debug] {state} {offense}: got 200 but "
                                   f"couldn't extract monthly rates ({e!r}). "
                                   f"raw body: {resp.text[:500]!r}")
                        FBI_STATS_DEBUG_COUNT["n"] += 1
                    return None
            else:
                log.warning(f"FBI API returned {resp.status_code} for {state} "
                           f"{offense} (attempt {attempt + 1}/2)")
                if resp.status_code == 400 and FBI_STATS_DEBUG_COUNT["n"] < 6:
                    log.warning(f"[fbi-debug] {state} {offense} "
                               f"400 body: {resp.text[:300]!r}")
                    FBI_STATS_DEBUG_COUNT["n"] += 1
        except Exception as e:
            log.warning(f"FBI fetch failed for {state} {offense}: {e} "
                       f"(attempt {attempt + 1}/2)")
        if attempt == 0:
            time.sleep(2)
    return None


def fetch_fbi_state_data(states: list[str]) -> dict[str, tuple[float, float, str]]:
    """
    Fetch violent and property crime rates per 100k population for all states.
    Returns: state_abbr -> (violent_per_100k, property_per_100k, confidence)

    FBI CDE API endpoint: /api/summarized/state/{state_abbr}/{offense}
    Uses api.data.gov key (same key works for FBI CDE). Violent and
    property crime are separate offense-specific endpoints, so each is
    fetched with its own call rather than assumed from one response.

    Circuit breaker: if the first CIRCUIT_BREAKER_THRESHOLD states in a
    row completely fail (both offenses, all attempts), we stop making
    live HTTP calls for the rest of this run and go straight to fallback.
    This was originally added while the backend appeared to be down for
    hours at a time — the actual root cause turned out to be a date
    format bug (from/to needed MM-YYYY, not a bare year), now fixed in
    _fetch_fbi_offense_rate. Keeping the breaker as a safety net for any
    genuine future outage, so a bad run can't silently burn ~40 minutes
    retrying every state against a service that's actually down.
    """
    fbi_key = CONFIG["fbi_key"]
    if not fbi_key:
        log.warning("FBI_API_KEY not set — using static fallback data")
        FBI_STATS["violent_fallback"] += len(states)
        FBI_STATS["property_fallback"] += len(states)
        return {
            s: (float(FBI_STATE_VIOLENT_FALLBACK.get(s, 380)),
                float(FBI_STATE_PROPERTY_FALLBACK.get(s, 1954)),
                "moderate")
            for s in states
        }

    result = {}
    CIRCUIT_BREAKER_THRESHOLD = 5
    consecutive_full_failures = 0

    for state in states:
        if FBI_STATS["circuit_tripped"]:
            violent, prop = None, None
        else:
            violent = _fetch_fbi_offense_rate(state, "violent-crime", fbi_key)
            time.sleep(CONFIG["request_delay"])
            prop = _fetch_fbi_offense_rate(state, "property-crime", fbi_key)
            time.sleep(CONFIG["request_delay"])

            if violent is None and prop is None:
                consecutive_full_failures += 1
                if consecutive_full_failures >= CIRCUIT_BREAKER_THRESHOLD:
                    FBI_STATS["circuit_tripped"] = True
                    log.warning(
                        f"FBI CDE: {CIRCUIT_BREAKER_THRESHOLD} states in a row "
                        f"failed completely — backend looks fully down. "
                        f"Switching to fallback for all remaining states "
                        f"rather than continuing to retry."
                    )
            else:
                consecutive_full_failures = 0

        if violent is not None:
            FBI_STATS["violent_real"] += 1
        else:
            violent = float(FBI_STATE_VIOLENT_FALLBACK.get(state, 380))
            FBI_STATS["violent_fallback"] += 1

        if prop is not None:
            FBI_STATS["property_real"] += 1
        else:
            prop = float(FBI_STATE_PROPERTY_FALLBACK.get(state, 1954))
            FBI_STATS["property_fallback"] += 1

        result[state] = (round(violent, 1), round(prop, 1), "moderate")

    log.info(f"FBI violent crime: {FBI_STATS['violent_real']} real, "
            f"{FBI_STATS['violent_fallback']} fallback")
    log.info(f"FBI property crime: {FBI_STATS['property_real']} real, "
            f"{FBI_STATS['property_fallback']} fallback")
    return result

# ── NOAA Climate Data Online ──────────────────────────────────────────────────

NOAA_STATS = {"real": 0, "fallback": 0}

def fetch_noaa_climate_data(states: list[str]) -> dict[str, tuple[float, float]]:
    """
    Fetch 30-year climate normals (HDD and CDD) per state via NOAA CDO API.
    Returns: state_abbr -> (heatingDegreeDays, coolingDegreeDays)

    Uses the NOAA CDO /data endpoint with datatypes HDD and CDD.
    Station IDs are from NOAA_STATE_STATIONS — one representative station per state.
    """
    noaa_key = CONFIG["noaa_key"]
    if not noaa_key:
        log.warning("NOAA_API_KEY not set — using static climate fallback data")
        NOAA_STATS["fallback"] += len(states)
        return {
            s: (float(NOAA_STATE_CLIMATE_FALLBACK.get(s, (4500, 1500))[0]),
                float(NOAA_STATE_CLIMATE_FALLBACK.get(s, (4500, 1500))[1]))
            for s in states
        }

    result = {}
    base = "https://www.ncei.noaa.gov/cdo-web/api/v2"
    headers = {
        "token": noaa_key,
        "User-Agent": "CityCompare-Pipeline/2.0",
    }

    for state in states:
        station_id = NOAA_STATE_STATIONS.get(state)
        if not station_id:
            hdd, cdd = NOAA_STATE_CLIMATE_FALLBACK.get(state, (4500, 1500))
            result[state] = (float(hdd), float(cdd))
            NOAA_STATS["fallback"] += 1
            continue

        try:
            # Fetch annual HDD normals (30-year: use recent year as proxy)
            # NOAA CDO normals dataset: NORMAL_ANN
            url = f"{base}/data"
            params = {
                "datasetid": "NORMAL_ANN",
                "stationid": f"GHCND:{station_id}",
                "datatypeid": "ANN-HTDD-NORMAL,ANN-CLDD-NORMAL",
                "startdate": "2010-01-01",
                "enddate": "2010-12-31",
                "units": "standard",
                "limit": 10,
            }
            resp = requests.get(url, params=params, headers=headers, timeout=15)

            hdd_val = None
            cdd_val = None

            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("results", []):
                    dtype = item.get("datatype", "")
                    val = item.get("value")
                    if val is not None:
                        try:
                            if "HTDD" in dtype:
                                hdd_val = float(val)
                            elif "CLDD" in dtype:
                                cdd_val = float(val)
                        except (ValueError, TypeError):
                            pass

            if hdd_val is None or cdd_val is None:
                fb = NOAA_STATE_CLIMATE_FALLBACK.get(state, (4500, 1500))
                hdd_val = hdd_val or float(fb[0])
                cdd_val = cdd_val or float(fb[1])
                log.debug(f"NOAA partial fallback for {state}: HDD={hdd_val}, CDD={cdd_val}")
                NOAA_STATS["fallback"] += 1
            else:
                NOAA_STATS["real"] += 1

            result[state] = (round(hdd_val, 1), round(cdd_val, 1))
            time.sleep(CONFIG["request_delay"])

        except Exception as e:
            log.warning(f"NOAA fetch failed for {state}: {e} — using fallback")
            fb = NOAA_STATE_CLIMATE_FALLBACK.get(state, (4500, 1500))
            result[state] = (float(fb[0]), float(fb[1]))
            NOAA_STATS["fallback"] += 1

    log.info(f"NOAA climate data: {NOAA_STATS['real']} real, "
            f"{NOAA_STATS['fallback']} fallback (of {len(states)} states)")
    return result

# ── Geocoding (uszipcode — offline city-centroid lookup) ──────────────────────
# NOTE: An earlier version of this used the Census Bureau's address geocoder
# (geocoding.geo.census.gov/geocoder/locations/onelineaddress). That endpoint
# is a street-address range locator — it requires a house number and street
# name to interpolate a point, and cannot resolve a bare "City, State" query.
# Verified: it returned zero matches across all 1,844 cities in this dataset.
#
# uszipcode ships a bundled/cached SQLite database of every US ZIP code with
# its centroid, and can look up a city's representative coordinates by name
# + state with no per-request network call. Verified locally against this
# exact city list: 1,748/1,855 (94.2%) resolve to real coordinates; the
# remainder (mostly small CDPs/unincorporated communities not in that DB)
# fall through to the existing WALKSCORE_STATE_FALLBACK constants below,
# same as before this change.

from uszipcode import SearchEngine

_ZIP_ENGINE = SearchEngine()
_GEOCODE_CACHE: dict[str, tuple[float, float] | None] = {}

def geocode_city(city: str, state: str) -> tuple[float | None, float | None]:
    """Returns (lat, lon) for a city centroid, or (None, None) if it can't
    be resolved. Cached per city+state for the run."""
    cache_key = f"{city.lower()}_{state}"
    if cache_key in _GEOCODE_CACHE:
        cached = _GEOCODE_CACHE[cache_key]
        return cached if cached else (None, None)

    try:
        results = _ZIP_ENGINE.by_city_and_state(city, state)
        if results:
            lat, lon = float(results[0].lat), float(results[0].lng)
            _GEOCODE_CACHE[cache_key] = (lat, lon)
            return lat, lon
    except Exception as e:
        log.debug(f"Geocoding failed for {city}, {state}: {e}")

    _GEOCODE_CACHE[cache_key] = None
    return None, None

# ── Walk Score ────────────────────────────────────────────────────────────────

WALKSCORE_STATS = {"real": 0, "fallback": 0, "status_counts": {}}

def fetch_walkscore(city: str, state: str,
                    lat: float = None, lon: float = None) -> tuple[float, float]:
    """Returns (walkabilityScore, transitAccessibilityIndex)."""
    ws_key = CONFIG.get("walkscore_key", "")
    if ws_key and lat and lon:
        try:
            import urllib.parse
            addr = urllib.parse.quote(f"{city}, {state} USA")
            url = (f"https://api.walkscore.com/score?format=json"
                   f"&address={addr}&lat={lat}&lon={lon}"
                   f"&transit=1&wsapikey={ws_key}")
            resp = requests.get(url, timeout=10,
                               headers={"User-Agent": "CityCompare-Pipeline/2.0"})
            time.sleep(CONFIG["request_delay"])
            if resp.status_code == 200:
                d = resp.json()
                status = d.get("status")
                WALKSCORE_STATS["status_counts"][str(status)] = (
                    WALKSCORE_STATS["status_counts"].get(str(status), 0) + 1)
                # Only status 1 means a real score was returned. Status 2 =
                # still calculating, 30/31 = bad coords / server error,
                # 40/41/42 = key/quota/IP problems. Any of those leave
                # "walkscore" absent or meaningless — treating it as 0
                # would silently fabricate a fake score, so fall through
                # to the state default instead.
                if status == 1:
                    raw_walk = d.get("walkscore")
                    if raw_walk is not None and float(raw_walk) > 0:
                        walk = float(raw_walk)
                        transit = float((d.get("transit") or {}).get("score") or 0)
                        WALKSCORE_STATS["real"] += 1
                        return walk, transit
                    else:
                        # status says "success" but there's no usable score
                        # (missing or literally 0 — no real US city legitimately
                        # scores 0). Print the raw payload for the first few
                        # cases so we can see exactly what came back, instead
                        # of silently trusting it or guessing again.
                        n = WALKSCORE_STATS.get("_zero_logged", 0)
                        if n < 8:
                            print(f"[walkscore-debug] {city}, {state} "
                                  f"lat={lat} lon={lon} raw_response={d}")
                            WALKSCORE_STATS["_zero_logged"] = n + 1
        except Exception as e:
            log.debug(f"Walk Score failed for {city}, {state}: {e}")

    WALKSCORE_STATS["fallback"] += 1
    return (float(WALKSCORE_STATE_FALLBACK.get(state, 40)),
            float(WALKSCORE_TRANSIT_FALLBACK.get(state, 25)))

# ── COL data ──────────────────────────────────────────────────────────────────

def get_col_data(city: str, state: str, meric: dict) -> tuple[float, float]:
    city_key = f"{city.lower()}_{state}"
    if city_key in meric:
        entry = meric[city_key]
        return float(entry.get("col", 100)), float(entry.get("grocery", 100))
    return float(MERIC_STATE_COL.get(state, 100)), float(MERIC_STATE_GROCERY.get(state, 100))

# ── Confidence ────────────────────────────────────────────────────────────────

def confidence_index(level: str) -> int:
    return {"high": 0, "moderate": 1, "limited": 2}.get(level, 1)

# ── Build single city record ──────────────────────────────────────────────────

def build_city_record(
    city: str,
    state: str,
    census: dict,
    bls: tuple[float, float],
    crime: tuple[float, float, str],
    climate: tuple[float, float],
    static: dict,
    run_ts: str,
) -> dict | None:

    cdata = census.get(city.lower())
    if not cdata:
        return None

    population = cdata["population"]
    if population < CONFIG["min_population"]:
        return None

    unemp, wage_growth = bls
    violent, prop_crime, crime_conf = crime
    hdd, cdd = climate

    geo_lat, geo_lon = geocode_city(city, state)
    walk, transit_idx = fetch_walkscore(city, state, geo_lat, geo_lon)
    col_idx, grocery_idx = get_col_data(city, state, static.get("meric", {}))

    taxes   = static.get("taxes", {})
    sales   = static.get("sales", {})
    energy  = static.get("energy", {})
    utils   = static.get("utils", {})
    transit = static.get("transit", {})

    state_income_tax = float(taxes.get(state, 5.0))
    sales_info       = sales.get(state, {})
    combined_sales   = float(sales_info.get("combined", 7.0))
    local_sales      = float(sales_info.get("avg_local", 2.0))

    gas_price        = float(energy.get("gas_by_state", {}).get(state, 3.20))
    kwh_rate         = float(energy.get("electricity_cents_per_kwh", {}).get(state, 13.0))
    monthly_kwh      = float(energy.get("avg_monthly_kwh_by_state", {}).get(state, 900))
    electricity_bill = round(kwh_rate * monthly_kwh / 100, 2)

    water_bill  = float(utils.get("water_sewer_trash", {}).get(state, 70))
    broadband   = float(utils.get("broadband_monthly", {}).get(state, 65))

    transit_cities   = transit.get("cities", {})
    transit_defaults = transit.get("state_defaults", {})
    t_info = transit_cities.get(f"{city}_{state}") or transit_defaults.get(state, {})
    transit_fare = float(t_info.get("round_trip", 2.00))
    transit_pass = t_info.get("monthly_pass")
    rideshare    = t_info.get("ride_share_5")

    if population >= 100000:
        overall_conf = "high"
    elif population >= 50000:
        overall_conf = "moderate"
    else:
        overall_conf = "limited"

    # Cities under 50k use state-level crime data — flag as limited
    if population < 50000:
        crime_conf = "limited"

    return {
        "name":                     city,
        "state":                    STATE_NAMES.get(state, state),
        "stateAbbr":                state,
        "population":               population,
        "medianHomePrice":          round(cdata["medianHomePrice"], 2),
        "medianMonthlyRent":        round(cdata["medianMonthlyRent"], 2),
        "effectivePropertyTaxRate": round(cdata["effectivePropertyTaxRate"], 2),
        "colIndex":                 round(col_idx, 1),
        "groceryIndex":             round(grocery_idx, 1),
        "gasPricePerGallon":        round(gas_price, 2),
        "monthlyElectricityBill":   round(electricity_bill, 2),
        "monthlyWaterSewerTrash":   round(water_bill, 2),
        "monthlyBroadband":         round(broadband, 2),
        "transitRoundTripFare":     round(transit_fare, 2),
        "transitMonthlyPass":       round(float(transit_pass), 2) if transit_pass else None,
        "rideShare5MileCost":       round(float(rideshare), 2) if rideshare else None,
        "stateIncomeTaxTopRate":    round(state_income_tax, 2),
        "effectiveSalesTaxRate":    round(combined_sales, 2),
        "localSalesTaxRate":        round(local_sales, 2),
        "unemploymentRate":         round(unemp, 1),
        "yoyWageGrowthRate":        round(wage_growth, 1),
        "medianHouseholdIncome":    round(cdata["medianHouseholdIncome"], 2),
        "violentCrimeRatePer100k":  round(violent, 1),
        "propertyCrimeRatePer100k": round(prop_crime, 1),
        "walkabilityScore":         round(walk, 1),
        "avgCommuteMinutes":        round(cdata["avgCommuteMinutes"], 1),
        "heatingDegreeDays":        round(hdd, 1),
        "coolingDegreeDays":        round(cdd, 1),
        "transitAccessibilityIndex": round(transit_idx, 1),
        "overallConfidence":        confidence_index(overall_conf),
        "safetyConfidence":         confidence_index(crime_conf),
        "lastUpdated":              run_ts,
    }

def to_free(record: dict) -> dict:
    return {k: v for k, v in record.items() if k in FREE_FIELDS}

# ── Release quality gate ───────────────────────────────────────────────────────
#
# Every other check in this pipeline (circuit breakers, fallback constants,
# "Verify output files exist" in the workflow) makes sure a run finishes
# without crashing. None of them check whether the run was actually GOOD.
# On 2026-07-15, Walk Score silently went from 1,531/1,844 real to 0/1,844
# real (daily quota exceeded, status 41) and still published as "latest"
# because the file existed and had the right shape — it just had the wrong
# data in it. This gate compares this run's per-source real-data fraction
# against the currently-published release and refuses to let a regressed
# run become the new "latest" release. The Flutter app always pulls
# "latest", so this is the last line of defense before bad data ships.

PREVIOUS_MANIFEST_URL = (
    "https://github.com/roddhc/usdemographic-data/releases/latest/download/manifest.json"
)

# How far a source's real-data fraction is allowed to fall, run-over-run,
# before the gate blocks the release. 0.40 means e.g. 83% real -> 0% real
# (exactly the Walk Score case) trips it, but normal noise (50/51 -> 49/51
# states) doesn't.
QUALITY_GATE_MAX_DROP = 0.40


def _source_real_fraction(source_stats: dict) -> float | None:
    """
    Given one manifest['api_sources'][<name>] dict, return the fraction of
    records that were real (not fallback), or None if this source's stats
    don't have a recognizable real/fallback pair (e.g. "census", which is
    just a string) — the gate skips those rather than guessing.
    """
    pairs = [
        ("cities_real", "cities_fallback"),
        ("states_real", "states_fallback"),
        ("unemployment_states_real", "unemployment_states_fallback"),
        ("wage_growth_states_real", "wage_growth_states_fallback"),
        ("violent_states_real", "violent_states_fallback"),
        ("property_states_real", "property_states_fallback"),
    ]
    for real_key, fallback_key in pairs:
        if isinstance(source_stats, dict) and real_key in source_stats:
            real = source_stats[real_key]
            fallback = source_stats.get(fallback_key, 0)
            total = real + fallback
            return (real / total) if total else None
    return None


def run_quality_gate(manifest: dict) -> None:
    """
    Fetches the manifest.json from the currently-published "latest"
    release and compares it against this run's manifest, source by
    source. If any source's real-data fraction dropped by more than
    QUALITY_GATE_MAX_DROP, logs exactly what regressed and exits 1 so
    the workflow stops before the "Create GitHub Release" step —
    the previous (good) release stays live instead of being overwritten.

    Fails open (does not block) if there's no previous release yet, or
    if the previous manifest can't be fetched at all — a network hiccup
    on our side reaching GitHub shouldn't be treated the same as an
    actual data regression.
    """
    try:
        resp = requests.get(PREVIOUS_MANIFEST_URL, timeout=15)
        if resp.status_code != 200:
            log.info("Quality gate: no previous release manifest found "
                      "(first run?) — nothing to compare against, skipping.")
            return
        previous = resp.json()
    except Exception as e:
        log.warning(f"Quality gate: couldn't fetch previous manifest ({e}) — "
                    f"skipping rather than blocking a release over our own "
                    f"network issue.")
        return

    prev_sources = previous.get("api_sources", {})
    curr_sources = manifest.get("api_sources", {})
    regressions = []

    for name, curr_stats in curr_sources.items():
        prev_stats = prev_sources.get(name)
        if prev_stats is None:
            continue
        curr_frac = _source_real_fraction(curr_stats)
        prev_frac = _source_real_fraction(prev_stats)
        if curr_frac is None or prev_frac is None:
            continue
        drop = prev_frac - curr_frac
        if drop > QUALITY_GATE_MAX_DROP:
            regressions.append(
                f"{name}: real data fraction dropped from {prev_frac:.0%} "
                f"to {curr_frac:.0%} versus the currently-live release"
            )

    if regressions:
        log.error("=" * 60)
        log.error("QUALITY GATE FAILED — refusing to publish this run as "
                   "the new release:")
        for r in regressions:
            log.error(f"  - {r}")
        log.error("Likely cause: an exhausted API quota, an expired/invalid "
                   "key, or a broken endpoint — not a real drop in data "
                   "availability. Fix the cause and re-run. The previous "
                   "release remains live in the meantime, so nothing is "
                   "downgraded for users.")
        log.error("=" * 60)
        sys.exit(1)

    log.info("Quality gate passed — no source regressed beyond "
              f"{QUALITY_GATE_MAX_DROP:.0%} versus the live release.")

# ── Main pipeline ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("CityCompare Data Pipeline v2 — starting")
    log.info("=" * 60)

    # Validate required keys
    missing_keys = []
    for key_name in ("census_key", "bls_key", "fbi_key", "noaa_key"):
        if not CONFIG[key_name]:
            missing_keys.append(key_name.upper().replace("_KEY", "_API_KEY"))
    if missing_keys:
        log.warning(f"Missing API keys (will use fallback data): {', '.join(missing_keys)}")

    run_ts = datetime.now(timezone.utc).isoformat()
    CONFIG["output_dir"].mkdir(parents=True, exist_ok=True)

    cities  = load_cities()
    static  = load_static_data()

    # Group by state
    by_state: dict[str, list[str]] = defaultdict(list)
    for city, state in cities:
        by_state[state].append(city)

    all_states = sorted(by_state.keys())
    log.info(f"Processing {len(cities)} cities across {len(all_states)} states")

    # ── Pre-fetch state-level data in bulk ────────────────────────────────────
    log.info("Fetching BLS unemployment + wage data for all states...")
    bls_data = fetch_bls_state_data(all_states)

    log.info("Fetching FBI crime data for all states...")
    fbi_data = fetch_fbi_state_data(all_states)

    log.info("Fetching NOAA climate data for all states...")
    noaa_data = fetch_noaa_climate_data(all_states)

    # ── Per-state Census fetch + city record build ────────────────────────────
    all_records = []
    failed      = []

    for state_abbr in all_states:
        state_cities = by_state[state_abbr]
        log.info(f"  Census ACS: {state_abbr} ({len(state_cities)} cities)...")

        census = fetch_census_acs(state_abbr)
        log.info(f"    → {len(census)} Census place records")

        bls   = bls_data.get(state_abbr, (4.0, 4.0))
        crime = fbi_data.get(state_abbr, (380.0, 1954.0, "moderate"))
        climate = noaa_data.get(state_abbr, (4500.0, 1500.0))

        for city in state_cities:
            try:
                record = build_city_record(
                    city, state_abbr, census, bls, crime, climate, static, run_ts
                )
                if record:
                    all_records.append(record)
                else:
                    failed.append(f"{city}, {state_abbr} (no Census match)")
            except Exception as e:
                log.error(f"  Error building {city}, {state_abbr}: {e}")
                failed.append(f"{city}, {state_abbr} (error: {e})")

    log.info(f"\nBuilt {len(all_records)} city records ({len(failed)} skipped)")
    if failed[:10]:
        log.info("Sample skipped cities:")
        for f in failed[:10]:
            log.info(f"  {f}")

    # Sort
    all_records.sort(key=lambda r: (r["stateAbbr"], r["name"]))

    # Validate Free fields
    valid_records = []
    for r in all_records:
        missing = [f for f in FREE_FIELDS if r.get(f) is None]
        if missing:
            log.warning(f"  Dropping {r['name']}, {r['stateAbbr']}: missing {missing}")
        else:
            valid_records.append(r)

    log.info(f"Validation passed: {len(valid_records)} records")

    # Write cities_max.json
    max_path = CONFIG["output_dir"] / "cities_max.json"
    with open(max_path, "w", encoding="utf-8") as f:
        json.dump(valid_records, f, separators=(",", ":"))
    log.info(f"Wrote {max_path} ({max_path.stat().st_size / 1024:.1f} KB)")

    # Write cities_free.json
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
        "api_sources": {
            "census":    "ACS 5-Year 2023",
            "bls":       {
                "mode": "real API" if CONFIG["bls_key"] else "static fallback",
                "unemployment_states_real":     BLS_STATS["unemployment_real"],
                "unemployment_states_fallback": BLS_STATS["unemployment_fallback"],
                "wage_growth_states_real":      BLS_STATS["wage_real"],
                "wage_growth_states_fallback":  BLS_STATS["wage_fallback"],
            },
            "fbi":       {
                "mode": "real API" if CONFIG["fbi_key"] else "static fallback",
                "circuit_breaker_tripped": FBI_STATS["circuit_tripped"],
                "violent_states_real":      FBI_STATS["violent_real"],
                "violent_states_fallback":  FBI_STATS["violent_fallback"],
                "property_states_real":     FBI_STATS["property_real"],
                "property_states_fallback": FBI_STATS["property_fallback"],
            },
            "noaa":      {
                "mode": "real API" if CONFIG["noaa_key"] else "static fallback",
                "states_real":     NOAA_STATS["real"],
                "states_fallback": NOAA_STATS["fallback"],
            },
            "walkscore": {
                "mode": "real API" if CONFIG["walkscore_key"] else "state defaults",
                "cities_real":     WALKSCORE_STATS["real"],
                "cities_fallback": WALKSCORE_STATS["fallback"],
                "status_counts":   WALKSCORE_STATS["status_counts"],
            },
        },
        "files": {
            "cities_free": "cities_free.json",
            "cities_max":  "cities_max.json",
        }
    }
    manifest_path = CONFIG["output_dir"] / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    log.info(f"Wrote {manifest_path}")

    # Refuses to let this run publish if a source has regressed hard
    # against the release that's currently live (see function docstring).
    run_quality_gate(manifest)

    log.info("\n" + "=" * 60)
    log.info(f"Pipeline complete. {len(valid_records)} cities.")
    log.info(f"Version: {manifest['version']}")
    log.info(f"API sources: {manifest['api_sources']}")
    log.info("=" * 60)

    return len(valid_records)

if __name__ == "__main__":
    count = main()
    sys.exit(0 if count > 0 else 1)
