# usdemographic-data

Public data pipeline for [CityCompare](https://github.com/roddhc) — produces city-level demographic, economic, and lifestyle data for ~1,800 U.S. cities.

## What this produces

Two JSON files published as GitHub Releases every Sunday:

| File | Description | Size |
|------|-------------|------|
| `cities_free.json` | Free-tier fields (14 fields × ~1,800 cities) | ~200 KB |
| `cities_max.json` | Full dataset (30+ fields × ~1,800 cities) | ~700 KB |
| `manifest.json` | Version tag, city count, field list | ~2 KB |

## Flutter integration

The CityCompare app checks for new data on launch:

```
GET https://api.github.com/repos/roddhc/usdemographic-data/releases/latest
```

If the release tag is newer than the locally stored version, the app downloads the appropriate JSON file and refreshes its Hive cache.

## Data sources

| Field category | Source | Update frequency |
|---|---|---|
| Population, housing, income, commute | Census Bureau ACS 5-Year | Annual (new release each December) |
| Unemployment | BLS LAUS state averages | Monthly |
| Wage growth | BLS QCEW | Quarterly |
| Crime rates | FBI UCR / state estimates | Annual |
| Climate (HDD/CDD) | NOAA 30-year normals | Every 10 years |
| Gas prices | EIA by state | Weekly average |
| Electricity | EIA Electric Power Monthly | Monthly |
| Walkability, transit | Walk Score API / state defaults | On-demand |
| Cost of living index | MERIC quarterly report | Quarterly |
| Sales taxes | Tax Foundation | Annual |
| State income taxes | Tax Foundation | Annual |
| Utilities, broadband | Static reference files | Annual review |
| Transit fares | Static — top 100 transit cities | As fares change |

## City coverage

~1,800 U.S. incorporated places with population ≥ 25,000.
All 50 states + Washington D.C.
Source: Census Bureau ACS 5-Year 2023.

## Running locally

```bash
pip install -r requirements.txt

# Set your API keys (Census key is already embedded in the script)
export CENSUS_API_KEY=your_key
export BLS_API_KEY=your_key        # optional — uses static fallback
export FBI_API_KEY=your_key        # optional — uses state-level fallback
export NOAA_API_KEY=your_key       # optional — uses state-level fallback
export WALKSCORE_API_KEY=your_key  # optional — uses state-level fallback

python build_cities.py
# Output: output/cities_free.json, output/cities_max.json, output/manifest.json
```

## Adding the MERIC COL data (quarterly manual step)

1. Download the MERIC Cost of Living report from [meric.mo.gov](https://meric.mo.gov/data/cost-living-data-series)
2. Run the parser: `python tools/parse_meric.py meric_report.xlsx`
3. This updates `data/meric_col.json` with city-level COL and grocery indices
4. Commit the updated file and re-run the pipeline

## Repository structure

```
usdemographic-data/
├── build_cities.py              ← main pipeline
├── requirements.txt
├── README.md
├── .github/
│   └── workflows/
│       └── build_data.yml       ← weekly Sunday cron job
├── data/
│   ├── american_cities_expanded_clean.csv   ← city list (1,855 cities)
│   ├── state_taxes.json         ← state income tax rates
│   ├── sales_taxes.json         ← state + local sales tax
│   ├── energy_prices.json       ← gas + electricity by state
│   ├── utilities.json           ← water/sewer/trash + broadband
│   ├── transit_fares.json       ← fares for top ~100 transit cities
│   └── meric_col.json           ← COL index (update quarterly)
└── output/
    ├── cities_free.json         ← generated output
    ├── cities_max.json          ← generated output
    └── manifest.json            ← generated output
```

## GitHub Actions secrets required

Add these in `Settings → Secrets and variables → Actions`:

| Secret | Required | Notes |
|--------|----------|-------|
| `CENSUS_API_KEY` | Yes | Already embedded in script — also set here |
| `BLS_API_KEY` | No | Falls back to static state averages |
| `FBI_API_KEY` | No | Falls back to state-level crime estimates |
| `NOAA_API_KEY` | No | Falls back to state-level climate normals |
| `WALKSCORE_API_KEY` | No | Falls back to state-level walk score estimates |
