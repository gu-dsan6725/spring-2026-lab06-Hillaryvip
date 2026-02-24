"""
Week 4 Lab: World Bank Data MCP Server

An MCP server that exposes:
- Resources: Local World Bank indicator data from CSV
- Tools: Live data from REST Countries and World Bank APIs

Transport: Streamable HTTP on port 8765
"""
import json
import logging
from pathlib import Path
from typing import Optional

import httpx
import polars as pl
from mcp.server.fastmcp import FastMCP



# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_FILE: Path = Path(__file__).parent / "data" / "world_bank_indicators.csv"
HOST: str = "127.0.0.1"
PORT: int = 8765

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP(
    "world-bank-server",
    host=HOST,
    port=PORT,
)


# =============================================================================
# PRIVATE HELPER FUNCTIONS
# =============================================================================

def _load_data() -> pl.DataFrame:
    """Load the World Bank indicators CSV file."""
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    return pl.read_csv(DATA_FILE)


def _fetch_rest_countries(country_code: str) -> dict:
    """Fetch country info from REST Countries API."""
    url = f"https://restcountries.com/v3.1/alpha/{country_code}"
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()[0]


def _fetch_world_bank_indicator(
    country_code: str,
    indicator: str,
    year: Optional[int] = None,
) -> list:
    """Fetch indicator from World Bank API."""
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
    params = {"format": "json", "per_page": 100}
    if year:
        params["date"] = str(year)

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if len(data) < 2 or not data[1]:
            return []
        return data[1]


# =============================================================================
# PART 1: RESOURCES (Local Data)
# =============================================================================

@mcp.resource("data://schema")
def get_schema() -> str:
    """
    Return the schema of the World Bank dataset.

    This resource is provided as an example - it's already implemented.
    """
    df = _load_data()
    schema_info = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
    return json.dumps(schema_info, indent=2)


@mcp.resource("data://countries")
def get_countries() -> str:
    df = _load_data()

    countries = (
        df.select(["countryiso3code", "country"])
        .unique()
        .sort("countryiso3code")
    )

    if countries.height == 0:
        return json.dumps({"error": "No countries found"})

    return countries.write_json()


@mcp.resource("data://indicators/{country_code}")
def get_country_indicators(country_code: str) -> str:
    df = _load_data()

    filtered = df.filter(pl.col("countryiso3code") == country_code)

    if filtered.height == 0:
        return json.dumps({"error": f"Country not found: {country_code}"})

    return filtered.write_json()


# =============================================================================
# PART 2: TOOLS (External APIs)
# =============================================================================


@mcp.tool()
def get_country_info(country_code: str) -> dict:
    logger.info(f"Fetching country info for: {country_code}")

    try:
        data = _fetch_rest_countries(country_code)
    except httpx.HTTPStatusError:
        return {"error": f"Country not found: {country_code}"}
    except Exception as e:
        return {"error": str(e)}

    return {
        "name": data.get("name", {}).get("common"),
        "capital": data.get("capital", [None])[0],
        "region": data.get("region"),
        "subregion": data.get("subregion"),
        "languages": list(data.get("languages", {}).values()),
        "currencies": list(data.get("currencies", {}).keys()),
        "population": data.get("population"),
        "flag": data.get("flag"),
    }


@mcp.tool()
def get_live_indicator(
    country_code: str,
    indicator: str,
    year: int = 2022,
) -> dict:

    logger.info(f"Fetching {indicator} for {country_code} in {year}")

    try:
        data = _fetch_world_bank_indicator(country_code, indicator, year)
    except httpx.HTTPStatusError:
        return {"error": f"Invalid request for {country_code} / {indicator}"}
    except Exception as e:
        return {"error": str(e)}

    if not data:
        return {
            "country": country_code,
            "indicator": indicator,
            "year": year,
            "value": None,
            "error": "No data available",
        }

    entry = data[0]

    return {
        "country": country_code,
        "country_name": entry.get("country", {}).get("value"),
        "indicator": indicator,
        "indicator_name": entry.get("indicator", {}).get("value"),
        "year": year,
        "value": entry.get("value"),
    }


@mcp.tool()
def compare_countries(
    country_codes: list[str],
    indicator: str,
    year: int = 2022,
) -> list[dict]:

    logger.info(f"Comparing {indicator} for countries: {country_codes}")

    results = []

    for code in country_codes:
        try:
            result = get_live_indicator(code, indicator, year)
            results.append(result)
        except Exception as e:
            results.append(
                {
                    "country": code,
                    "indicator": indicator,
                    "year": year,
                    "value": None,
                    "error": str(e),
                }
            )

    return results


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    logger.info(f"Starting World Bank MCP Server on http://{HOST}:{PORT}/mcp")
    logger.info(f"Connect with MCP Inspector or test client at http://{HOST}:{PORT}/mcp")
    logger.info("Press Ctrl+C to stop")
    mcp.run(transport="streamable-http")
