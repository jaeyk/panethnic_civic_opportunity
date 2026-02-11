#!/usr/bin/env python3

import argparse
import os
import sys
from typing import Dict, List

import pandas as pd
import requests


ACS1_START_YEAR = 2005
ACS5_START_YEAR = 2009
CURRENT_YEAR = 2025


def parse_args():
    p = argparse.ArgumentParser(description="Build Asian/Latino population series from Census APIs and optional historical file.")
    p.add_argument("--output", default="outputs/population/population_series.csv")
    p.add_argument("--historical-input", default="raw_data/population_manual_1980_2008.csv")
    p.add_argument("--places-input", default="misc/selected_places.csv")
    p.add_argument("--api-key", default=os.getenv("CENSUS_API_KEY", ""))
    p.add_argument("--skip-api", action="store_true", help="Use only historical/local input files.")
    return p.parse_args()


def call_census_api(url: str, params: Dict[str, str]) -> pd.DataFrame:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if len(data) < 2:
      return pd.DataFrame()
    return pd.DataFrame(data[1:], columns=data[0])


def fetch_national_acs(api_key: str) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for year in range(ACS1_START_YEAR, CURRENT_YEAR + 1):
        url = f"https://api.census.gov/data/{year}/acs/acs1"
        params = {
            "get": "NAME,B03002_001E,B03002_006E,B03002_012E",
            "for": "us:1",
        }
        if api_key:
            params["key"] = api_key

        try:
            df = call_census_api(url, params)
        except Exception:
            continue

        if df.empty:
            continue

        df["year"] = year
        df = df.rename(
            columns={
                "NAME": "geo_name",
                "us": "geo_id",
                "B03002_001E": "population_total",
                "B03002_006E": "population_asian",
                "B03002_012E": "population_latino",
            }
        )
        df["geo_level"] = "national"
        rows.append(df)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True)
    for c in ["population_total", "population_asian", "population_latino"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def fetch_selected_places_acs(places_csv: str, api_key: str) -> pd.DataFrame:
    if not os.path.exists(places_csv):
        return pd.DataFrame()

    places = pd.read_csv(places_csv)
    need = {"state_fips", "place_fips", "geo_name"}
    if not need.issubset(set(places.columns)):
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for year in range(ACS5_START_YEAR, CURRENT_YEAR + 1):
        url = f"https://api.census.gov/data/{year}/acs/acs5"
        for _, row in places.iterrows():
            params = {
                "get": "NAME,B03002_001E,B03002_006E,B03002_012E",
                "for": f"place:{int(row['place_fips']):05d}",
                "in": f"state:{int(row['state_fips']):02d}",
            }
            if api_key:
                params["key"] = api_key

            try:
                df = call_census_api(url, params)
            except Exception:
                continue

            if df.empty:
                continue

            df["year"] = year
            df["geo_name_manual"] = row["geo_name"]
            if "region_focus" in places.columns:
                df["region_focus"] = row.get("region_focus")
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out.rename(
        columns={
            "NAME": "geo_name",
            "B03002_001E": "population_total",
            "B03002_006E": "population_asian",
            "B03002_012E": "population_latino",
            "state": "state_fips",
            "place": "place_fips",
        }
    )
    out["geo_id"] = out["state_fips"].astype(str).str.zfill(2) + out["place_fips"].astype(str).str.zfill(5)
    out["geo_level"] = "place"

    for c in ["population_total", "population_asian", "population_latino"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


def load_historical(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()

    df = pd.read_csv(path)
    needed = {"year", "geo_level", "geo_id", "geo_name", "population_asian", "population_latino"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Historical population file missing columns: {sorted(missing)}")

    if "population_total" not in df.columns:
        df["population_total"] = pd.NA

    return df


def main():
    args = parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    pieces: List[pd.DataFrame] = []

    hist = load_historical(args.historical_input)
    if not hist.empty:
        hist["source"] = hist.get("source", "manual_historical")
        pieces.append(hist)

    if not args.skip_api:
        national = fetch_national_acs(args.api_key)
        if not national.empty:
            national["source"] = "acs1_api"
            pieces.append(national)

        places = fetch_selected_places_acs(args.places_input, args.api_key)
        if not places.empty:
            places["source"] = "acs5_api"
            pieces.append(places)

    if not pieces:
        pd.DataFrame(
            columns=[
                "year",
                "geo_level",
                "geo_id",
                "geo_name",
                "population_total",
                "population_asian",
                "population_latino",
                "source",
            ]
        ).to_csv(args.output, index=False)
        print("No population data retrieved; wrote empty output schema.")
        return

    out = pd.concat(pieces, ignore_index=True)
    out = out.sort_values(["geo_level", "geo_id", "year"]).drop_duplicates(
        subset=["year", "geo_level", "geo_id"], keep="last"
    )

    out.to_csv(args.output, index=False)
    print(f"Wrote {len(out):,} rows to {args.output}")


if __name__ == "__main__":
    main()
