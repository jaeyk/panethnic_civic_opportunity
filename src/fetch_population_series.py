#!/usr/bin/env python3

import argparse
import csv
import json
import os
from typing import Dict, List
from urllib.parse import urlencode
from urllib.request import urlopen

def parse_args():
    p = argparse.ArgumentParser(description="Build Asian/Latino population series from Census APIs using an explicit variable map.")
    p.add_argument("--output", default="processed_data/population/population_series.csv")
    p.add_argument("--historical-input", default="raw_data/population_manual_1980_2008.csv")
    p.add_argument("--places-input", default="misc/selected_places.csv")
    p.add_argument("--variable-map", default="misc/census_variable_map.csv")
    p.add_argument("--api-key", default=os.getenv("CENSUS_API_KEY", ""))
    p.add_argument("--skip-api", action="store_true", help="Use only historical/local input files.")
    return p.parse_args()


def to_int(x):
    try:
        return int(x)
    except Exception:
        return None


def read_csv_rows(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: str, rows: List[Dict[str, str]], fieldnames: List[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def call_census_api(url: str, params: Dict[str, str]) -> List[Dict[str, str]]:
    qs = urlencode(params)
    with urlopen(f"{url}?{qs}", timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if len(payload) < 2:
        return []
    header = payload[0]
    rows = []
    for row in payload[1:]:
        rows.append({header[i]: str(row[i]) for i in range(min(len(header), len(row)))})
    return rows


def load_variable_map(path: str) -> List[Dict[str, str]]:
    rows = read_csv_rows(path)
    if not rows:
        return []
    needed = {"source_id", "year_start", "year_end", "geo_level", "dataset", "var_total", "var_asian", "var_latino"}
    if not needed.issubset(set(rows[0].keys())):
        return []
    return rows


def expand_map_rows(varmap_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out = []
    for r in varmap_rows:
        ys = to_int(r.get("year_start", ""))
        ye = to_int(r.get("year_end", ""))
        if ys is None or ye is None:
            continue
        for year in range(ys, ye + 1):
            rr = dict(r)
            rr["year"] = str(year)
            out.append(rr)
    return out


def fetch_national_from_map(varmap_rows: List[Dict[str, str]], api_key: str) -> List[Dict[str, str]]:
    out = []
    for m in varmap_rows:
        if m.get("geo_level") != "national":
            continue
        year = m.get("year", "")
        dataset = m.get("dataset", "")
        vtot = m.get("var_total", "")
        vas = m.get("var_asian", "")
        vlat = m.get("var_latino", "")
        if not year or not dataset or not vtot or not vas or not vlat:
            continue

        url = f"https://api.census.gov/data/{year}/{dataset}"
        params = {"get": f"NAME,{vtot},{vas},{vlat}", "for": "us:1"}
        if api_key:
            params["key"] = api_key

        try:
            rows = call_census_api(url, params)
        except Exception:
            continue

        for r in rows:
            out.append(
                {
                    "year": str(year),
                    "geo_level": "national",
                    "geo_id": r.get("us", "1"),
                    "geo_name": r.get("NAME", "United States"),
                    "population_total": r.get(vtot, ""),
                    "population_asian": r.get(vas, ""),
                    "population_latino": r.get(vlat, ""),
                    "source": f"{dataset}_api",
                    "source_id": m.get("source_id", ""),
                    "source_dataset": dataset,
                    "var_total": vtot,
                    "var_asian": vas,
                    "var_latino": vlat,
                }
            )
    return out


def fetch_selected_places_from_map(places_csv: str, varmap_rows: List[Dict[str, str]], api_key: str) -> List[Dict[str, str]]:
    places = read_csv_rows(places_csv)
    if not places:
        return []

    need = {"state_fips", "place_fips", "geo_name"}
    if not need.issubset(set(places[0].keys())):
        return []

    out = []
    for m in varmap_rows:
        if m.get("geo_level") != "place":
            continue
        year = m.get("year", "")
        dataset = m.get("dataset", "")
        vtot = m.get("var_total", "")
        vas = m.get("var_asian", "")
        vlat = m.get("var_latino", "")
        if not year or not dataset or not vtot or not vas or not vlat:
            continue

        url = f"https://api.census.gov/data/{year}/{dataset}"
        for p in places:
            sf = to_int(p.get("state_fips", ""))
            pf = to_int(p.get("place_fips", ""))
            if sf is None or pf is None:
                continue
            params = {
                "get": f"NAME,{vtot},{vas},{vlat}",
                "for": f"place:{pf:05d}",
                "in": f"state:{sf:02d}",
            }
            if api_key:
                params["key"] = api_key
            try:
                rows = call_census_api(url, params)
            except Exception:
                continue
            for r in rows:
                out.append(
                    {
                        "year": str(year),
                        "geo_level": "place",
                        "geo_id": f"{r.get('state',''):0>2}{r.get('place',''):0>5}",
                        "geo_name": r.get("NAME", p.get("geo_name", "")),
                        "population_total": r.get(vtot, ""),
                        "population_asian": r.get(vas, ""),
                        "population_latino": r.get(vlat, ""),
                        "source": f"{dataset}_api",
                        "source_id": m.get("source_id", ""),
                        "source_dataset": dataset,
                        "var_total": vtot,
                        "var_asian": vas,
                        "var_latino": vlat,
                    }
                )
    return out


def load_historical(path: str) -> List[Dict[str, str]]:
    rows = read_csv_rows(path)
    if not rows:
        return []

    needed = {"year", "geo_level", "geo_id", "geo_name", "population_asian", "population_latino"}
    if not needed.issubset(set(rows[0].keys())):
        missing = sorted(list(needed - set(rows[0].keys())))
        raise ValueError(f"Historical population file missing columns: {missing}")

    out = []
    for r in rows:
        out.append(
            {
                "year": r.get("year", ""),
                "geo_level": r.get("geo_level", ""),
                "geo_id": r.get("geo_id", ""),
                "geo_name": r.get("geo_name", ""),
                "population_total": r.get("population_total", ""),
                "population_asian": r.get("population_asian", ""),
                "population_latino": r.get("population_latino", ""),
                "source": r.get("source", "manual_historical"),
                "source_id": r.get("source_id", "manual_historical"),
                "source_dataset": r.get("source_dataset", ""),
                "var_total": r.get("var_total", ""),
                "var_asian": r.get("var_asian", ""),
                "var_latino": r.get("var_latino", ""),
            }
        )
    return out


def dedupe_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    rows_sorted = sorted(rows, key=lambda r: (r.get("geo_level", ""), r.get("geo_id", ""), to_int(r.get("year", "")) or -1, r.get("source", "")))
    seen = {}
    for r in rows_sorted:
        key = (r.get("year", ""), r.get("geo_level", ""), r.get("geo_id", ""))
        seen[key] = r
    return [seen[k] for k in sorted(seen.keys(), key=lambda x: (x[1], x[2], to_int(x[0]) or -1))]


def main():
    args = parse_args()

    rows: List[Dict[str, str]] = []
    rows.extend(load_historical(args.historical_input))

    if not args.skip_api:
        varmap = load_variable_map(args.variable_map)
        varmap = expand_map_rows(varmap)
        rows.extend(fetch_national_from_map(varmap, args.api_key))
        rows.extend(fetch_selected_places_from_map(args.places_input, varmap, args.api_key))

    fields = [
        "year",
        "geo_level",
        "geo_id",
        "geo_name",
        "population_total",
        "population_asian",
        "population_latino",
        "source",
        "source_id",
        "source_dataset",
        "var_total",
        "var_asian",
        "var_latino",
    ]

    if not rows:
        write_csv_rows(args.output, [], fields)
        print("No population data retrieved; wrote empty output schema.")
        return

    out = dedupe_rows(rows)
    write_csv_rows(args.output, out, fields)
    print(f"Wrote {len(out):,} rows to {args.output}")


if __name__ == "__main__":
    main()
