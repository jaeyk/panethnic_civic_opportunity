#!/usr/bin/env python3

import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(description="Visualize population vs organization growth gaps.")
    p.add_argument("--org-enriched", default="outputs/org_enriched/org_civic_enriched.csv")
    p.add_argument("--population", default="outputs/population/population_series.csv")
    p.add_argument("--selected-places", default="misc/selected_places.csv")
    p.add_argument("--out-dir", default="outputs/figures")
    return p.parse_args()


def load_org_series(path: str) -> pd.DataFrame:
    org = pd.read_csv(path)

    year_col = None
    for cand in ["F.year", "fnd_yr"]:
        if cand in org.columns:
            year_col = cand
            break

    if year_col is None:
        raise ValueError("org enriched file must include either 'F.year' or 'fnd_yr' for yearly trend analysis")

    org["founding_year"] = pd.to_numeric(org[year_col], errors="coerce")
    org = org.dropna(subset=["founding_year", "panethnic_group"]).copy()
    org["founding_year"] = org["founding_year"].astype(int)

    series = (
        org.groupby(["panethnic_group", "founding_year"], as_index=False)
        .agg(new_orgs=("ein", "nunique"))
        .rename(columns={"founding_year": "year"})
        .sort_values(["panethnic_group", "year"])
    )
    series["cumulative_orgs"] = series.groupby("panethnic_group")["new_orgs"].cumsum()
    return series


def make_national_plot(org_series: pd.DataFrame, pop: pd.DataFrame, out_dir: str):
    national = pop[pop["geo_level"] == "national"].copy()
    if national.empty:
        return

    national_long = []
    for grp, pop_col in [("asian", "population_asian"), ("latino", "population_latino")]:
        if pop_col not in national.columns:
            continue
        tmp = national[["year", pop_col]].copy()
        tmp["panethnic_group"] = grp
        tmp = tmp.rename(columns={pop_col: "population"})
        national_long.append(tmp)

    if not national_long:
        return

    national_long = pd.concat(national_long, ignore_index=True)
    merged = org_series.merge(national_long, on=["year", "panethnic_group"], how="inner")
    if merged.empty:
        return

    merged["orgs_per_100k"] = merged["cumulative_orgs"] / merged["population"] * 100000

    fig, axes = plt.subplots(1, 2, figsize=(12, 4), dpi=150)
    for grp, color in [("asian", "#1b9e77"), ("latino", "#d95f02")]:
        g = merged[merged["panethnic_group"] == grp]
        if g.empty:
            continue
        axes[0].plot(g["year"], g["population"], label=f"{grp} population", color=color)
        axes[1].plot(g["year"], g["orgs_per_100k"], label=f"{grp} orgs per 100k", color=color)

    axes[0].set_title("Population")
    axes[1].set_title("Organizations per 100,000")
    for ax in axes:
        ax.set_xlabel("Year")
        ax.legend(frameon=False)
        ax.grid(alpha=0.2)

    fig.suptitle("National Population and Civic Organization Representation Gap")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "national_growth_gap.png"))
    plt.close(fig)


def make_place_plot(org_df: pd.DataFrame, pop: pd.DataFrame, selected_places_path: str, out_dir: str):
    if not os.path.exists(selected_places_path):
        return

    selected = pd.read_csv(selected_places_path)
    required = {"city", "state_abbr", "geo_id"}
    if not required.issubset(selected.columns):
        return

    if not {"irs_city", "irs_state", "panethnic_group", "fnd_yr"}.issubset(org_df.columns):
        return

    org = org_df.copy()
    org["year"] = pd.to_numeric(org["fnd_yr"], errors="coerce")
    org = org.dropna(subset=["year", "irs_city", "irs_state", "panethnic_group"])
    org["year"] = org["year"].astype(int)

    org["city_norm"] = org["irs_city"].str.upper().str.strip()
    org["state_norm"] = org["irs_state"].str.upper().str.strip()
    selected["city_norm"] = selected["city"].str.upper().str.strip()
    selected["state_norm"] = selected["state_abbr"].str.upper().str.strip()

    org = org.merge(selected[["city_norm", "state_norm", "geo_id", "city", "state_abbr"]], on=["city_norm", "state_norm"], how="inner")
    if org.empty:
        return

    annual = (
        org.groupby(["geo_id", "city", "state_abbr", "panethnic_group", "year"], as_index=False)
        .agg(new_orgs=("ein", "nunique"))
        .sort_values(["geo_id", "panethnic_group", "year"])
    )
    annual["cumulative_orgs"] = annual.groupby(["geo_id", "panethnic_group"])["new_orgs"].cumsum()

    place_pop = pop[pop["geo_level"] == "place"].copy()
    if place_pop.empty:
        return

    place_long = []
    for grp, pop_col in [("asian", "population_asian"), ("latino", "population_latino")]:
        if pop_col not in place_pop.columns:
            continue
        tmp = place_pop[["geo_id", "year", pop_col]].copy()
        tmp["panethnic_group"] = grp
        tmp = tmp.rename(columns={pop_col: "population"})
        place_long.append(tmp)

    if not place_long:
        return

    place_long = pd.concat(place_long, ignore_index=True)
    merged = annual.merge(place_long, on=["geo_id", "year", "panethnic_group"], how="inner")
    if merged.empty:
        return

    merged["orgs_per_100k"] = merged["cumulative_orgs"] / merged["population"] * 100000

    focus = selected.copy()
    if "region_focus" in focus.columns:
        focus = focus[focus["region_focus"].str.lower().isin(["midwest", "south"])]

    focus_geo = set(focus["geo_id"].astype(str))
    merged = merged[merged["geo_id"].astype(str).isin(focus_geo)]
    if merged.empty:
        return

    for geo_id, gdf in merged.groupby("geo_id"):
        city = gdf["city"].iloc[0]
        state = gdf["state_abbr"].iloc[0]
        fig, ax = plt.subplots(figsize=(8, 4), dpi=150)
        for grp, color in [("asian", "#1b9e77"), ("latino", "#d95f02")]:
            t = gdf[gdf["panethnic_group"] == grp]
            if t.empty:
                continue
            ax.plot(t["year"], t["orgs_per_100k"], label=f"{grp} orgs per 100k", color=color)
        ax.set_title(f"{city}, {state}: Organization Representation Trend")
        ax.set_xlabel("Year")
        ax.grid(alpha=0.2)
        ax.legend(frameon=False)
        fig.tight_layout()
        slug = f"{city}_{state}".lower().replace(" ", "_")
        fig.savefig(os.path.join(out_dir, f"city_gap_{slug}.png"))
        plt.close(fig)


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    org_df = pd.read_csv(args.org_enriched)
    pop = pd.read_csv(args.population)
    pop["year"] = pd.to_numeric(pop["year"], errors="coerce")
    pop = pop.dropna(subset=["year"]).copy()
    pop["year"] = pop["year"].astype(int)

    org_series = load_org_series(args.org_enriched)
    make_national_plot(org_series, pop, args.out_dir)
    make_place_plot(org_df, pop, args.selected_places, args.out_dir)

    print(f"Figures written to {args.out_dir}")


if __name__ == "__main__":
    main()
