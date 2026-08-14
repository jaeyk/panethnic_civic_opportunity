# Method note: dating panethnic legal organizations

Supports the manuscript paragraph on Asian American and Latino legal
organizations (professional associations vs. legal services & advocacy).
Documents the identification procedure and the founding-year correction that
underlies the reported medians. Reproduces/extends `src/identify_panethnic_orgs.R`
and `outputs/analysis/panethnic_orgs_summary.csv`.

The Figure 23 organizational-type composition analysis (professional,
hobby/sports, and arts & culture proportions across eras) is documented in
`README.md` under "org-type composition flow figure", not here.

## 1. Data and scope

Source: IRS tax-exempt organization records and organization website text,
as compiled by Kim, de Vries, and Han (2025, *Scientific Data*). Universe:
organizations already flagged as representing Asian American or Latino
panethnic constituencies (`detection_method %in% c("RE", "both", "ML",
"ground_truth")`, `panethnic_group %in% c("asian", "latino", "both")` in
`processed_data/org_enriched/org_civic_enriched.csv`).

## 2. Identifying legal organizations and subtypes

An organization is classified as legal if its IRS name matches
`legal|\blaw\b|lawyer|attorney|bar assoc`, excluding `counsel(ing)` (mental
health counseling false positives) and `law enforcement|peace officer`
(not legal-professional orgs). Matches are then split into two subtypes by
a first-match keyword rule (full logic in `src/identify_panethnic_orgs.R`):

- **Professional associations** — bar associations, lawyers/lawyer
  associations, law student/school associations, foundations. n = 101
  (60 Asian American, 41 Latino).
- **Legal services & advocacy** — legal defense funds, legal resource
  centers, law caucuses/alliances. n = 10 (5 Asian American, 5 Latino).

## 3. Founding-year measure

`fnd_yr` (IRS record of the current legal entity's registration year) is
used as a proxy for founding year. This is a reasonable proxy for
organizations incorporated once and never renamed, but it understates true
age for organizations that reorganized or re-incorporated under a new legal
name — the current entity's IRS registration date postdates when the
organization actually began operating.

**Correction rule:** apply a true-founding-year override only where
independent documentary evidence (institutional history pages, archival
finding aids) shows the current IRS entity is a re-incorporation of an
earlier organization under a different name — not merely a short
administrative lag between starting operations and filing for tax-exempt
status, which is normal and not treated as bias. Applying the override
indiscriminately (e.g., to any 1–2 year filing lag) would not be a
defensible correction; it is reserved for documented identity discontinuities.

Checking all five Asian American legal services & advocacy organizations
against their own institutional histories:

| Organization | EIN | `fnd_yr` | True founding year | Basis |
| --- | --- | ---: | ---: | --- |
| Asian Americans Advancing Justice–Asian Law Caucus | 942176139 | 1998 | **1972** | Founded 1972 in Oakland as the Asian Law Caucus; re-incorporated under the "Advancing Justice" name decades later. [Org history](https://www.asianlawcaucus.org/about/our-history) |
| Asian American Legal Defense and Education Fund | 132855641 | 1975 | 1974 | Founded 1974; `fnd_yr` reflects a 1-year filing lag, not a renaming — no override applied. [AALDEF history](https://www.aaldef.org/about/history/) |
| Santa Clara County Asian Law Alliance | 942439581 | 1978 | 1977–1978 | Began taking cases Jan. 1977; registered under its current name Jan. 1978 — `fnd_yr` already reflects the current entity's origin, not a renaming — no override applied. [Org history](https://asianlawalliance.org/mission-history) |
| Asian American Law Fund of New York | 133779413 | 1994 | 1993 | Established 1993 by the Asian American Bar Association of New York; 1-year filing lag — no override applied. [AALFNY about page](https://www.asianamericanlawfund.org/about-aalfny/) |
| Asian Pacific American Legal Resource Center | 522148028 | 1999 | 1998 | Formed as an all-volunteer org in 1998; 1-year filing lag — no override applied. [Org history](https://www.apalrc.org/home/about-us/) |

Only the Asian Law Caucus entry meets the reorganization criterion, so it is
the sole override applied: `fnd_yr` 1998 → 1972.

The five Latino legal services & advocacy organizations (MALDEF, Centro
Legal de la Raza, La Raza Centro Legal, Latina and Latino Critical Legal
Theory, Auxilio Legal Latino) were not found to have comparable
reorganization histories in the sources checked, so their `fnd_yr` values
are used as reported.

## 4. Results

Professional-association medians are unaffected by this correction (raw
`fnd_yr`, no reorganization cases identified in that subtype).

| Subtype | Group | n | Median | IQR |
| --- | --- | ---: | ---: | --- |
| Professional associations | Asian American | 60 | 2011 | 2003–2015 |
| Professional associations | Latino | 41 | 2008 | 1996–2015 |
| Legal services & advocacy | Asian American (corrected) | 5 | **1978** | 1975–1994 |
| Legal services & advocacy | Latino | 5 | 1975 | 1972–2018 |

(Asian American legal services & advocacy, raw `fnd_yr` before correction:
median 1994, IQR 1978–1998 — see `outputs/analysis/panethnic_orgs_summary.csv`.)

## 5. Reportable text

> But this bar association is far from representative. Using tax returns
> filed by tax-exempt organizations and information from their websites
> (Kim, de Vries, and Han 2025), I identified legal organizations
> representing Asian American and Latino panethnic groups and classified
> them into two broad categories. The first consists of professional
> associations, including bar associations, lawyers associations, and law
> student associations. The second consists of legal service and advocacy
> organizations, including legal defense funds and legal resource centers.
> The median incorporation year, used here as a proxy for founding, is 2011
> for Asian American legal professional organizations and 2008 for Latino
> legal professional organizations. For both Asian American and Latino
> legal service and advocacy organizations, the median incorporation years
> are respectively 1978 and 1975.

## 6. Caveats

- The reorganization check (§3) was done for the Asian American legal
  services & advocacy subtype only, prompted by the known Asian Law Caucus
  case. Professional associations and the Latino legal services & advocacy
  group were not individually audited against institutional histories;
  their reported medians use raw `fnd_yr` and may be subject to the same
  understatement if any of those organizations also reorganized under a
  new name.
- n = 5 per legal services & advocacy group is small; each org's founding
  year materially moves the median.
