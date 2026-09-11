#!/usr/bin/env python3
"""Explore and query the extracted OpenData.org entity resolution dataset with DuckDB."""

import sys
from pathlib import Path
import duckdb

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "opendata_sample"


def main():
    org_path = DATA_DIR / "organization.parquet"
    loc_path = DATA_DIR / "locations.parquet"
    peo_path = DATA_DIR / "peoplebusiness.parquet"

    if not org_path.exists() or not loc_path.exists() or not peo_path.exists():
        print(f"Sample parquet files not found in {DATA_DIR}")
        sys.exit(1)

    con = duckdb.connect()

    print("=" * 70)
    print(" OPENDATA.ORG (SENZING FORMAT) EVALUATION DATASET")
    print("=" * 70)

    # Counts
    org_count = con.execute(f"SELECT count(*) FROM '{org_path}'").fetchone()[0]
    loc_count = con.execute(f"SELECT count(*) FROM '{loc_path}'").fetchone()[0]
    peo_count = con.execute(f"SELECT count(*) FROM '{peo_path}'").fetchone()[0]

    print("Loaded records:")
    print(f"  • Organizations:    {org_count:>10,} rows  ({org_path.stat().st_size / 1024**2:.1f} MB)")
    print(f"  • Locations:        {loc_count:>10,} rows  ({loc_path.stat().st_size / 1024**2:.1f} MB)")
    print(f"  • People/Business:  {peo_count:>10,} rows  ({peo_path.stat().st_size / 1024**2:.1f} MB)")
    print(f"  • Total in sample:  {org_count + loc_count + peo_count:>10,} rows\n")

    print("--- 1. Organizations Overview ---")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE v_org AS
        SELECT 
            RECORD_ID as record_id,
            list_filter(FEATURES, x -> map_contains(x, 'NAME_ORG'))[1]['NAME_ORG'] as name_org,
            list_filter(FEATURES, x -> map_contains(x, 'ADDR_CITY'))[1]['ADDR_CITY'] as city,
            list_filter(FEATURES, x -> map_contains(x, 'ADDR_STATE'))[1]['ADDR_STATE'] as state,
            list_filter(FEATURES, x -> map_contains(x, 'ADDR_COUNTRY'))[1]['ADDR_COUNTRY'] as country,
            list_filter(FEATURES, x -> map_contains(x, 'LEI_NUMBER'))[1]['LEI_NUMBER'] as lei,
            list_filter(FEATURES, x -> map_contains(x, 'PLACEKEY'))[1]['PLACEKEY'] as placekey,
            list_filter(FEATURES, x -> map_contains(x, 'WEBSITE_ADDRESS'))[1]['WEBSITE_ADDRESS'] as website,
            list_filter(FEATURES, x -> map_contains(x, 'LINKEDIN'))[1]['LINKEDIN'] as linkedin
        FROM '{org_path}'
    """)

    org_stats = con.execute("""
        SELECT 
            count(name_org) as with_name,
            count(city) as with_city,
            count(lei) as with_lei,
            count(placekey) as with_placekey,
            count(website) as with_website,
            count(linkedin) as with_linkedin
        FROM v_org
    """).df()
    print("Identifier coverage across organizations:")
    for col in org_stats.columns:
        cnt = org_stats[col].iloc[0]
        pct = (cnt / org_count) * 100
        print(f"  {col:<16}: {cnt:>8,} ({pct:>5.1f}%)")

    print("\n--- 2. Top 5 Organizations with Digital Identifiers ---")
    print(con.execute("""
        SELECT 
            replace(name_org, '"', '') as name,
            replace(city, '"', '') as city,
            replace(state, '"', '') as state,
            replace(website, '"', '') as website
        FROM v_org
        WHERE website IS NOT NULL AND linkedin IS NOT NULL
        LIMIT 5
    """).df().to_string(index=False))

    print("\n--- 3. People / Executive Linkages ---")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE v_people AS
        SELECT 
            RECORD_ID as person_id,
            list_filter(FEATURES, x -> x.NAME_FULL IS NOT NULL)[1].NAME_FULL as name,
            list_filter(FEATURES, x -> x.REL_POINTER_KEY IS NOT NULL)[1].REL_POINTER_KEY as company_id,
            list_filter(FEATURES, x -> x.REL_POINTER_ROLE IS NOT NULL)[1].REL_POINTER_ROLE as role,
            list_filter(FEATURES, x -> x.ADDR_COUNTRY IS NOT NULL)[1].ADDR_COUNTRY as country,
            list_filter(FEATURES, x -> x.LINKEDIN IS NOT NULL)[1].LINKEDIN as linkedin
        FROM '{peo_path}'
    """)

    print("Top Countries in People/Business:")
    print(con.execute("""
        SELECT country, count(*) as count
        FROM v_people
        WHERE country IS NOT NULL
        GROUP BY 1
        ORDER BY count DESC
        LIMIT 8
    """).df().to_string(index=False))

    print("\nSample Executives with LinkedIn profiles:")
    print(con.execute("""
        SELECT name, country, role, linkedin
        FROM v_people
        WHERE linkedin IS NOT NULL AND name IS NOT NULL
        LIMIT 5
    """).df().to_string(index=False))

    print("\n" + "=" * 70)
    print("To run ad-hoc SQL in Python:")
    print("  import duckdb")
    print("  con = duckdb.connect()")
    print("  con.execute(\"SELECT * FROM 'data/opendata_sample/organization.parquet' LIMIT 5\").df()")
    print("=" * 70)


if __name__ == "__main__":
    main()
