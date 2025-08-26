# API 3 — Age-Group Beneficiary Summary

from fastapi import Query, Path
from typing import List
import re

DEFAULT_BINS = "0-5,6-17,18-35,36-60,61-120"
BIN_RE = re.compile(r"^\d+-\d+(,\d+-\d+)*$")

@app.get("/api/v1/beneficiaries/summary/age-group", tags=["Beneficiary APIs"])
def age_group_beneficiary_summary(
    country: str = Query(None, description="Filter by country"),
    year: int = Query(None, description="Filter by project year"),
    project_id: str = Query(None, description="Filter by single project"),
    bins: str = Query(DEFAULT_BINS,
                      description="Comma-separated age brackets (low-high)"),
    db: Session = Depends(get_db)
):
    # Basic validation of bin string
    if not BIN_RE.match(bins):
        raise HTTPException(400, detail="bins must be like '0-5,6-17,18-35'")

    sql = text("""
        WITH params AS (
            SELECT :bins::text AS bins_text
        ),
        bucket AS (
            SELECT unnest(string_to_array(bins_text, ',')) AS bin_spec FROM params
        ),
        ranges AS (
            SELECT
              split_part(bin_spec, '-', 1)::int AS low,
              split_part(bin_spec, '-', 2)::int AS high
            FROM bucket
        ),
        binned AS (
            SELECT
                concat(r.low::text, '-', r.high::text)           AS age_group,
                SUM(b.count)                                     AS beneficiary_count
            FROM       ranges          r
            CROSS JOIN beneficiaries   b
            JOIN       projects        p ON b.project_id = p.project_id
            WHERE b.age BETWEEN r.low AND r.high
              AND (:country IS NULL    OR p.country     = :country)
              AND (:year    IS NULL    OR p.year_active = :year)
              AND (:project_id IS NULL OR p.project_id  = :project_id)
            GROUP BY r.low, r.high
        )
        SELECT age_group, beneficiary_count
        FROM   binned
        ORDER  BY split_part(age_group, '-', 1)::int;
    """)

    result = db.execute(sql, {
        "bins": bins,
        "country": country,
        "year": year,
        "project_id": project_id
    }).fetchall()

    return [dict(row) for row in result]
