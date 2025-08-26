from fastapi import FastAPI, Query, Depends, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import os
import re

"""Merged FastAPI application implementing four endpoints:
1. year_country_funding_summary   – /api/v1/projects/summary/year-country
2. project_title_description_list – /api/v1/projects/title-description
3. age_group_beneficiary_summary  – /api/v1/beneficiaries/summary/age-group
4. funding_group_breakdown        – /api/v1/funding/summary/by-group

Assumed tables
---------------
projects(
    project_id PK,
    country,
    year_active INT,
    title,
    description,
    funding_org,
    total_commitment_usd NUMERIC,
    total_disbursed_usd  NUMERIC,
    funding_amount_usd   NUMERIC
)

beneficiaries(
    beneficiary_id PK,
    project_id FK -> projects(project_id),
    age INT,
    count INT
)
"""

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
DB_NAME = "aid_projects"
DB_USER = "postgres"
DB_PASSWORD = "Shahzad01-"
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "project_data"

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL = os.getenv("DATABASE_URL", DB_URL)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------------------------------------------------------------------
# Constants & validation helpers
# ---------------------------------------------------------------------------
ALLOWED_FUND_COLS = {
    "total_commitment_usd",
    "total_disbursed_usd",
    "funding_amount_usd",
}

DEFAULT_BINS = "0-5,6-17,18-35,36-60,61-120"
BIN_RE = re.compile(r"^\d+-\d+(,\d+-\d+)*$")

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Project Funding & Beneficiary API",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# API 1 – Year‑Country Funding Summary
# ---------------------------------------------------------------------------
@app.get("/api/v1/projects/summary/year-country", tags=["Project APIs"], name="year_country_funding_summary")
def year_country_funding_summary(
    start_year: int = Query(..., description="Inclusive start year"),
    end_year: int = Query(..., description="Inclusive end year"),
    fund_col: str = Query("total_commitment_usd", description="Funding column to sum"),
    db: Session = Depends(get_db),
):
    if start_year > end_year:
        raise HTTPException(400, detail="start_year must be <= end_year")
    if fund_col not in ALLOWED_FUND_COLS:
        raise HTTPException(400, detail=f"fund_col must be one of {ALLOWED_FUND_COLS}")

    sql = text(f"""
        SELECT
            year_active                 AS year,
            country,
            COUNT(*)                    AS project_count,
            SUM(COALESCE({fund_col}, 0)) AS total_funding_usd
        FROM   project_data
        WHERE  year_active BETWEEN :start_year AND :end_year
        GROUP  BY year_active, country
        ORDER  BY year_active, country;
    """)

    rows = db.execute(sql, {"start_year": start_year, "end_year": end_year}).fetchall()
    return [dict(row._mapping) for row in rows]

# ---------------------------------------------------------------------------
# API 2 – Project Title & Description List
# ---------------------------------------------------------------------------
@app.get("/api/v1/projects/title-description", tags=["Project APIs"], name="project_title_description_list")
def project_title_description_list(
    country: str = Query(None, description="Filter by country"),
    year: int = Query(None, description="Filter by year_active"),
    search: str = Query(None, description="Keyword search in title or description"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db),
):
    sql = text("""
        SELECT
            project_id,
            project_title,
            project_description,
            country,
            year_active
        FROM project_data
        WHERE
            (:country IS NULL OR country = :country)
            AND (:year IS NULL OR year_active = :year)
            AND (
                :search IS NULL OR
                project_title ILIKE '%' || :search || '%' OR
                project_description ILIKE '%' || :search || '%'
            )
        ORDER BY year_active DESC, country
        LIMIT :limit OFFSET :offset;
    """)

    rows = db.execute(
        sql,
        {
            "country": country,
            "year": year,
            "search": search,
            "limit": limit,
            "offset": offset,
        },
    ).fetchall()
    return [dict(row._mapping) for row in rows]

# ---------------------------------------------------------------------------
# API 3 – Age‑Group Beneficiary Summary
# ---------------------------------------------------------------------------
# @app.get("/api/v1/beneficiaries/summary/age-group", tags=["Beneficiary APIs"], name="age_group_beneficiary_summary")
# def age_group_beneficiary_summary(
#     country: str = Query(None, description="Filter by country"),
#     year: int = Query(None, description="Filter by project year"),
#     project_id: str = Query(None, description="Filter by single project"),
#     bins: str = Query(DEFAULT_BINS, description="Comma‑separated age ranges e.g. '0-5,6-17'"),
#     db: Session = Depends(get_db),
# ):
#     if not BIN_RE.match(bins):
#         raise HTTPException(400, detail="bins must follow pattern low-high,low-high… e.g. '0-5,6-17'")

#     sql = text("""
#         WITH params AS (
#             SELECT :bins AS bins_text
#         ),
#         bucket AS (
#             SELECT unnest(string_to_array(bins_text, ',')) AS bin_spec FROM params
#         ),
#         ranges AS (
#             SELECT
#                 split_part(bin_spec, '-', 1)::int AS low,
#                 split_part(bin_spec, '-', 2)::int AS high
#             FROM bucket
#         ),
#         binned AS (
#             SELECT
#                 concat(r.low::text, '-', r.high::text) AS age_group,
#                 SUM(
#                     CAST(p.total_beneficiaries AS NUMERIC) *
#                     CAST(p.percent_of_total AS NUMERIC) / 100.0
#                 ) AS beneficiary_count
#             FROM       ranges r
#             JOIN       project_data p ON CAST(p.percent_of_total AS NUMERIC) BETWEEN r.low AND r.high
#             WHERE
#                 (:country IS NULL OR p.country = :country)
#                 AND (:year IS NULL OR p.year_active = :year)
#                 AND (:project_id IS NULL OR p.project_id = :project_id)
#             GROUP BY r.low, r.high
#         )
#         SELECT age_group, beneficiary_count
#         FROM   binned
#         ORDER  BY split_part(age_group, '-', 1)::int;
#     """)

#     rows = db.execute(
#         sql,
#         {
#             "bins": bins,
#             "country": country,
#             "year": year,
#             "project_id": project_id,
#         },
#     ).fetchall()
#     return [dict(row._mapping) for row in rows]

# ---------------------------------------------------------------------------
# API 4 – Funding Breakdown by Group
# ---------------------------------------------------------------------------
@app.get("/api/v1/funding/summary/by-group", tags=["Funding APIs"], name="funding_group_breakdown")
def funding_group_breakdown(
    country: str = Query(None, description="Filter by country"),
    year: int = Query(None, description="Filter by year"),
    fund_col: str = Query("total_commitment_usd", description="Funding column to sum"),
    limit: int = Query(20, ge=1, le=1000, description="Top N groups"),
    db: Session = Depends(get_db),
):
    if fund_col not in ALLOWED_FUND_COLS:
        raise HTTPException(400, detail=f"fund_col must be one of {ALLOWED_FUND_COLS}")

    sql = text(f"""
        SELECT
            donor,
            SUM(COALESCE({fund_col}, 0)) AS total_funding_usd
        FROM   project_data
        WHERE
            (:country IS NULL OR country = :country)
            AND (:year IS NULL OR year_active = :year)
        GROUP BY donor
        ORDER BY total_funding_usd DESC
        LIMIT :limit;
    """)

    rows = db.execute(
        sql,
        {
            "country": country,
            "year": year,
            "limit": limit,
        },
    ).fetchall()
    return [dict(row._mapping) for row in rows]

# ---------------------------------------------------------------------------
# Health check endpoint (optional)
# ---------------------------------------------------------------------------
@app.get("/health", tags=["Utility"])
def health():
    """Simple health check so orchestration tools can probe the API."""
    return {"status": "ok"}
