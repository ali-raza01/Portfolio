# API 1 — Year-wise Country Funding Summary

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from db import get_db  # your session factory

app = FastAPI()

ALLOWED_FUND_COLS = {
    "total_commitment_usd",
    "total_disbursed_usd",
    "funding_amount_usd"
}

@app.get("/api/v1/projects/summary/year-country")
async def year_country_summary(
    start_year: int,
    end_year: int,
    fund_col: str = "total_commitment_usd",
    db: Session = Depends(get_db)
):
    if fund_col not in ALLOWED_FUND_COLS:
        raise HTTPException(400, detail=f"fund_col must be one of {ALLOWED_FUND_COLS}")

    sql = text(f"""
        SELECT
            year_active                 AS year,
            country,
            COUNT(*)                    AS project_count,
            SUM(COALESCE({fund_col},0)) AS total_funding_usd
        FROM   projects
        WHERE  year_active BETWEEN :start_year AND :end_year
        GROUP  BY year_active, country
        ORDER  BY year_active, country;
    """)

    rows = db.execute(sql, {"start_year": start_year, "end_year": end_year}).fetchall()
    return [dict(r) for r in rows]
