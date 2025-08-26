# API 4 — Funding Breakdown by Funding Group

@app.get("/api/v1/funding/summary/by-group", tags=["Funding APIs"])
def funding_group_breakdown(
    country: str = Query(None, description="Filter by country"),
    year: int = Query(None, description="Filter by year"),
    fund_col: str = Query("total_commitment_usd", description="Funding field to sum"),
    limit: int = Query(20, ge=1, le=1000, description="Top N funding groups to return"),
    db: Session = Depends(get_db)
):
    allowed_cols = {"total_commitment_usd", "total_disbursed_usd", "funding_amount_usd"}

    if fund_col not in allowed_cols:
        raise HTTPException(400, detail=f"`fund_col` must be one of {allowed_cols}")

    sql = text(f"""
        SELECT
            funding_org,
            SUM(COALESCE({fund_col}, 0)) AS total_funding_usd
        FROM
            projects
        WHERE
            (:country IS NULL OR country = :country)
            AND (:year IS NULL OR year_active = :year)
        GROUP BY
            funding_org
        ORDER BY
            total_funding_usd DESC
        LIMIT :limit;
    """)

    result = db.execute(sql, {
        "country": country,
        "year": year,
        "limit": limit
    }).fetchall()

    return [dict(row) for row in result]
