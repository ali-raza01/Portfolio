# API 2: Project Title and Description.

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import os

# -----------------------------
# Database setup
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/your_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(
    title="Project API",
    version="1.0.0"
)

# -----------------------------
# API 2: Project Title & Description
# -----------------------------
@app.get("/api/v1/projects/title-description", tags=["Project APIs"])
def project_title_description_list(
    country: str = Query(None, description="Filter by country name"),
    year: int = Query(None, description="Filter by year_active"),
    search: str = Query(None, description="Keyword search in title/description"),
    limit: int = Query(100, ge=1, le=1000, description="Limit number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db)
):
    sql = text("""
        SELECT
            project_id,
            title,
            description,
            country,
            year_active
        FROM projects
        WHERE
            (:country IS NULL OR country = :country)
            AND (:year IS NULL OR year_active = :year)
            AND (
                :search IS NULL OR
                title ILIKE '%' || :search || '%' OR
                description ILIKE '%' || :search || '%'
            )
        ORDER BY year_active DESC, country
        LIMIT :limit OFFSET :offset;
    """)

    result = db.execute(sql, {
        "country": country,
        "year": year,
        "search": search,
        "limit": limit,
        "offset": offset
    }).fetchall()

    return [dict(row) for row in result]
