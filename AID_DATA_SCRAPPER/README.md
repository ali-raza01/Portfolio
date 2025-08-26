# Aid-Data Unified Scraper & Ingestion Pipeline

A modular Python project that **scrapes, standardises, and ingests international-development project data** from multiple public sources into a single PostgreSQL table.
It exposes a small **FastAPI** service for on-demand scraping / ingestion and for returning the harmonised data in a single JSON payload.&#x20;

---

## Table of Contents

1. [Key Features](#key-features)
2. [Architecture](#architecture)
3. [Directory Layout](#directory-layout)
4. [Quick-start](#quick-start)
5. [API Reference](#api-reference)
6. [Running Individual Scrapers](#running-individual-scrapers)
7. [Unified Mapping Schema](#unified-mapping-schema)
8. [Logging & Error Files](#logging--error-files)

---

## Key Features

* **Seven headless Selenium / API scrapers** for FCDO, IATI, OECD, WHO-GHED, USA Foreign Assistance, UN SDGs and World Bank.
* **FastAPI micro-service** with two endpoints: `/check-payload` (idempotency check) and `/get-data` (trigger scrape ➜ ingest ➜ return data).&#x20;
* **Unified mapping layer** that converts wildly different column names into \~60 canonical fields (`COLUMN_MAPPING`).&#x20;
* **PostgreSQL ingestion** with automatic DB creation (`aid_projects`), duplicate file detection, and archiving of processed downloads.&#x20;
* **Extensive logging** to timestamped `.log` files per scraper + a central `ingestion_error.log`.
* **Pluggable** – add a new source by dropping a scraper in `scrappers/` and extending the mapping dicts.

---

## Architecture

```text
            +--------------+           filters            +------------------+
 Client ───►| FastAPI app  |  /get-data payload  ────────► |  Scraper Runner  |
            |  main.py     |◄─────────── JSON results ──── |  (per source)    |
            +--------------+                               +------------------+
                     │                                             │
                     │                               raw CSV / JSON│
                     ▼                                             ▼
            +----------------------+ ingest()            +------------------+
            | db_setup_and_ingest_ |  ───────────────────►| Postgres (table) |
            | org.py               |   mapped DataFrame  +------------------+
            +----------------------+        ▲
                                            │
                                   unified_mapping.py
```

---

## Directory Layout

```text
.
├── scrappers/                # All Selenium/API scrapers
│   ├── fcdo_scrapper.py
│   ├── foreign_assistance_scraper.py
│   ├── iati_scrapper.py
│   ├── oecd_scrapper.py
│   ├── sdgs_scraper.py
│   ├── who_ghed_scraper.py
│   └── world_bank_scrapper.py
├── db_setup_and_ingest_org.py # DB utils + mapping + ingestion
├── unified_mapping.py         # Column & filter dictionaries
├── main.py                    # FastAPI service
├── test2.py                   # Example request payloads
├── <source>_downloads/        # One folder per source (+/archive)
└── *.log                      # Rolling logs per component
```

---

## Quick Start

### 1 . Prerequisites

* Python 3.9+ (venv strongly recommended)
* Google Chrome + chromedriver (webdriver-manager auto-installs)
* PostgreSQL ≥ 13 (user/password with superuser rights)

### 2 . Clone & install

```bash
git clone https://github.com/your-org/aid-data-pipeline.git
cd aid-data-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 3 . Configure database

Edit **`db_setup_and_ingest_org.py`** or export ENV variables:

```bash
export DB_NAME=aid_projects
export DB_USER=postgres
export DB_PASSWORD=•••••
export DB_HOST=localhost
export DB_PORT=5432
```

First run will auto-create the DB & table.&#x20;

### 4 . Launch API

```bash
uvicorn main:app --reload
```

Local docs now at `http://127.0.0.1:8000/docs` (Swagger).

---

## API Reference

| Method | Endpoint         | Body fields                                          | Description                                                                             |
| ------ | ---------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `POST` | `/check-payload` | `{sources: [..], filters: {...}}`                    | Returns `already_executed` flag so you can avoid duplicate scrapes.                     |
| `POST` | `/get-data`      | Same payload + `ingest_only`, `proceed_if_duplicate` | Triggers scraping and returns the unified rows (or just ingests if `ingest_only=true`). |

### Payload schema

* **sources** – list of strings: `"fcdo"`, `"iati"`, `"oecd"`, `"worldbank"`, `"ghed"`, `"foreignassistance"`, `"sdg"`.
* **filters** – general keys (`country`, `sector`, `start_year`, …) automatically remapped per source via `FILTER_KEY_MAPPING`.&#x20;
* **ingest\_only** – skip returning data (good for cron jobs).
* **proceed\_if\_duplicate** – bypass the SHA-like idempotency check.

See `test2.py` for many ready-made examples.&#x20;

---

## Running Individual Scrapers

```python
from scrappers.fcdo_scrapper import run_fcdo_scraper
run_fcdo_scraper({
    "Sectors": "Health",
    "Activity Status": "Closed",
    "Benefiting Regions": "NG"
})
```

Each scraper function:

1. Opens the vendor site (headless Chrome by default).
2. Applies filters (dropdowns or SQL-like query).
3. Downloads raw CSV/JSON into `<source>_downloads/`.
4. Moves de-duplicated files to `<source>_downloads/archive/`.
5. Returns the local file path (or ingests directly).

---

## Unified Mapping Schema

All fields used across sources map to a common superset so downstream analysis sees **one tidy table**. Edit `unified_mapping.py` to add or adjust mappings. Typical fields include:

* `project_id`, `project_title`, `donor_name`, `country_code`, `sector`,
* `total_commitment_usd`, `total_disbursed_usd`, `year_active`, `source`, …

Mappings exist for both **columns** (`COLUMN_MAPPING`) and **filter keys** (`FILTER_KEY_MAPPING`).&#x20;

---

## Logging & Error Files

| Component                  | File                      |
| -------------------------- | ------------------------- |
| Global ingestion errors    | `ingestion_error.log`     |
| World Bank fetch           | `world_bank_error.log`    |
| FCDO scraper               | `fcdo_scraper_errors.log` |
| Foreign Assistance scraper | `foreign_scraper.log`     |
| IATI scraper               | `iati_scraper_errors.log` |
| OECD scraper               | `oecd_scraper_errors.log` |
| WHO GHED scraper           | `who_ghed_error.log`      |

Logs rotate at 500 KB (via **loguru** for the API).&#x20;


