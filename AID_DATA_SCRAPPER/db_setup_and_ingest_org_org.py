# db_setup_and_ingest.py

import os
import glob
import json
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from datetime import datetime
from scrappers.foreign_assistance_scraper import run_foreign_assistance_scraper
from scrappers.who_ghed_scraper import run_who_ghed_scraper 
import logging
import shutil
from datetime import datetime
import os
import re
from datetime import datetime
import shutil
from sqlalchemy import text
from pathlib import Path

def sanitize_filename(text):
    return re.sub(r'[^A-Za-z0-9_\-]+', '_', text)
# Set up logging (place at top of file if not already defined)
# Configure logging
class NoTracebackFormatter(logging.Formatter):
    # strip any traceback delivered via exc_info
    def formatException(self, exc_info):
        return ""                      # completely omit traceback
    def format(self, record):
        record.exc_info = None         # safety-belt
        return super().format(record)

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
log_file = "world_bank_scraper.log"

logger_wb = logging.getLogger("world_bank_scraper")
logger_wb.setLevel(logging.INFO)
logger_wb.propagate = False              # don’t pass records to root logger

# clean stream handler ------------------------------------------------------
stream_h = logging.StreamHandler()
stream_h.setFormatter(NoTracebackFormatter(LOG_FMT))
logger_wb.addHandler(stream_h)

# clean file handler --------------------------------------------------------
if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(log_file)
           for h in logger_wb.handlers):
    file_h = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_h.setFormatter(NoTracebackFormatter(LOG_FMT))
    logger_wb.addHandler(file_h)

log_file = "ingestion.log"

logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)
logger.propagate = False              # don’t pass records to root logger

# clean stream handler ------------------------------------------------------
stream_h = logging.StreamHandler()
stream_h.setFormatter(NoTracebackFormatter(LOG_FMT))
logger.addHandler(stream_h)

# clean file handler --------------------------------------------------------
if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(log_file)
           for h in logger.handlers):
    file_h = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_h.setFormatter(NoTracebackFormatter(LOG_FMT))
    logger.addHandler(file_h)
# --- Config ---
from config import DB_URL, TABLE_NAME

def safe_date(df, col_name):
    if col_name not in df.columns:
        logger.info(f"⚠️ Column '{col_name}' not found in DataFrame.")
        return None
    return pd.to_datetime(df[col_name], errors="coerce").dt.date

CREATE_TABLE_SQL = f"""
CREATE TABLE {TABLE_NAME} (
    total_beneficiaries TEXT,
    impact_per_dollar NUMERIC,
    end_date DATE,
    year_active INTEGER,
    similarity_score NUMERIC,
    number_reached BIGINT,
    women_reached_pct TEXT,
    youth_reached_pct NUMERIC,
    persons_with_disabilities BIGINT,
    latitude NUMERIC,
    correlation_value NUMERIC,
    total_commitment_usd NUMERIC,
    geo_needs_alignment_score NUMERIC,
    percent_of_total NUMERIC,
    outcomes_reported BIGINT,
    outputs_reported BIGINT,
    total_disbursed_usd NUMERIC,
    disbursed_amount_usd NUMERIC,
    annual_spend NUMERIC,
    funding_amount_usd NUMERIC,
    longitude NUMERIC,
    start_date DATE,
    leverage_ratio NUMERIC,
    cost_per_beneficiary NUMERIC,
    impact_score NUMERIC,
    percent_outcomes_achieved NUMERIC,
    last_updated TIMESTAMP WITHOUT TIME ZONE,
    avg_impact_score NUMERIC,
    avg_outcomes_achieved NUMERIC,
    impact_per_usd NUMERIC,
    beneficiary_count BIGINT,
    cross_cutting_themes TEXT,
    thematic_focus TEXT,
    sdg_goals TEXT,
    top_sectors TEXT,
    beneficiary_type TEXT,
    beneficiary_subgroup TEXT,
    beneficiary_disaggregation TEXT,
    marginalised_groups_tag TEXT,
    target_population TEXT,
    outcome_indicators TEXT,
    outcome_indicator_name TEXT,
    outcome_indicator_value TEXT,
    outcome_values TEXT,
    output_indicators TEXT,
    indicator_values TEXT,
    need_indicator_name TEXT,
    need_indicator_value TEXT,
    benchmark_comparison TEXT,
    unintended_outcomes TEXT,
    barriers_to_impact TEXT,
    spillover_effects TEXT,
    cluster_id CHARACTER VARYING,
    cluster_name TEXT,
    cluster_basis TEXT,
    cluster_tags TEXT,
    member_project_ids TEXT,
    correlation_group TEXT,
    correlation_type TEXT,
    map_overlay_type TEXT,
    scenario_forecast TEXT,
    logframe_link TEXT,
    evaluation_docs TEXT,
    document_links TEXT,
    narrative_summary TEXT,
    summary_text TEXT,
    stakeholder_feedback TEXT,
    quote_tags TEXT,
    source CHARACTER VARYING,
    source_of_data TEXT,
    data_source TEXT,
    data_standard TEXT,
    scrape_run_id CHARACTER VARYING,
    project_description TEXT,
    top_donors TEXT,
    total_projects TEXT,
    project_id CHARACTER VARYING,
    donor TEXT,
    project_title TEXT,
    donor_name TEXT,
    donor_id CHARACTER VARYING,
    implementer_name TEXT,
    implementing_partner TEXT,
    collaboration_type TEXT,
    cofunded_partner_name TEXT,
    programme_id CHARACTER VARYING,
    project_ids TEXT,
    country_code CHARACTER VARYING,
    country TEXT,
    region TEXT,
    subnational_area TEXT,
    admin_level_1 TEXT,
    admin_level_2 TEXT,
    geography TEXT,
    spatial_resolution TEXT,
    map_layer_group TEXT,
    project_phase TEXT,
    funding_modality TEXT,
    funding_type TEXT,
    project_status TEXT,
    status TEXT,
    success_category TEXT,
    performance_classification TEXT,
    data_quality_score TEXT,
    fragility_context TEXT,
    climate_component TEXT,
    sector TEXT,
    subsector TEXT
);
"""

from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import ProgrammingError
# ---------------------------------------------------------------------
# 1️⃣  Add just after the other imports
# ---------------------------------------------------------------------
from sqlalchemy.exc import ProgrammingError

# ---------------------------------------------------------------------
# 2️⃣  Helper that runs an idempotent migration
# ---------------------------------------------------------------------
def _ensure_text_columns(engine):
    """
    Convert total_beneficiaries + women_reached_pct to TEXT
    if they are not already. Safe to run repeatedly.
    """
    with engine.begin() as conn:
        # Check current data-types
        sql_check = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = :tbl
          AND column_name IN ('total_beneficiaries', 'women_reached_pct');
        """
        result = conn.execute(text(sql_check), {"tbl": TABLE_NAME.lower()})
        types  = {row.column_name: row.data_type for row in result}

        needs_change = [
            col for col, dtype in types.items()
            if dtype.lower() != "text"
        ]
        if not needs_change:
            logger.info("ℹ️ Columns already TEXT – no migration needed.")
            return

        # Build ALTER TABLE only for the columns that still need it
        alters = ", ".join(
            f"ALTER COLUMN {col} TYPE TEXT USING {col}::TEXT"
            for col in needs_change
        )
        alter_sql = f"ALTER TABLE {TABLE_NAME} {alters};"

        try:
            conn.execute(text(alter_sql))
            logger.info(f"✅ Migrated {', '.join(needs_change)} → TEXT.")
        except ProgrammingError as e:
            logger.error(f"❌ Column-type migration failed: {e}")
            raise

# ---------------------------------------------------------------------
# 3️⃣  Call the helper from *inside* init_database(), just after
#     the table-creation block succeeds.
# ---------------------------------------------------------------------
def init_database():
    engine = create_engine(DB_URL, echo=False)

    if not database_exists(engine.url):
        logger.info("📦 Creating database...")
        create_database(engine.url)
    else:
        logger.info("✅ Database already exists.")

    try:
        with engine.begin() as conn:
            conn.execute(text(CREATE_TABLE_SQL))
            logger.info("✅ Ensured project_data table exists.")
    except ProgrammingError as e:
        if "already exists" in str(e.orig).lower():
            logger.info("ℹ️ project_data table already exists.")
        else:
            logger.error(f"❌ Table creation failed: {e}")
            raise

    # ⬇️  🔄 run the migration here
    _ensure_text_columns(engine)

    return engine


# --- Utility functions ---
def safe_first(val):
    if isinstance(val, list):
        return val[0] if val else None
    return val

def parse_date(val):
    val = safe_first(val)
    if isinstance(val, str) and val:
        try:
            return datetime.fromisoformat(val.replace("Z", "")).date()
        except:
            return None
    return None

# --- Map CSV to Unified Schema ---
def map_csv_to_standard(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.strip().lower() for col in df.columns]

    # Required raw columns from IATI CSV
    required = [
        "/iati-identifier", "/title/narrative", "/reporting-org/narrative",
        "/recipient-country@code", "/activity-status@code",
        "/budget/value", "/transaction/value"
    ]

    print("🔍 Checking for required columns in CSV...")
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        logger.info(f"❌ Missing required columns: {missing_cols}")
        return pd.DataFrame()  # Return empty DataFrame to skip ingestion

    mapped = pd.DataFrame()

    # Basic fields
    mapped["project_id"] = df.get("/iati-identifier")
    mapped["project_title"] = df.get("/title/narrative")
    mapped["donor_name"] = df.get("/reporting-org/narrative")
    mapped["donor_id"] = df.get("/reporting-org@ref")
    mapped["implementer_name"] = df.get("/participating-org/narrative")
    mapped["implementing_partner"] = df.get("/participating-org@ref")
    mapped["country_code"] = df.get("/recipient-country@code")
    mapped["country"] = df.get("/recipient-country/narrative")
    mapped["region"] = df.get("/recipient-region/narrative")
    mapped["subnational_area"] = df.get("/location/administrative@code")
    if "/location/point/pos" in df.columns:
        try:
            location_series = df["/location/point/pos"].fillna("").astype(str)
            mapped["latitude"] = location_series.str.split().str[0]
            mapped["longitude"] = location_series.str.split().str[1]
        except Exception as e:
            logger.info(f"⚠️ Failed to extract lat/lon: {e}")
            mapped["latitude"] = None
            mapped["longitude"] = None
    else:
        mapped["latitude"] = None
        mapped["longitude"] = None
    mapped["start_date"] = df[df["/activity-date@type"] == 1].get("/activity-date@iso-date")
    mapped["end_date"] = df[df["/activity-date@type"] == 3].get("/activity-date@iso-date")
    mapped["project_status"] = df.get("/activity-status@code")
    mapped["status"] = df.get("/activity-status@code")
    mapped["total_commitment_usd"] = pd.to_numeric(df.get("/budget/value"), errors="coerce")
    mapped["total_disbursed_usd"] = pd.to_numeric(
        df[df["/transaction/transaction-type@code"] == 3].get("/transaction/value"), errors="coerce"
    )
    mapped["funding_amount_usd"] = pd.to_numeric(
        df[df["/transaction/transaction-type@code"] == 11].get("/transaction/value"), errors="coerce"
    )
    mapped["funding_modality"] = df.get("/default-aid-type@code")
    mapped["sector"] = df.get("/sector/narrative")
    mapped["project_description"] = df.get("/description/narrative")
    mapped["document_links"] = df.get("/document-link@url")
    mapped["evaluation_docs"] = df.get("/result/document-link@url")
    mapped["source"] = "IATI"
    # Drop empty rows and ensure key identifiers are present
    mapped.dropna(how="all", inplace=True)
    mapped.dropna(subset=["project_id", "project_title", "country_code"], how="all", inplace=True)

    return mapped.reset_index(drop=True)

# --- Map JSON to Unified Schema ---
def map_json_to_standard(data: dict) -> pd.DataFrame:
    if "response" in data and "docs" in data["response"]:
        docs = data["response"]["docs"]
        if not docs:
            logger.info("⚠️ No documents found in response.")
            return pd.DataFrame()

        doc = docs[0]  # assuming first doc is sufficient

        def get_first(value):
            return value[0] if isinstance(value, list) and value else value

        def extract_participating_orgs_by_role(docs, role=4):
            return ", ".join(
                org.get("narrative") for org in docs.get("participating_org", [])
                if str(org.get("role")) == str(role) and org.get("narrative")
            )

        def extract_related_ids(docs):
            return ", ".join(docs.get("related_activity_ref", []))

        def extract_document_urls(docs):
            return ", ".join(docs.get("document_link_url", []))

        def extract_logframe_link(docs):
            titles = docs.get("document_link_title_narrative", [])
            urls = docs.get("document_link_url", [])
            for title, url in zip(titles, urls):
                if "logical framework" in title.lower():
                    return url
            return None

        def extract_evaluation_docs(docs):
            titles = docs.get("document_link_title_narrative", [])
            urls = docs.get("document_link_url", [])
            return ", ".join(
                url for title, url in zip(titles, urls) if "review" in title.lower()
            )

        row = {
            "project_id": doc.get("iati_identifier"),
            "project_title": get_first(doc.get("title_narrative_first") or doc.get("title")),
            "donor_name": get_first(doc.get("reporting_org_narrative")),
            "donor_id": doc.get("reporting-org.ref"),
            "implementer_name": extract_participating_orgs_by_role(doc, role=4),
            "country_code": get_first(doc.get("recipient-country.code")),
            "country": get_first(doc.get("recipient-country.name")),
            "start_date": parse_date(doc.get("activity-date.start-actual") or doc.get("activity_date_iso_date")),
            "end_date": parse_date(doc.get("activity-date.end-planned") or doc.get("activity_date_iso_date")),
            "project_status": get_first(doc.get("activity-status.code")),
            "status": get_first(doc.get("activity-status.code")),
            "project_description": get_first(doc.get("description_narrative")),
            "sector": ", ".join(doc.get("sector_narrative", [])),
            "subsector": ", ".join([
                f"{code} - {name}" for code, name in zip(doc.get("sector_code", []), doc.get("sector_narrative", []))
            ]),
            "total_commitment_usd": float(get_first(doc.get("activity_plus_child_aggregation_commitment_value_usd")) or 0),
            "total_disbursed_usd": float(get_first(doc.get("activity_plus_child_aggregation_disbursement_value_usd")) or 0),
            "funding_amount_usd": float(get_first(doc.get("activity_plus_child_aggregation_budget_value_usd")) or 0),
            "document_links": extract_document_urls(doc),
            "logframe_link": extract_logframe_link(doc),
            "evaluation_docs": extract_evaluation_docs(doc),
            "project_ids": extract_related_ids(doc),
            "collaboration_type": ", ".join([str(org.get("role")) for org in doc.get("participating_org", []) if org.get("role")]),
            "thematic_focus": ", ".join(doc.get("policy_marker_narrative", [])),
            "climate_component": ", ".join([
                tag for tag in doc.get("policy_marker_narrative", []) if "climate" in tag.lower()
            ]),
            "cross_cutting_themes": ", ".join(doc.get("policy_marker_narrative", [])),
            "programme_id": (doc.get("iati_identifier", "").split("-")[:4]),
            "summary_text": get_first(doc.get("description_narrative")),
            "top_sectors": get_first(doc.get("sector_narrative")),
            "top_donors": "UK - FCDO",
            "total_projects": len(doc.get("related_activity_ref", [])),
            "cluster_tags": ", ".join(doc.get("sector_narrative", []) + doc.get("policy_marker_narrative", [])),
            "source": "FCDO"
        }

        return pd.DataFrame([row])

    return pd.DataFrame()

# --- Ingest Data ---
def ingest_data(df: pd.DataFrame):
    if df.empty:
        logger.warning("⚠️ Skipping ingestion: DataFrame is empty or invalid.")
        print("⚠️ Skipping ingestion: DataFrame is empty or invalid.")
        return

    print(f"📥 Ingesting {len(df)} rows into {TABLE_NAME}")
    logger.info(f"📥 Ingesting {len(df)} rows into {TABLE_NAME}")

    engine = init_database()
    try:
        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        logger.info(f"🚀 Ingested {len(df)} rows into {TABLE_NAME}.")
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}")
        raise 

# --- Parse IATI CSV Files ---

def parse_iati_csvs(folder="iati_downloads"):
    archive_folder = os.path.join(folder, "archive")
    os.makedirs(archive_folder, exist_ok=True)

    csv_paths = glob.glob(os.path.join(folder, "*.csv"))
    for path in csv_paths:
        filename = os.path.basename(path)
        archive_path = os.path.join(archive_folder, filename)

        logger.info(f"📄 Parsing CSV: {filename}")
        if os.path.exists(archive_path):
            logger.info(f"📁 File already in archive, skipping ingestion.")
            os.remove(path)
            logger.info(f"🗑️ Deleted duplicate: {path}")
            continue

        try:
            df = pd.read_csv(path)
            # logger.info("📌 CSV Columns:", df.columns.tolist())

            mapped = map_csv_to_standard(df)
            ingest_data(mapped)
            logger.info(f"✅ Ingested: {filename}")

            shutil.move(path, archive_path)
            logger.info(f"📦 Moved to archive: {archive_path}")

        except Exception as e:
            logger.info(f"❌ Error processing {filename}: {e}")

# --- Parse FCDO JSON Links ---
# --- Parse FCDO JSON Links from Saved File ---
def parse_fcdo_jsons(file_path: str):
    if not os.path.exists(file_path):
        logger.error(f"JSON links file not found: {file_path}")
        return

    with open(file_path) as f:
        urls = [line.strip() for line in f if line.strip()]

    for url in urls:
        try:
            logger.info(f"Fetching JSON: {url}")
            res = requests.get(url)
            data = res.json()
            mapped = map_json_to_standard(data)

            if mapped.empty or mapped.isnull().all(axis=1).iloc[0]:
                logger.warning(f"Skipping JSON with all-null fields: {url}")
                continue

            ingest_data(mapped)

        except Exception as e:
            logger.warning(f"Failed to process {url}: {e}")

    # Move processed file to archive subfolder
    try:
        archive_dir = os.path.join(os.path.dirname(file_path), "archive")
        os.makedirs(archive_dir, exist_ok=True)
        archived_path = os.path.join(archive_dir, os.path.basename(file_path))
        os.rename(file_path, archived_path)
        logger.info(f"Moved ingested file to archive: {archived_path}")
    except Exception as e:
        logger.error(f"Could not move file to archive: {e}")

    # --- Move the processed file to archive folder ---
    # archive_dir = "fcdo_archive"
    # os.makedirs(archive_dir, exist_ok=True)
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # archive_path = os.path.join(archive_dir, f"project_json_links_{timestamp}.txt")

    # try:
    #     os.rename(file, archive_path)
    #     logging.info(f"📦 Archived JSON link file to: {archive_path}")
    # except Exception as e:
    #     logging.error(f"❌ Failed to archive file: {e}")

from scrappers import world_bank_scrapper  # import from scrapper1.py

def parse_worldbank_data(country_code="NG", indicator_code="SP.POP.TOTL", date_range="2015:2020"):
    logger_wb.info(f"🌍 Fetching World Bank data: {country_code}, {indicator_code}, {date_range}")
    df = world_bank_scrapper.fetch_indicator_data(country_code, indicator_code, date_range)

    if df.empty:
        logger_wb.info("⚠️ No World Bank data returned.")
        return
    logger_wb.info("📋 DataFrame Columns:", df.columns.tolist())
    mapped = pd.DataFrame()
    mapped["project_id"] = df.get("id")
    mapped["project_title"] = df.get("project_name")
    mapped["donor_name"] = "World Bank"
    mapped["donor_id"] = "WB"
    mapped["donor"] = "World Bank"
    mapped["country_code"] = df.get("countrycode")
    mapped["country"] = df.get("countryname") or df.get("countryshortname")
    mapped["region"] = df.get("regionname")
    mapped["start_date"] = safe_date(df, "boardapprovaldate")
    mapped["end_date"] = safe_date(df, "closingdate")
    mapped["last_updated"] = safe_date(df, "p2a_updated_date")
    mapped["project_status"] = df.get("projectstatusdisplay") or df.get("status")
    mapped["status"] = df.get("status")
    mapped["last_updated"] = pd.to_datetime(df.get("p2a_updated_date"), errors="coerce").dt.date
    mapped["project_description"] = None  # Not explicitly available
    mapped["implementer_name"] = df.get("impagency")
    mapped["implementing_partner"] = df.get("impagency")
    mapped["funding_modality"] = df.get("lendinginstr")
    mapped["funding_type"] = df.get("prodlinetext")
    mapped["funding_amount_usd"] = pd.to_numeric(df.get("totalamt"), errors="coerce")
    mapped["total_commitment_usd"] = pd.to_numeric(df.get("totalcommamt"), errors="coerce")
    mapped["disbursed_amount_usd"] = None  # Not provided in static dump
    mapped["subsector"] = df.get("sector1.Name")
    mapped["sector"] = df.get("mjtheme_namecode").apply(lambda x: x[0]['name'] if isinstance(x, list) and x else None)
    mapped["document_links"] = df.get("url")
    mapped["geography"] = df.get("countryname") or df.get("countryshortname")
    mapped["year_active"] = df.get("approvalfy")
    mapped["source"] = "World Bank"

    ingest_data(mapped)

def parse_worldbank_projects(country, indicator_code, date_range):
    # Handle ISO-to-name mapping
    

    
    
    url = f"https://search.worldbank.org/api/v2/projects?format=json&countrycode={country}"
    res = requests.get(url)
    data = res.json()

    projects = data.get("projects", {})
    if not projects:
        logger_wb.info("⚠️ No projects found.")
        return

    df = pd.DataFrame.from_dict(projects, orient="index")

    mapped = pd.DataFrame()
    mapped["project_id"] = df.get("id")
    mapped["project_title"] = df.get("project_name")
    mapped["donor_name"] = "World Bank"
    mapped["donor_id"] = "WB"
    mapped["donor"] = "World Bank"
    mapped["country"] = df["countryname"].apply(lambda x: x[0] if isinstance(x, list) else x)
    mapped["country_code"] = df["countrycode"].apply(lambda x: x[0] if isinstance(x, list) else x)
    mapped["geography"] = df["countryname"].apply(lambda x: x[0] if isinstance(x, list) else x)
    mapped["region"] = df.get("regionname")
    mapped["start_date"] = pd.to_datetime(df.get("boardapprovaldate"), errors="coerce").dt.date
    mapped["end_date"] = pd.to_datetime(df.get("closingdate"), errors="coerce").dt.date
    mapped["project_status"] = df.get("status")
    mapped["status"] = df.get("status")
    mapped["funding_type"] = df.get("prodlinetext")
    mapped["funding_modality"] = df.get("lendinginstr")
    mapped["funding_amount_usd"] = pd.to_numeric(df.get("totalamt"), errors="coerce")
    mapped["total_commitment_usd"] = pd.to_numeric(df.get("totalcommamt"), errors="coerce")
    mapped["implementer_name"] = df.get("impagency")
    mapped["implementing_partner"] = df.get("impagency")
    mapped["sector"] = df["mjtheme_namecode"].apply(lambda x: x[0]["name"] if isinstance(x, list) and x else None)
    mapped["subsector"] = df["sector1"].apply(lambda x: x.get("Name") if isinstance(x, dict) else None)
    mapped["document_links"] = df.get("url")
    mapped["project_description"] = df.get("project_abstract")
    mapped["year_active"] = df.get("approvalfy")
    mapped["last_updated"] = pd.to_datetime(df.get("p2a_updated_date"), errors="coerce").dt.date
    mapped["source"] = "World Bank"
    # File and folder setup
    base_dir = "world_bank_downloads"
    archive_dir = os.path.join(base_dir, "archive")
    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    base_country = sanitize_filename(country)
    base_indicator = sanitize_filename(indicator_code or "none")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{base_country}_{base_indicator}_{timestamp}.csv"
    filepath = os.path.join(base_dir, filename)

    # Check for archive version (without timestamp)
    archive_prefix   = f"{base_country}_{base_indicator}_"
    already_archived = any(f.startswith(archive_prefix) for f in os.listdir(archive_dir))

    if already_archived:
        logger.info(f"⏩ Skipping ingestion: archive for {country} / {indicator_code} already exists.")
        return

    # Save and ingest
    mapped.to_csv(filepath, index=False)
    logger_wb.info(f"✅ Saved World Bank data to {filepath}")

    try:
        ingest_data(mapped)
        logger.info("📥 Ingested data successfully.")

        # Move to archive
        archived_path = os.path.join(archive_dir, filename)
        shutil.move(filepath, archived_path)
        logger_wb.info(f"📦 Moved file to archive: {archived_path}")
    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}")

def parse_foreign_assistance_data(filters):
    # Modified scraper should return path to saved file (or None)
    csv_file_path = run_foreign_assistance_scraper(filters)
    # csv_file_path = r'E:\Portfolio\code_ver7\foreign_assistance_downloads\Country Name-Afghanistan_US Sector Name-Health_20250630_151449.csv'
    try:
        df = pd.read_csv(csv_file_path)
        if df.empty:
            logger.info("⚠️ No data extracted from CSV (empty file).")
            return
    except Exception as e:
        logger.info(f"⚠️ Failed to load CSV: {e}")
        return

    mapped = pd.DataFrame()
    mapped["project_id"] = df.get("Activity ID")
    mapped["project_title"] = df.get("Activity Name")
    mapped["project_description"] = df.get("Activity Description")
    mapped["donor_name"] = df.get("Funding Agency Name")
    mapped["donor_id"] = df.get("Funding Agency ID")
    mapped["implementer_name"] = df.get("Implementing Partner Name")
    mapped["country"] = df.get("Country Name")
    mapped["country_code"] = df.get("Country Code")
    mapped["region"] = df.get("Region Name")
    mapped["funding_modality"] = df.get("Aid Type Group Name")
    mapped["sector"] = df.get("US Sector Name")
    mapped["subsector"] = df.get("International Sector Name")
    mapped["start_date"] = pd.to_datetime(df.get("Activity Start Date"), errors="coerce").dt.date
    mapped["end_date"] = pd.to_datetime(df.get("Activity End Date"), errors="coerce").dt.date
    mapped["total_commitment_usd"] = pd.to_numeric(df.get("Current Dollar Amount"), errors="coerce")
    mapped["funding_amount_usd"] = pd.to_numeric(df.get("activity_budget_amount"), errors="coerce")
    mapped["year_active"] = df.get("Fiscal Year")
    mapped["status"] = df.get("Transaction Type Name")
    mapped["source"] = "Foreign Assistance"

    mapped.dropna(subset=["project_id", "project_title", "country_code"], how="all", inplace=True)

    # Ingest the mapped DataFrame
    ingest_data(mapped)

    # Move the file to archive
    archive_dir = os.path.join("foreign_assistance_downloads", "archive")
    os.makedirs(archive_dir, exist_ok=True)

    archive_path = os.path.join(archive_dir, os.path.basename(csv_file_path))
    shutil.move(csv_file_path, archive_path)
    logger.info(f"📦 Moved file to archive: {archive_path}")


import shutil

def parse_who_ghed_data(country: str, start_year: int, end_year: int, download_dir: str = "ghed_downloads"):
    file_path = run_who_ghed_scraper(country, start_year, end_year, download_dir)
    if not file_path or not os.path.exists(file_path):
        logger.info("❌ No file downloaded.")
        return

    try:
        # Check archive for duplicate (ignore timestamp)
        archive_dir = os.path.join(download_dir, "archive")
        os.makedirs(archive_dir, exist_ok=True)

        downloaded_filename = os.path.basename(file_path)
        base_name = "_".join(downloaded_filename.split("_")[:-1])  # Remove timestamp

        # Check for any matching base in archive
        archived_files = os.listdir(archive_dir)
        match_found = any(f.startswith(base_name) for f in archived_files)

        if match_found:
            logger.info(f"⚠️ File with base name '{base_name}' already exists in archive. Skipping ingestion.")
            os.remove(file_path)
            logger.info(f"🗑️ Deleted duplicate: {file_path}")
            return

        # --- Continue to ingest since no match was found ---
        df = pd.read_excel(file_path, sheet_name="Data", engine="openpyxl", dtype=str)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

        mapping = {
            "code": "country_code",
            "region": "region",
            "location": "subnational_area",
            "year": "year_active",
            "che": "total_commitment_usd",
            "gghed": "total_disbursed_usd",
            "che_pc_usd": "cost_per_beneficiary",
            "che_gdp": "impact_per_usd",
            "gghed_che": "leverage_ratio",
            "pvtd": "beneficiary_count",
            "gghed_gdp": "impact_score",
            "ext": "funding_amount_usd",
            "gghed_pc_usd": "avg_impact_score",
            "pvtd_pc_usd": "percent_outcomes_achieved",
            "oop_pc_usd": "impact_per_dollar",
            "ext_pc_usd": "avg_outcomes_achieved",
            "source": "WHO"
        }

        df.rename(columns=mapping, inplace=True)
        df.columns = df.columns.str.strip()
        df = df[[col for col in mapping.values() if col in df.columns]]

        numeric_cols = [
            "total_commitment_usd", "total_disbursed_usd", "cost_per_beneficiary",
            "impact_per_usd", "leverage_ratio", "beneficiary_count", "impact_score",
            "funding_amount_usd", "avg_impact_score", "percent_outcomes_achieved",
            "impact_per_dollar", "avg_outcomes_achieved"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "beneficiary_count" in df.columns:
            df["beneficiary_count"] = df["beneficiary_count"].round().dropna().astype("Int64")

        df.dropna(subset=["country_code", "year_active"], how="any", inplace=True)
        df["source"] = "GHED"
        ingest_data(df)

    except Exception as e:
        logger.info(f"❌ Error processing GHED Excel: {e}")

    finally:
        archive_path = os.path.join(download_dir, "archive", os.path.basename(file_path))
        shutil.move(file_path, archive_path)
        logger.info(f"📦 Archived: {archive_path}")


def parse_un_sdg_data(indicator: str, area_code: int, start_year: int, end_year: int):
    import requests
    import pandas as pd

    base_url = "https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg"
    params = {
        "indicator": [indicator],
        "areaCode": [area_code],
        "timePeriodStart": start_year,
        "timePeriodEnd": end_year
    }

    try:
        logger.info(f"🌐 Requesting UN SDG data for indicator {indicator}, area {area_code}, years {start_year}-{end_year}")
        response = requests.get(f"{base_url}/Indicator/Data", params=params)
        response.raise_for_status()
        data = response.json()

        if "data" not in data or not data["data"]:
            logger.info("❌ No data found in response.")
            return

        records = data["data"]
        df = pd.json_normalize(records)

        logger.info(f"✅ Retrieved {len(df)} rows")

        # Column mapping for ingestion
        mapping = {
            "geoAreaCode": "country_code",
            "geoAreaName": "subnational_area",
            "timePeriodStart": "year_active",
            "value": "impact_score",
            "valueComments": "notes"
        }

        df.rename(columns=mapping, inplace=True)
        df.columns = df.columns.str.strip()

        df = df[[col for col in mapping.values() if col in df.columns]]
        df.dropna(subset=["country_code", "year_active", "impact_score"], inplace=True)
        df["country_code"] = df["country_code"].astype(str)

        # Save backup (optional)
        csv_path = f"un_sdg_{indicator}_{area_code}.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"💾 Saved results to {csv_path}")

        ingest_data(df)

    except Exception as e:
        logger.info(f"❌ Error retrieving or processing UN SDG data: {e}")

def parse_oecd_csvs(folder="oecd_downloads"):
    archive_folder = os.path.join(folder, "archive")
    os.makedirs(archive_folder, exist_ok=True)
    logger.info(f"📄 Starting OECD CSV parsing in: {folder}")

    COLUMN_MAPPING = {
        "STRUCTURE": "programme_id",
        "STRUCTURE_ID": "project_id",
        "STRUCTURE_NAME": "project_title",
        "ACTION": "project_status",
        "REF_AREA": "country_code",
        "Reference area": "country",
        "FREQ": "year_active",
        "TIME_PERIOD": "year_active",
        "MEASURE": "outcome_indicator_name",
        "OBS_VALUE": "outcome_indicator_value",
        "OBS_STATUS": "data_quality_score",
        "DECIMALS": "indicator_values",
        "RISK": "need_indicator_name",
        "AGE": "beneficiary_disaggregation",
        "SEX": "beneficiary_disaggregation",
        "CONVERSION_TYPE": "data_standard",
        "PRICE_BASE": "benchmark_comparison"
    }

    csv_paths = glob.glob(os.path.join(folder, "*.csv"))
    for path in csv_paths:
        filename = os.path.basename(path)
        base_name = re.sub(r' \(\d+\)', '', filename)
        archive_path = os.path.join(archive_folder, base_name)

        logger.info(f"📄 Parsing OECD CSV: {filename}")
        if os.path.exists(archive_path):
            logger.info(f"⏩ Duplicate found in archive, skipping: {filename}")
            os.remove(path)
            continue

        try:
            df = pd.read_csv(path)
            logger.info(f"🔍 Loaded {len(df)} rows from {filename}")

            mapped_data = pd.DataFrame()
            for src_col, tgt_col in COLUMN_MAPPING.items():
                if src_col in df.columns:
                    if tgt_col in mapped_data.columns:
                        mapped_data[tgt_col] = (
                            mapped_data[tgt_col].fillna('') + " " + df[src_col].fillna('').astype(str)
                        ).str.strip()
                    else:
                        mapped_data[tgt_col] = df[src_col]

            if "project_id" not in mapped_data.columns:
                mapped_data["project_id"] = range(1, len(mapped_data) + 1)
            if "year_active" in mapped_data.columns:
                mapped_data["year_active"] = mapped_data["year_active"].astype(str).str.extract(r'(\d{4})').astype(float).astype("Int64")
            mapped_data["source"] = "OECD"
            logger.info(f"✅ Mapped {len(mapped_data)} rows for ingestion from {filename}")
            logger.info(f"🧩 Columns in mapped data: {mapped_data.columns.tolist()}")

            ingest_data(mapped_data)

            try:
                shutil.move(path, archive_path)
                logger.info(f"📦 Archived OECD file to: {archive_path}")
            except Exception as archive_error:
                logger.error(f"❌ Failed to archive file {filename}: {archive_error}")

        except Exception as e:
            logger.error(f"❌ Error processing {filename}: {e}")


try:
    import pycountry
    _iso = lambda c: pycountry.countries.lookup(c).alpha_3 if pd.notna(c) else None
except ImportError:      # fallback: just return the original value
    _iso = lambda c: c


def map_bii_direct_to_target(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a raw BII-export DataFrame into the target project schema.
    Unmapped columns are left as NaN/None for downstream processing.
    """

    tgt = pd.DataFrame()

    # ── straightforward 1-to-1 mappings ───────────────────────────────
    tgt["project_id"]          = raw_df["Project number"]
    tgt["project_title"]       = raw_df["Investment name"]
    tgt["donor_name"]          = "British International Investment"
    tgt["implementer_name"]    = raw_df["Company"]
    tgt["region"]              = raw_df["Region"]
    tgt["sector"]              = raw_df["Sector"]
    tgt["subnational_area"]    = raw_df["Who Geography 1"]

    # ISO-3 country code conversion (fallback = original value)
    tgt["country_code"]        = raw_df["Country"]
    tgt["geography"]           = raw_df["Country"]                 # optional plain-name field

    # ── dates ────────────────────────────────────────────────────────
    tgt["start_date"]          = pd.to_datetime(raw_df["Start Date 1"], errors="coerce").dt.date
    tgt["end_date"]            = pd.to_datetime(raw_df["End date 1"],  errors="coerce").dt.date
    tgt["year_active"]         = pd.to_datetime(raw_df["Start Date 1"], errors="coerce").dt.year

    # ── finance fields ───────────────────────────────────────────────
    tgt["total_commitment_usd"] = (
        raw_df[["USD Amount 1", "USD Amount 2"]].fillna(0).sum(axis=1)
    )
    tgt["funding_amount_usd"]   = tgt["total_commitment_usd"]
    tgt["funding_modality"]     = raw_df["Investment type 1"]

    # ── impact / indicator fields ────────────────────────────────────
    tgt["impact_score"]         = raw_df["Impact Score"]
    tgt["outcome_values"]       = raw_df["Impact Score"]
    tgt["outcome_indicator_name"]  = raw_df["What"].where(raw_df["What"].notna(), raw_df["How Primary"])
    tgt["outcome_indicator_value"] = raw_df["Impact Score"]

    # combine SDGs + climate flags into a single cross-cutting string
    tgt["cross_cutting_themes"] = (
        raw_df["Sustainable Development Goals"].fillna("") + "|" +
        raw_df["Climate Finance status"].fillna("")
    ).str.strip("|")

    # gender & beneficiary counts
    tgt["women_reached_pct"]    = raw_df["2X Gender Finance Percentage"]
    tgt["total_beneficiaries"]  = raw_df["Scale"]

    # environmental risk → fragility context
    tgt["fragility_context"]    = raw_df["Environmental and social risk"]

    # notes / narratives
    tgt["narrative_summary"]    = raw_df["Expected impact"]
    tgt["evaluation_docs"]      = raw_df["Environmental and social summary"]

    # status
    if "Status" in raw_df.columns:
        tgt["project_status"]   = raw_df["Status"]
    tgt["source"]      = "BII"
    # any columns still missing in TARGET_COLS remain NaN/None
    return tgt

# ---------------------------------------------------------------------
# Mapping for Funds CSV
# ---------------------------------------------------------------------
def map_bii_fund_to_target(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw BII Funds export to the target schema."""
    tgt = pd.DataFrame()

    # 1-to-1 / direct mappings
    tgt["project_id"]        = raw_df["Project number"]            # BII deal ID
    tgt["project_title"]     = raw_df["Investment name"]           # Deal title
    tgt["donor_name"]        = "British International Investment"  # constant
    tgt["implementer_name"]  = raw_df["Fund name"]                 # Fund vehicle
    tgt["region"]            = raw_df["Region"]
    tgt["sector"]            = raw_df["Sector"]

    tgt["country_code"]      = raw_df["Country"]
    tgt["subnational_area"]  = raw_df["Who Geography 1"]

    # dates
    tgt["start_date"]        = pd.to_datetime(raw_df["Start date"], errors="coerce").dt.date
    tgt["end_date"]          = pd.to_datetime(raw_df["End date"],   errors="coerce").dt.date
    tgt["year_active"]       = pd.to_datetime(raw_df["Start date"], errors="coerce").dt.year

    # finance
    tgt["total_commitment_usd"] = raw_df["USD Amount"].fillna(0)
    tgt["funding_amount_usd"]   = tgt["total_commitment_usd"]
    tgt["funding_modality"]     = raw_df["Investment type"]

    # impact / indicators
    tgt["impact_score"]         = raw_df["Impact Score"]
    tgt["outcome_values"]       = raw_df["Impact Score"]
    tgt["outcome_indicator_name"]  = raw_df["What"].where(raw_df["What"].notna(), raw_df["How Primary"])
    tgt["outcome_indicator_value"] = raw_df["Impact Score"]

    # SDGs & climate
    tgt["cross_cutting_themes"] = (
        raw_df["Sustainable Development Goals"].fillna("") + "|" +
        raw_df["Climate Finance status"].fillna("")
    ).str.strip("|")

    # gender, beneficiaries, risk
    tgt["women_reached_pct"]    = raw_df["2X Gender Finance Percentage"]
    tgt["total_beneficiaries"]  = raw_df["Scale"]
    tgt["fragility_context"]    = raw_df["Environmental and social risk"]

    # narrative fields
    tgt["narrative_summary"]    = raw_df["Expected impact"]
    tgt["evaluation_docs"]      = raw_df["Environmental and social summary"]
    tgt["source"]      = "BII"
    # status if present
    if "Status" in raw_df.columns:
        tgt["project_status"]   = raw_df["Status"]

    return tgt

def map_bii_underlying_to_target(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Convert simplified 'Underlying' CSV to unified target schema."""
    tgt = pd.DataFrame()

    tgt["project_id"]         = raw_df["Fund ID"]
    tgt["project_title"]      = raw_df["Fund name"]
    tgt["implementer_name"]   = raw_df["Company name"]

    tgt["region"]             = raw_df["Region"]
    tgt["country_code"]       = raw_df["Country"]  # convert to ISO3
    tgt["sector"]             = raw_df["Sector"]
    tgt["project_status"]     = raw_df["Status"]
    tgt["climate_component"]  = raw_df["Fossil fuel or renewable exposure"]

    # Dates
    tgt["start_date"]         = pd.to_datetime(raw_df["Start Date"], errors="coerce").dt.date
    tgt["end_date"]           = pd.to_datetime(raw_df["End date"], errors="coerce").dt.date
    tgt["year_active"]        = pd.to_datetime(raw_df["Start Date"], errors="coerce").dt.year
    tgt["source"]      = "BII"
    return tgt


def parse_bii_csvs(folder="bii_downloads"):
    """
    Loop over every CSV in `bii_downloads/`, detect type (Direct, Fund, Underlying),
    map to target schema, ingest, then move to `bii_downloads/archive/`.
    If a file with the same name is already archived, delete the new one
    and skip ingestion.
    If a file in the archive starts with 'export', delete it.
    """
    folder = Path(folder)
    archive_folder = folder / "archive"
    archive_folder.mkdir(exist_ok=True)

    # ── delete archived 'export*.csv' files ─────────────────────────────
    for archived_file in archive_folder.glob("export*.csv"):
        logger.info(f"🗑️ Deleting archived export file: {archived_file.name}")
        archived_file.unlink(missing_ok=True)

    for csv_path in folder.glob("*.csv"):
        archive_target = archive_folder / csv_path.name

        # ── duplicate check ───────────────────────────────────────────
        parts = csv_path.stem.split("_")

        if len(parts) >= 5:
            # Remove last two chunks (date and time)
            base_name = "_".join(parts[:-2])
        else:
            # Fallback for unexpected filename format
            base_name = csv_path.stem

        archive_matches = list(archive_folder.glob(f"{base_name}_*.csv"))

        if archive_matches:
            logger.warning(f"⚠️ Duplicate file (by base name) detected, deleting un-ingested copy: {csv_path.name}")
            csv_path.unlink(missing_ok=True)
            continue

        # ── read & detect structure ──────────────────────────────────
        try:
            raw = pd.read_csv(csv_path)
            cols = set(raw.columns)
            
            if {"Project number", "Company", "USD Amount 1"}.issubset(cols):
                mapped = map_bii_direct_to_target(raw)
                label = "Direct"

            elif {"Fund name", "USD Amount", "Investment type"}.issubset(cols):
                mapped = map_bii_fund_to_target(raw)
                label = "Fund"

            elif {"Fund ID", "Company name", "Fossil fuel or renewable exposure"}.issubset(cols):
                mapped = map_bii_underlying_to_target(raw)
                label = "Underlying"

            else:
                logger.warning(f"⚠️ Unrecognised BII CSV layout: {csv_path.name}")
                csv_path.rename(archive_target)
                continue

            # ── ingest into DB / target store ────────────────────────
            ingest_data(mapped)
            logger.info(f"✅ Ingested {label} CSV → {csv_path.name}")

            # ── archive the file ─────────────────────────────────────
            csv_path.rename(archive_target)
            logger.info(f"📦 Archived → {archive_target.name}")

        except Exception as e:
            logger.error(f"❌ Error processing {csv_path.name}: {e}")
            csv_path.rename(archive_target.with_suffix(".error.csv"))
