# --- utils/ingest.py --------------------------------
from sqlalchemy import create_engine
import pandas as pd

# Change DB URL if needed
DB_URL = "sqlite:///project_data.db"
engine = create_engine(DB_URL)

def ingest_data(df: pd.DataFrame, table_name: str = "project_data"):
    """
    Append the given DataFrame to the specified database table.
    """
    if df.empty:
        print("⚠️  Skipped ingest — empty DataFrame.")
        return
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"✅  Ingested {len(df)} rows into '{table_name}'")
