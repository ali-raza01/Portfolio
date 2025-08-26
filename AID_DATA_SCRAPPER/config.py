# config.py

DB_NAME = "aid_projects"
DB_USER = "postgres"
DB_PASSWORD = "Shahzad01-"
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "project_data"

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"