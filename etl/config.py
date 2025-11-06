import os
from dotenv import load_dotenv

load_dotenv()

#DB settings (edit via .env)
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", 5432))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
PGDATABASE = os.getenv("PGDATABASE", "postgres")
PGSCHEMA = os.getenv("PGSCHEMA", "healthtech")


# Project paths (assumes repo layout)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOGS_DIR, "etl.log")

#DB settings
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    os.path.join(BASE_DIR, "healthtech.duckdb") 
)

# Your specific Excel files
DOCTORS_XLSX = os.path.join(RAW_DIR, "Data Enginner's Doctors Excel - VIP Medical Group.xlsx")
APPTS_XLSX = os.path.join(RAW_DIR, "Data Engineer's Appointments Excel - VIP Medical Group.xlsx")

# Sheet names
DOCTORS_SHEET = "doctors"
APPTS_SHEET = "appointments"
