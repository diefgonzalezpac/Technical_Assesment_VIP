import duckdb
import pandas as pd
from .config import DUCKDB_PATH
from .logging_utils import get_logger

logger = get_logger("etl.load_duckdb")

DDL_DOCTORS = """
CREATE TABLE IF NOT EXISTS healthtech.healthtech.doctors (
    doctor_id INTEGER,
    doctor_name TEXT,
    specialty TEXT
);
"""

DDL_APPOINTMENTS = """
CREATE TABLE IF NOT EXISTS healthtech.healthtech.appointments (
    appointment_id INTEGER,
    patient_id INTEGER,
    doctor_id INTEGER,
    appointment_datetime TIMESTAMP,
    status TEXT
);
"""

def load(doctors_df: pd.DataFrame, appts_df: pd.DataFrame):
    logger.info(f"Connecting DuckDB at: {DUCKDB_PATH}")
    con = duckdb.connect(DUCKDB_PATH)

    try:
        # Ensure schema & tables exist.
        con.execute("CREATE SCHEMA IF NOT EXISTS healthtech;")
        con.execute(DDL_DOCTORS)
        con.execute(DDL_APPOINTMENTS)

        # Idempotent load: replace tables with the fresh cleaned data.
        con.register("doctors_df", doctors_df)
        con.register("appts_df", appts_df)

        con.execute("""
            CREATE OR REPLACE TABLE healthtech.healthtech.doctors AS
            SELECT doctor_id, doctor_name, specialty
            FROM doctors_df;
        """)

        con.execute("""
            CREATE OR REPLACE TABLE healthtech.healthtech.appointments AS
            SELECT appointment_id, patient_id, doctor_id, appointment_datetime, status
            FROM appts_df;
        """)

        logger.info("DuckDB load completed (tables replaced).")
    finally:
        con.close()
