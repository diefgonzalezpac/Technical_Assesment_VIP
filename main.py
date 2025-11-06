import os
import argparse
from etl.extract import read_sources
from etl.transform import clean_doctors, clean_appointments
from etl.logging_utils import get_logger
from etl.config import PROCESSED_DIR

logger = get_logger("etl.main")

def parse_args():
    p = argparse.ArgumentParser(description="HealthTech ETL")
    p.add_argument("--duckdb", action="store_true",
                   help="Use DuckDB instead of PostgreSQL.")
    return p.parse_args()

def main():
    args = parse_args()

    # Choose loader based on flag (DuckDB or Postgres by default)
    if args.duckdb:
        from etl.load_duckdb import load
        logger.info("Backend selected: DuckDB")
    else:
        from etl.load import load
        logger.info("Backend selected: PostgreSQL")

    logger.info("Starting ETL pipeline...")
    doctors_raw, appts_raw = read_sources()
    doctors = clean_doctors(doctors_raw)
    appts = clean_appointments(appts_raw, valid_doctors=doctors)

    # Persist the “final dataset” outputs
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    doctors_out = os.path.join(PROCESSED_DIR, "doctors_clean.csv")
    appts_out = os.path.join(PROCESSED_DIR, "appointments_clean.csv")
    doctors.to_csv(doctors_out, index=False)
    appts.to_csv(appts_out, index=False)
    logger.info(f"Wrote processed datasets: {doctors_out}, {appts_out}")

    # Load to the selected backend
    load(doctors, appts)

    logger.info("ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()
