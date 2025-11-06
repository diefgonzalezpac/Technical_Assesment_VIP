import pandas as pd
from .config import DOCTORS_XLSX, APPTS_XLSX, DOCTORS_SHEET, APPTS_SHEET
from .logging_utils import get_logger

logger = get_logger("etl.extract")

# What we expect IN the source files (before renaming)
REQUIRED_DOCTORS = {"doctor_id", "name", "specialty"}
REQUIRED_APPTS = {"booking_id", "patient_id", "doctor_id", "booking_date", "status"}

def _read_excel(path: str, sheet_name: str) -> pd.DataFrame:
    logger.info(f"Reading Excel: {path} (sheet={sheet_name})")
    df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    # keep original case as-is for validation, then normalize to lowercase for transform step
    return df

def read_sources():
    doctors = _read_excel(DOCTORS_XLSX, DOCTORS_SHEET)
    appts = _read_excel(APPTS_XLSX, APPTS_SHEET)

    # Validate required columns exist
    if not REQUIRED_DOCTORS.issubset(set(doctors.columns)):
        missing = REQUIRED_DOCTORS - set(doctors.columns)
        raise ValueError(f"Doctors sheet missing columns: {missing}")

    if not REQUIRED_APPTS.issubset(set(appts.columns)):
        missing = REQUIRED_APPTS - set(appts.columns)
        raise ValueError(f"Appointments sheet missing columns: {missing}")

    logger.info("Extraction complete.")
    return doctors, appts
