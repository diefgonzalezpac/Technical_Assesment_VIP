import pandas as pd
from datetime import datetime
from .logging_utils import get_logger

logger = get_logger("etl.transform")

def _parse_booking_date(s: pd.Series) -> pd.Series:
    """
    booking_date looks like '10/20/2025' (MM/DD/YYYY). We'll parse flexibly.
    If time isn't present, set 00:00:00.
    """
    # try strict MM/DD/YYYY first; fall back to to_datetime for flexibility
    parsed = pd.to_datetime(s, format="%m/%d/%Y", errors="coerce")
    # if NaT, try general parser
    fallback_mask = parsed.isna()
    if fallback_mask.any():
        parsed.loc[fallback_mask] = pd.to_datetime(s.loc[fallback_mask], errors="coerce")
    return parsed

def _normalize_status(s: pd.Series) -> pd.Series:
    """
    Normalize various spellings to: confirmed | cancelled | pending
    Defaults unknowns to 'pending'.
    """
    mapping = {
        "confirmed": "confirmed",
        "confirm": "confirmed",
        "cnf": "confirmed",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "cnl": "cancelled",
        "pending": "pending",
        "pnd": "pending",
        "scheduled": "pending",
    }
    s = s.astype(str).str.strip().str.lower()
    return s.map(mapping).fillna(
        s.where(s.isin(["confirmed", "cancelled", "pending"]), other="pending")
    )

def clean_doctors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns: doctor_id, name, specialty
    Output columns: doctor_id, doctor_name, specialty
    """
    logger.info("Cleaning doctors...")
    df = df.copy()

    # Standardize column names
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={"name": "doctor_name"})

    # Types & formatting
    df["doctor_id"] = pd.to_numeric(df["doctor_id"], errors="coerce").astype("Int64")
    df["doctor_name"] = df["doctor_name"].astype(str).str.strip().str.title()
    df["specialty"] = df["specialty"].astype(str).str.strip().str.title()

    before = len(df)
    df = df.dropna(subset=["doctor_id", "doctor_name"])
    # de-dup on doctor_id (keep last)
    df = df.sort_values(by="doctor_id").drop_duplicates(subset=["doctor_id"], keep="last")
    logger.info(f"Doctors cleaned: {before} -> {len(df)} rows")

    # Final column order
    return df[["doctor_id", "doctor_name", "specialty"]]

def clean_appointments(df: pd.DataFrame, valid_doctors: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns: booking_id, patient_id, doctor_id, booking_date, status
    Output columns: appointment_id, patient_id, doctor_id, appointment_datetime, status
    """
    logger.info("Cleaning appointments...")
    df = df.copy()

    # Standardize column names
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={
        "booking_id": "appointment_id",
        "booking_date": "appointment_datetime",
    })

    # Types
    df["appointment_id"] = pd.to_numeric(df["appointment_id"], errors="coerce").astype("Int64")
    df["patient_id"] = pd.to_numeric(df["patient_id"], errors="coerce").astype("Int64")
    df["doctor_id"] = pd.to_numeric(df["doctor_id"], errors="coerce").astype("Int64")

    # Dates & status
    df["appointment_datetime"] = _parse_booking_date(df["appointment_datetime"])
    df["status"] = _normalize_status(df["status"])

    before = len(df)
    df = df.dropna(subset=["appointment_id", "patient_id", "doctor_id", "appointment_datetime"])

    # Remove exact dupes
    df = df.drop_duplicates()

    # FK filter: keep only appointments whose doctor exists
    valid_ids = set(valid_doctors["doctor_id"].dropna().astype(int).tolist())
    df = df[df["doctor_id"].astype(int).isin(valid_ids)]

    logger.info(f"Appointments cleaned: {before} -> {len(df)} rows")

    # Final column order
    return df[["appointment_id", "patient_id", "doctor_id", "appointment_datetime", "status"]]
