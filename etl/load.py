import io
import psycopg2
from psycopg2 import sql
import pandas as pd
from .config import PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, PGSCHEMA
from .logging_utils import get_logger

logger = get_logger("etl.load")

DDL_DOCTORS = """
CREATE TABLE IF NOT EXISTS {schema}.doctors (
    doctor_id INTEGER PRIMARY KEY,
    doctor_name TEXT NOT NULL,
    specialty TEXT
);
"""

DDL_APPOINTMENTS = """
CREATE TABLE IF NOT EXISTS {schema}.appointments (
    appointment_id INTEGER PRIMARY KEY,
    patient_id INTEGER NOT NULL,
    doctor_id INTEGER NOT NULL REFERENCES {schema}.doctors(doctor_id),
    appointment_datetime TIMESTAMP NOT NULL,
    status TEXT CHECK (status IN ('confirmed','cancelled','pending'))
);
"""

def _connect():
    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
    )
    conn.autocommit = False
    return conn

def _copy_from_df(conn, df: pd.DataFrame, full_table_name: str):
    buf = io.StringIO()
    cols = list(df.columns)
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV)").format(
                sql.SQL(full_table_name),
                sql.SQL(", ").join(map(sql.Identifier, cols))
            ).as_string(cur),
            file=buf
        )

def load(doctors_df: pd.DataFrame, appts_df: pd.DataFrame):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(PGSCHEMA)))
            cur.execute(sql.SQL(DDL_DOCTORS).format(schema=sql.Identifier(PGSCHEMA)))
            cur.execute(sql.SQL(DDL_APPOINTMENTS).format(schema=sql.Identifier(PGSCHEMA)))
            # idempotent: clear and restart identities each run
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}.appointments, {}.doctors RESTART IDENTITY")
                  .format(sql.Identifier(PGSCHEMA), sql.Identifier(PGSCHEMA))
            )
            logger.info("Schema and tables ready; truncated for idempotent load.")

        # load in FK-safe order
        _copy_from_df(conn, doctors_df[["doctor_id", "doctor_name", "specialty"]], f"{PGSCHEMA}.doctors")
        _copy_from_df(conn, appts_df[["appointment_id","patient_id","doctor_id","appointment_datetime","status"]],
                      f"{PGSCHEMA}.appointments")

        conn.commit()
        logger.info("Load committed.")
    except Exception:
        conn.rollback()
        logger.exception("Load failed, rolled back.")
        raise
    finally:
        conn.close()
