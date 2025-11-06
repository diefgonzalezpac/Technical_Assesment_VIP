# VIP Assesment ETL — from Excel files → PostgreSQL (default) or DuckDB (for simplicity)

An idempotent **ETL pipeline** that reads two Excel files (doctors, appointments), cleans/transforms them, and loads the results into:
- **PostgreSQL** (default; via Docker or your local server), or
- **DuckDB** (no server required) with `--duckdb`.

It also writes final cleaned CSVs and includes business SQL queries.

## 🧰 Tech Stack

- **Python** (pandas, openpyxl, python-dotenv)
- **PostgreSQL** (via Docker or local)
- **DuckDB** (embedded; single file)
- **Logging** to console and `logs/etl.log`

---

## 📦 Repository Layout

healthtech-etl/
├─ etl/
│ ├─ extract.py # read Excel sources
│ ├─ transform.py # normalize/clean data
│ ├─ load.py # PostgreSQL loader (TRUNCATE/LOAD)
│ ├─ load_duckdb.py # DuckDB loader (CREATE OR REPLACE TABLE)
│ ├─ logging_utils.py # console + file logs
│ └─ config.py # paths, env vars, backends
├─ data/
│ ├─ raw/
│ │ ├─ Data Enginner's Doctors Excel - VIP Medical Group.xlsx
│ │ └─ Data Engineer's Appointments Excel - VIP Medical Group.xlsx
│ └─ processed/ # output CSVs
├─ logs/ # etl.log written here
├─ queries.sql # business questions
├─ docker-compose.yml # optional local Postgres (14)
├─ requirements.txt
├─ .env.example
└─ main.py

## 🚀 Quick Start

```bash
git clone https://github.com/your-org/sales-etl.git
cd VIP_Test

python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt
```
**(VERY IMPORTANT)** I have Include two possible ways to run the ETL

First one with PostgreSQL integration (No Dockerized)

```bash
cp .env.example .env
# .env defaults:
# PGHOST=localhost
# PGPORT=5432
# PGUSER=postgres
# PGPASSWORD=postgres
# PGDATABASE=postgres
# PGSCHEMA=healthtech

# Run ETL (PostgreSQL backend)
python main.py
```

Second one (Recommended) Run with DuckDB (no server required)
```bash
# Run ETL (DuckDB backend)
python main.py --duckdb
```

## 📥 Inputs  📤 Outputs

- **Inputs** (place in data/raw/):

    Data Enginner's Doctors Excel - VIP Medical Group.xlsx (sheet: doctors)

    Data Engineer's Appointments Excel - VIP Medical Group.xlsx (sheet: appointments)

- **Outputs:**

    Final datasets (CSV): data/processed/doctors_clean.csv, data/processed/appointments_clean.csv

- **Database:**

    Postgres: schema healthtech, tables doctors, appointments

    DuckDB: file healthtech.duckdb with schema healthtech and the same tables

- **Logs:** logs/etl.log

## Questions

🔢 Q1 — Which doctor has the most confirmed appointments? **ANSWER 152**

![image alt](https://github.com/diefgonzalezpac/Technical_Assesment_VIP/blob/4e50da54e98833b9b40df405006a13bfae4a15f4/Query%20Question%201.png)

🧍‍♀️ Q2 — How many confirmed appointments does the patient with patient_id ‘34’ have? **ANSWER 14**

![image alt](https://github.com/diefgonzalezpac/Technical_Assesment_VIP/blob/4e50da54e98833b9b40df405006a13bfae4a15f4/Query%20Question%202.png)

🗓️ Q3 — How many cancelled appointments are there between Oct 21, 2025 and Oct 24, 2025 (inclusive)? **ANSWER 66**

![image alt](https://github.com/diefgonzalezpac/Technical_Assesment_VIP/blob/4e50da54e98833b9b40df405006a13bfae4a15f4/Query%20Question%203.png)

🧑‍⚕️ Q4 — What is the total number of confirmed appointments for each doctor? **ANSWER See table below**

![image alt](https://github.com/diefgonzalezpac/Technical_Assesment_VIP/blob/4e50da54e98833b9b40df405006a13bfae4a15f4/Query%20Question%204.png)

## AWS PROD ETL

-- **Orchestration:** 

    Amazon MWAA (Managed Airflow)
    Centralized DAGs, retries, SLAs, dependency management, auditability.

-- **Extract (E):** 

    AWS Lambda on S3 events or scheduled by Airflow
    Validates uploads, unzips/xlsx→Parquet, basic normalization, pushes to S3 raw/bronze.

-- **Transform (T):**

    ELT in Snowflake using dbt (models, tests, docs) and/or SnowflakeOperator from Airflow. This keeps logic closer to the data and scales with Snowflake’s compute.

-- **Load (L):**

    As discussed in our last meeting Snowflake is the company’s warehouse
    Airflow → SnowflakeOperator: run COPY INTO on a schedule with file manifests 

## Why these tools?

-- **MWAA (Airflow):** Standard orchestration, rich operator ecosystem (Snowflake, S3, Lambda, Glue), operational visibility, retries and also because of the kind of data, we have medical records about appointments that could be included day by day using cron operations, Airflow is an amaizing tool for this kind of scenarios

-- **Lambda (Extraction):** Serverless, cheap for light/medium workload; perfect for parsing Excel → Parquet and metadata validation. Seconds to minutes runtime, for simplicity.

-- **Snowflake + DBT:(Transformation + Load):** Because of the little amount of data services like AWS Glue could be too much, light dbt operations inside Snowflake could be used for data modeling, testing, and table creations simplifying the operation
