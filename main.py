from pathlib import Path
from dotenv import load_dotenv

from app.utils.logger import setup_logger
from app.sources.csv_source import load_csv_source
from app.sources.database_source import load_database_source
from app.sources.api_source import fetch_api_source
from app.sources.mongodb_source import load_mongodb_source
from app.transformation.cleaner import clean_student_data
from app.transformation.transformer import transform_student_data
from app.transformation.integration import integrate_sources
from app.validation.quality import validate_student_records
from app.output.csv_writer import write_csv_output

# Load environment configuration (.env)
project_root = Path(__file__).resolve().parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(dotenv_path=env_file)
else:
    load_dotenv()

logger = setup_logger("main_pipeline")


def run_pipeline() -> None:
    """Orchestrates the entire Student Data Integration & ETL Pipeline across 4 heterogeneous sources:

    1. Data Extraction (CSV, SQLite Relational DB, REST API, MongoDB NoSQL)
    2. Data Cleaning & Normalization (Whitespace, Casing, City Standardizations)
    3. Multi-Source Integration (Full Outer Join on student_id)
    4. Transformation & Feature Engineering (Type Casting, GPA/Attendance Metrics, NoSQL Array Serialization)
    5. Data Quality Validation (Zero-Trust Quality Gates, Critical vs Optional Isolation)
    6. Loading & Reporting (Persisting final_dataset.csv, rejected_records.csv, and logs/pipeline.log)
    """
    raw_csv_path = project_root / "data" / "raw" / "students.csv"
    db_path = project_root / "database" / "students.db"
    processed_output_path = project_root / "data" / "processed" / "final_dataset.csv"
    rejected_output_path = project_root / "data" / "rejected" / "rejected_records.csv"

    logger.info("=================================================================")
    logger.info(">>> STARTING STUDENT DATA INTEGRATION & ETL PIPELINE EXECUTION <<<")
    logger.info("=================================================================")

    # -------------------------------------------------------------
    # STAGE 1: Data Extraction (4 Heterogeneous Sources)
    # -------------------------------------------------------------
    logger.info("[Stage 1/6] Extracting data from heterogeneous sources...")

    # 1.1 CSV Source (Demographics)
    df_csv = load_csv_source(raw_csv_path)

    # 1.2 SQLite Database Source (Academic Profiles & Grades via SQL JOIN)
    df_db = load_database_source(db_path)

    # 1.3 REST API Source (Student Attendance Records with Resilient Fallback)
    df_api = fetch_api_source()

    # 1.4 MongoDB Source (Additional Student Profiles, Contacts, Skills & Projects)
    df_mongo = load_mongodb_source()

    logger.info(
        f"Extraction Summary: CSV={len(df_csv)} records, DB={len(df_db)} records, "
        f"API={len(df_api)} records, MongoDB={len(df_mongo)} records."
    )

    # -------------------------------------------------------------
    # STAGE 2: Cleaning & Text Standardization
    # -------------------------------------------------------------
    logger.info("[Stage 2/6] Cleaning demographics and standardizing textual attributes...")
    df_csv_cleaned = clean_student_data(df_csv)

    # -------------------------------------------------------------
    # STAGE 3: Multi-Source Data Integration (Outer Join)
    # -------------------------------------------------------------
    logger.info("[Stage 3/6] Integrating 4 data sources on primary key 'student_id'...")
    df_integrated = integrate_sources(
        sources=[df_csv_cleaned, df_db, df_api, df_mongo],
        join_key="student_id",
        how="outer",
    )

    # -------------------------------------------------------------
    # STAGE 4: Transformation, Metrics & Array Serialization
    # -------------------------------------------------------------
    logger.info("[Stage 4/6] Applying type casts, derived academic metrics, and NoSQL array serialization...")
    df_transformed = transform_student_data(df_integrated)

    # -------------------------------------------------------------
    # STAGE 5: Data Quality Validation & Error Segregation
    # -------------------------------------------------------------
    logger.info("[Stage 5/6] Enforcing strict quality gates and isolating defective records...")
    valid_df, rejected_df = validate_student_records(df_transformed)

    # -------------------------------------------------------------
    # STAGE 6: Loading & Export
    # -------------------------------------------------------------
    logger.info("[Stage 6/6] Persisting processed datasets to storage layers...")
    write_csv_output(valid_df, processed_output_path)
    write_csv_output(rejected_df, rejected_output_path)

    # -------------------------------------------------------------
    # Execution Summary Report
    # -------------------------------------------------------------
    logger.info("=================================================================")
    logger.info(">>> PIPELINE EXECUTION SUMMARY REPORT <<<")
    logger.info(f"Total Consolidated Candidates : {len(df_transformed)}")
    logger.info(f"Accepted Valid Records        : {len(valid_df)} -> {processed_output_path.name}")
    logger.info(f"Rejected Defective Records    : {len(rejected_df)} -> {rejected_output_path.name}")
    logger.info("Audit log recorded to         : logs/pipeline.log")
    logger.info("=================================================================")


if __name__ == "__main__":
    run_pipeline()
