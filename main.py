from pathlib import Path
from time import perf_counter
from dotenv import load_dotenv
import pandas as pd

from app.utils.logger import setup_logger
from app.sources.csv_source import load_csv_source
from app.sources.database_source import load_database_source
from app.sources.api_source import fetch_api_source
from app.sources.mongodb_source import load_mongodb_source
from app.transformation.cleaner import clean_student_data
from app.transformation.transformer import transform_student_data
from app.transformation.integration import integrate_sources
from app.validation.quality import validate_source_records, validate_student_records
from app.output.csv_writer import write_csv_output

# Load environment configuration (.env)
project_root = Path(__file__).resolve().parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(dotenv_path=env_file)
else:
    load_dotenv()

logger = setup_logger("main_pipeline")

OUTPUT_COLUMNS = [
    "student_id",
    "name",
    "age",
    "email",
    "contact.phone",
    "address.street",
    "address.city",
    "address.country",
    "major",
    "enrollment_year",
    "gpa",
    "total_credits",
    "attendance_rate",
    "performance_level",
    "attendance_status",
    "skills",
    "courses",
    "projects",
    "guardian.name",
    "guardian.relationship",
    "guardian.phone",
]


def run_pipeline() -> None:
    """Orchestrates the entire Student Data Integration & ETL Pipeline across 4 heterogeneous sources:

    1. Data Extraction (CSV, SQLite Relational DB, REST API, MongoDB NoSQL)
    2. Data Cleaning & Normalization for all extracted sources
    3. Source-level Quality Validation before Integration
    4. Multi-Source Integration (Full Outer Join on student_id)
    5. Transformation and feature engineering
    6. Final Quality Validation
    7. Loading and reporting accepted, rejected, and pipeline metrics
    """
    pipeline_started = perf_counter()
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
    logger.info("[Stage 1/7] Extracting data from heterogeneous sources...")

    # 1.1 CSV Source (Demographics)
    df_csv = load_csv_source(raw_csv_path)

    # 1.2 SQLite Database Source (Academic Profiles & Grades via SQL JOIN)
    df_db = load_database_source(db_path)

    # 1.3 REST API Source (Student Attendance Records with Resilient Fallback)
    df_api = fetch_api_source(endpoint_url="https://emranyaseen.pythonanywhere.com/my-json")

    # 1.4 MongoDB Source (Additional Student Profiles, Contacts, Skills & Projects)
    df_mongo = load_mongodb_source()

    logger.info(
        f"Extraction Summary: CSV={len(df_csv)} records, DB={len(df_db)} records, "
        f"API={len(df_api)} records, MongoDB={len(df_mongo)} records."
    )

    # -------------------------------------------------------------
    # STAGE 2: Cleaning & Text Standardization
    # -------------------------------------------------------------
    logger.info("[Stage 2/7] Cleaning and standardizing all extracted sources...")
    source_frames = [
        ("CSV", df_csv),
        ("SQLite", df_db),
        ("REST API", df_api),
        ("MongoDB", df_mongo),
    ]
    cleaned_sources = []
    exact_duplicates_removed = 0
    for source_name, source_df in source_frames:
        cleaned_df = clean_student_data(source_df)
        exact_duplicates_removed += len(source_df) - len(cleaned_df)
        cleaned_sources.append((source_name, cleaned_df))

    logger.info("Removed %s exact duplicate source rows during cleaning.", exact_duplicates_removed)

    # -------------------------------------------------------------
    # STAGE 3: Source-Level Quality Validation
    # -------------------------------------------------------------
    logger.info("[Stage 3/7] Validating source records before integration...")
    validated_sources = []
    source_rejections = []
    for source_name, source_df in cleaned_sources:
        valid_source, rejected_source = validate_source_records(source_df, source_name)
        validated_sources.append(valid_source)
        if not rejected_source.empty:
            source_rejections.append(rejected_source)

    # -------------------------------------------------------------
    # STAGE 4: Multi-Source Data Integration (Outer Join)
    # -------------------------------------------------------------
    logger.info("[Stage 4/7] Integrating 4 data sources on primary key 'student_id'...")
    df_integrated = integrate_sources(
        sources=validated_sources,
        join_key="student_id",
        how="outer",
    )

    # -------------------------------------------------------------
    # STAGE 5: Transformation, Metrics & Array Serialization
    # -------------------------------------------------------------
    logger.info("[Stage 5/7] Applying type casts, derived metrics, and NoSQL array serialization...")
    for column in OUTPUT_COLUMNS:
        if column not in df_integrated.columns:
            df_integrated[column] = pd.NA
    df_transformed = transform_student_data(df_integrated)

    # -------------------------------------------------------------
    # STAGE 6: Final Data Quality Validation & Error Segregation
    # -------------------------------------------------------------
    logger.info("[Stage 6/7] Enforcing final quality gates and isolating defective records...")
    valid_df, final_rejections = validate_student_records(df_transformed)
    rejected_frames = source_rejections + [final_rejections]
    rejected_df = (
        pd.concat(rejected_frames, ignore_index=True, sort=False)
        if any(not frame.empty for frame in rejected_frames)
        else pd.DataFrame()
    )

    # -------------------------------------------------------------
    # STAGE 7: Loading & Export
    # -------------------------------------------------------------
    logger.info("[Stage 7/7] Persisting processed datasets to storage layers...")
    write_csv_output(valid_df.reindex(columns=OUTPUT_COLUMNS), processed_output_path)
    write_csv_output(rejected_df, rejected_output_path)

    elapsed_seconds = perf_counter() - pipeline_started
    missing_values = int(df_transformed.isna().sum().sum())
    duplicate_id_rejections = sum(
        rejected.get("error_reason", pd.Series(dtype="string"))
        .astype("string")
        .str.contains("Duplicate student_id", na=False)
        .sum()
        for rejected in source_rejections
    )

    # -------------------------------------------------------------
    # Execution Summary Report
    # -------------------------------------------------------------
    logger.info("=================================================================")
    logger.info(">>> PIPELINE EXECUTION SUMMARY REPORT <<<")
    logger.info(f"Total Consolidated Candidates : {len(df_transformed)}")
    logger.info(f"Accepted Valid Records        : {len(valid_df)} -> {processed_output_path.name}")
    logger.info(f"Rejected Defective Records    : {len(rejected_df)} -> {rejected_output_path.name}")
    logger.info(f"Exact Duplicate Rows Removed  : {exact_duplicates_removed}")
    logger.info(f"Duplicate ID Source Rejections: {duplicate_id_rejections}")
    logger.info(f"Missing Values Before Quality : {missing_values}")
    logger.info(f"Processing Time               : {elapsed_seconds:.2f} seconds")
    logger.info("Audit log recorded to         : logs/pipeline.log")
    logger.info("=================================================================")


if __name__ == "__main__":
    run_pipeline()
