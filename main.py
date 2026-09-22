from pathlib import Path
from app.utils.logger import setup_logger
from app.sources.csv_source import load_csv_source
from app.sources.database_source import load_database_source
from app.sources.api_source import fetch_api_source
from app.transformation.cleaner import clean_student_data
from app.transformation.transformer import transform_student_data
from app.transformation.integration import integrate_sources
from app.validation.quality import validate_student_records
from app.output.csv_writer import write_csv_output

logger = setup_logger("main_pipeline")


def run_pipeline() -> None:
    """Orchestrates the entire Student Data Integration & ETL Pipeline:

    1. Extraction:
       - Demographic CSV (pandas)
       - Academic records (SQLite relational query with JOIN on student_id)
       - Attendance records (REST API with error handling & fallback)
    2. Cleaning & Standardization:
       - Text normalization (city casing, extra whitespace, name formatting)
       - Deduplication
    3. Integration:
       - Multi-source join on primary key 'student_id'
    4. Transformation & Feature Engineering:
       - Type conversions
       - Derived metric: performance_level (from GPA)
       - Derived metric: attendance_status (from attendance_rate)
    5. Data Quality Validation:
       - Strict range checks (Age 16-80, GPA 0-4, Attendance 0-100, student_id non-null/unique)
       - Routing rejected records with error_reason
    6. Loading / Persistence:
       - Exporting to final_dataset.csv and rejected_records.csv
       - Logging execution history to logs/pipeline.log
    """
    base_dir = Path(__file__).resolve().parent
    raw_csv_path = base_dir / "data" / "raw" / "students.csv"
    db_path = base_dir / "database" / "students.db"
    processed_output_path = base_dir / "data" / "processed" / "final_dataset.csv"
    rejected_output_path = base_dir / "data" / "rejected" / "rejected_records.csv"

    logger.info("=================================================================")
    logger.info(">>> STARTING STUDENT DATA INTEGRATION & ETL PIPELINE EXECUTION <<<")
    logger.info("=================================================================")

    # -------------------------------------------------------------
    # STAGE 1: Data Extraction
    # -------------------------------------------------------------
    logger.info("[Stage 1/5] Extracting data from heterogeneous sources...")
    
    # 1.1 CSV Source (Demographics)
    df_csv = load_csv_source(raw_csv_path)

    # 1.2 Database Source (Academic Profiles & Grades via SQL JOIN)
    df_db = load_database_source(db_path)

    # 1.3 REST API Source (Student Attendance Records)
    df_api = fetch_api_source()

    logger.info(
        f"Extraction complete: CSV={len(df_csv)} records, DB={len(df_db)} records, API={len(df_api)} records."
    )

    # -------------------------------------------------------------
    # STAGE 2: Cleaning & Text Standardization
    # -------------------------------------------------------------
    logger.info("[Stage 2/5] Cleaning demographics and standardizing textual attributes...")
    df_csv_cleaned = clean_student_data(df_csv)

    # -------------------------------------------------------------
    # STAGE 3: Multi-Source Data Integration
    # -------------------------------------------------------------
    logger.info("[Stage 3/5] Integrating extracted sources on primary key 'student_id'...")
    df_integrated = integrate_sources(
        sources=[df_csv_cleaned, df_db, df_api],
        join_key="student_id",
        how="outer",
    )

    # -------------------------------------------------------------
    # STAGE 4: Transformation & Feature Engineering
    # -------------------------------------------------------------
    logger.info("[Stage 4/5] Applying type casts and generating derived metrics...")
    df_transformed = transform_student_data(df_integrated)

    # -------------------------------------------------------------
    # STAGE 5: Data Quality Validation & Record Segregation
    # -------------------------------------------------------------
    logger.info("[Stage 5/5] Enforcing strict quality gates and isolating defective records...")
    valid_df, rejected_df = validate_student_records(df_transformed)

    # -------------------------------------------------------------
    # STAGE 6: Loading / Export
    # -------------------------------------------------------------
    logger.info("Persisting datasets to storage layers...")
    write_csv_output(valid_df, processed_output_path)
    write_csv_output(rejected_df, rejected_output_path)

    # -------------------------------------------------------------
    # Execution Summary Report
    # -------------------------------------------------------------
    logger.info("=================================================================")
    logger.info(">>> PIPELINE EXECUTION SUMMARY REPORT <<<")
    logger.info(f"Total Consolidated Candidates: {len(df_transformed)}")
    logger.info(f"Accepted Valid Records       : {len(valid_df)} -> {processed_output_path.name}")
    logger.info(f"Rejected Defective Records   : {len(rejected_df)} -> {rejected_output_path.name}")
    logger.info("All execution steps successfully logged to logs/pipeline.log")
    logger.info("=================================================================")


if __name__ == "__main__":
    run_pipeline()
