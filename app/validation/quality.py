from typing import List, Tuple
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("quality_validator")


def validate_student_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Enforces strict data quality and governance rules on integrated student records:

    Rules:
    1. student_id: Must not be null/empty, must be a positive integer, and must be unique.
    2. age: Must be numeric and between 16 and 80 inclusive (16 <= age <= 80).
    3. gpa: Must be numeric and between 0.0 and 4.0 inclusive (0.0 <= gpa <= 4.0).
    4. attendance_rate: Must be numeric and between 0.0 and 100.0 inclusive (0.0 <= rate <= 100.0).
    5. score (if present): Must be numeric and between 0.0 and 100.0 inclusive.

    Non-compliant records are segregated with specific failure reasons stored in 'error_reason'.

    Args:
        df: Enriched student DataFrame.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (valid_records, rejected_records)
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to quality validator.")
        return df, pd.DataFrame()

    logger.info(f"Executing data quality validation checks on {len(df)} records...")
    df_copy = df.copy()
    reasons_list: List[str | None] = []

    # Check for duplicate student_ids among non-null rows
    duplicated_ids = set()
    if "student_id" in df_copy.columns:
        valid_ids = df_copy["student_id"].dropna()
        duplicated_ids = set(valid_ids[valid_ids.duplicated()].tolist())

    for idx, row in df_copy.iterrows():
        errors = []

        # Rule 1: student_id validation
        if "student_id" not in row or pd.isna(row["student_id"]):
            errors.append("Missing student_id")
        else:
            try:
                sid = int(row["student_id"])
                if sid <= 0:
                    errors.append("Invalid student_id: must be positive integer")
                elif sid in duplicated_ids:
                    errors.append(f"Duplicate student_id ({sid})")
            except (ValueError, TypeError):
                errors.append("Non-integer student_id")

        # Rule 2: age validation (16 <= age <= 80)
        if "age" in row and pd.notna(row["age"]):
            try:
                age_val = float(row["age"])
                if age_val < 16 or age_val > 80:
                    errors.append(f"Age out of bounds [16-80]: {age_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric age value")

        # Rule 3: GPA validation (0.0 <= gpa <= 4.0)
        if "gpa" in row and pd.notna(row["gpa"]):
            try:
                gpa_val = float(row["gpa"])
                if gpa_val < 0.0 or gpa_val > 4.0:
                    errors.append(f"GPA out of bounds [0.0-4.0]: {gpa_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric GPA value")

        # Rule 4: Attendance rate validation (0.0 <= attendance_rate <= 100.0)
        if "attendance_rate" in row and pd.notna(row["attendance_rate"]):
            try:
                att_val = float(row["attendance_rate"])
                if att_val < 0.0 or att_val > 100.0:
                    errors.append(f"Attendance rate out of bounds [0-100]: {att_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric attendance rate")

        # Rule 5: Score validation (0.0 <= score <= 100.0) if present
        if "score" in row and pd.notna(row["score"]):
            try:
                score_val = float(row["score"])
                if score_val < 0.0 or score_val > 100.0:
                    errors.append(f"Score out of bounds [0-100]: {score_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric score")

        # Rule 6: Email validation format check if present
        if "email" in row and pd.notna(row["email"]):
            email_str = str(row["email"]).strip()
            if "@" not in email_str or "." not in email_str.split("@")[-1]:
                errors.append(f"Invalid email format: '{email_str}'")

        if errors:
            reasons_list.append("; ".join(errors))
        else:
            reasons_list.append(None)

    df_copy["error_reason"] = reasons_list

    # Separate valid and rejected records
    rejected_mask = df_copy["error_reason"].notna()
    rejected_records = df_copy[rejected_mask].copy()
    valid_records = df_copy[~rejected_mask].drop(columns=["error_reason"]).copy()

    logger.info(
        f"Validation complete: {len(valid_records)} records PASSED quality gates, "
        f"{len(rejected_records)} records REJECTED."
    )

    return valid_records, rejected_records

