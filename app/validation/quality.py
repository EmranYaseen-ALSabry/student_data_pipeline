from typing import List, Tuple
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("quality_validator")


def validate_student_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Enforces strict data quality and governance gates on integrated student records:

    ========================================================================
    ARCHITECTURAL POLICY: CRITICAL FIELDS VS. OPTIONAL ADDITIONAL FIELDS
    ========================================================================
    1. Critical Core Fields (MANDATORY & STRICT):
       Violations in these rules immediately isolate the candidate record into
       the rejected dataset (`rejected_records.csv`) with explicit `error_reason`.
       - student_id: Must not be null/empty, must be a positive integer, must be unique.
       - age: If present, must be within academic bounds: 16 <= age <= 80.
       - gpa: If present, must be within valid grading scale: 0.0 <= gpa <= 4.0.
       - attendance_rate: If present, must be a valid percentage: 0.0 <= attendance_rate <= 100.0.
       - score: If present, must be within valid bounds: 0.0 <= score <= 100.0.
       - email: If present, must contain '@' and domain dot.

    2. Optional Additional Fields (MONGODB & EXTENDED PROFILES):
       Absence or partial completion of these fields NEVER causes record rejection:
       - contact.phone, contact.emergency_contact
       - address.street, address.city, address.country
       - guardian.name, guardian.relationship, guardian.phone
       - skills, courses, projects
       Missing values in these optional fields are preserved as 'N/A' or empty.

    Args:
        df: Consolidated student DataFrame after transformation.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (valid_records, rejected_records)
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to quality validator.")
        return df, pd.DataFrame()

    logger.info(f"Executing data quality validation checks on {len(df)} candidate records...")
    df_copy = df.copy()
    reasons_list: List[str | None] = []

    # Check for duplicate student_ids among non-null candidate rows
    duplicated_ids = set()
    if "student_id" in df_copy.columns:
        valid_ids = df_copy["student_id"].dropna()
        duplicated_ids = set(valid_ids[valid_ids.duplicated()].tolist())

    for idx, row in df_copy.iterrows():
        errors = []

        # -------------------------------------------------------------
        # Critical Rule 1: student_id validation (Must be non-null, > 0, unique)
        # -------------------------------------------------------------
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

        # -------------------------------------------------------------
        # Critical Rule 2: Age boundary validation (16 <= age <= 80)
        # -------------------------------------------------------------
        if "age" in row and pd.notna(row["age"]):
            try:
                age_val = float(row["age"])
                if age_val < 16 or age_val > 80:
                    errors.append(f"Age out of bounds [16-80]: {age_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric age value")

        # -------------------------------------------------------------
        # Critical Rule 3: GPA boundary validation (0.0 <= gpa <= 4.0)
        # -------------------------------------------------------------
        if "gpa" in row and pd.notna(row["gpa"]):
            try:
                gpa_val = float(row["gpa"])
                if gpa_val < 0.0 or gpa_val > 4.0:
                    errors.append(f"GPA out of bounds [0.0-4.0]: {gpa_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric GPA value")

        # -------------------------------------------------------------
        # Critical Rule 4: Attendance rate boundary (0.0 <= rate <= 100.0)
        # -------------------------------------------------------------
        if "attendance_rate" in row and pd.notna(row["attendance_rate"]):
            try:
                att_val = float(row["attendance_rate"])
                if att_val < 0.0 or att_val > 100.0:
                    errors.append(f"Attendance rate out of bounds [0-100]: {att_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric attendance rate")

        # -------------------------------------------------------------
        # Critical Rule 5: Score boundary validation (0.0 <= score <= 100.0)
        # -------------------------------------------------------------
        if "score" in row and pd.notna(row["score"]):
            try:
                score_val = float(row["score"])
                if score_val < 0.0 or score_val > 100.0:
                    errors.append(f"Score out of bounds [0-100]: {score_val}")
            except (ValueError, TypeError):
                errors.append("Non-numeric score")

        # -------------------------------------------------------------
        # Critical Rule 6: Email syntax check if present
        # -------------------------------------------------------------
        if "email" in row and pd.notna(row["email"]):
            email_str = str(row["email"]).strip()
            if "@" not in email_str or "." not in email_str.split("@")[-1]:
                errors.append(f"Invalid email format: '{email_str}'")

        # -------------------------------------------------------------
        # Optional MongoDB Fields: Checked for awareness, non-blocking
        # -------------------------------------------------------------
        # Missing contact, guardian, skills, or projects NEVER generates rejection errors.

        if errors:
            reasons_list.append("; ".join(errors))
        else:
            reasons_list.append(None)

    df_copy["error_reason"] = reasons_list

    # Segregate valid from rejected records
    rejected_mask = df_copy["error_reason"].notna()
    rejected_records = df_copy[rejected_mask].copy()
    valid_records = df_copy[~rejected_mask].drop(columns=["error_reason"]).copy()

    logger.info(
        f"Validation complete: {len(valid_records)} records PASSED quality gates, "
        f"{len(rejected_records)} records REJECTED."
    )

    return valid_records, rejected_records
