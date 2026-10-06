import math
from typing import List, Tuple

import pandas as pd

from app.utils.logger import setup_logger

logger = setup_logger("quality_validator")

NUMERIC_BOUNDS = {
    "age": (16.0, 80.0),
    "gpa": (0.0, 4.0),
    "attendance_rate": (0.0, 100.0),
    "score": (0.0, 100.0),
}
INTEGER_FIELDS = {"age", "enrollment_year", "total_credits"}
REQUIRED_FINAL_FIELDS = ("age", "gpa", "attendance_rate")
FIELD_LABELS = {
    "age": "Age",
    "gpa": "GPA",
    "attendance_rate": "Attendance rate",
    "score": "Score",
}


def validate_source_records(
    df: pd.DataFrame,
    source_name: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validate source-level keys and any quality fields provided by that source."""
    if df.empty:
        logger.warning("Empty DataFrame passed to source validator for %s.", source_name)
        return df.copy(), pd.DataFrame()

    df = df.reset_index(drop=True)
    logger.info(
        "Validating %s source records from %s before integration...",
        len(df),
        source_name,
    )
    numeric_ids = (
        pd.to_numeric(df["student_id"], errors="coerce")
        if "student_id" in df.columns
        else pd.Series(float("nan"), index=df.index)
    )
    integral_positive_ids = numeric_ids.map(
        lambda value: pd.notna(value)
        and math.isfinite(float(value))
        and float(value).is_integer()
        and value > 0
    )
    duplicate_ids = set(
        numeric_ids[integral_positive_ids][
            numeric_ids[integral_positive_ids].duplicated(keep=False)
        ].tolist()
    )

    errors_by_index: dict[object, str] = {}
    for index, row in df.iterrows():
        errors: List[str] = []
        _validate_student_id(row.get("student_id"), duplicate_ids, errors)
        _validate_present_fields(row, errors)
        if errors:
            errors_by_index[index] = "; ".join(errors)

    rejected_mask = pd.Series(df.index.isin(errors_by_index), index=df.index)
    rejected = df.loc[rejected_mask].copy()
    if not rejected.empty:
        rejected["source_name"] = source_name
        rejected["rejection_stage"] = "source_validation"
        rejected["error_reason"] = [
            errors_by_index[index] for index in rejected.index
        ]

    valid = df.loc[~rejected_mask].copy()
    if "student_id" in valid.columns:
        valid["student_id"] = pd.to_numeric(
            valid["student_id"], errors="raise"
        ).astype("Int64")

    logger.info(
        "%s source validation complete: %s valid, %s rejected.",
        source_name,
        len(valid),
        len(rejected),
    )
    return valid, rejected


def validate_student_records(
    df: pd.DataFrame,
    source_name: str = "integrated",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply final quality gates and reject incomplete analytical records.

    The selected missing-value policy is conservative: student_id, age, GPA,
    and attendance are required for the final dataset. Optional profile fields
    remain missing rather than being filled with invented values.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to final quality validator.")
        return df.copy(), pd.DataFrame()

    df = df.reset_index(drop=True)
    logger.info("Executing final quality checks on %s records...", len(df))
    duplicated_ids = set()
    if "student_id" in df.columns:
        numeric_ids = pd.to_numeric(df["student_id"], errors="coerce")
        duplicated_ids = set(
            numeric_ids[numeric_ids.duplicated(keep=False)].dropna().tolist()
        )

    errors_by_index: dict[object, str] = {}
    for index, row in df.iterrows():
        errors: List[str] = []

        for field in REQUIRED_FINAL_FIELDS:
            if _is_missing(row.get(field)):
                errors.append(f"Missing required field: {field}")

        _validate_student_id(row.get("student_id"), duplicated_ids, errors)
        _validate_present_fields(row, errors)
        if errors:
            errors_by_index[index] = "; ".join(errors)

    rejected_mask = pd.Series(df.index.isin(errors_by_index), index=df.index)
    rejected = df.loc[rejected_mask].copy()
    if not rejected.empty:
        rejected["source_name"] = source_name
        rejected["rejection_stage"] = "final_validation"
        rejected["error_reason"] = [
            errors_by_index[index] for index in rejected.index
        ]

    valid = df.loc[~rejected_mask].copy()
    logger.info(
        "Final validation complete: %s valid, %s rejected.",
        len(valid),
        len(rejected),
    )
    return valid, rejected


def _validate_student_id(
    value: object,
    duplicate_ids: set[object],
    errors: List[str],
) -> None:
    if _is_missing(value):
        errors.append("Missing student_id")
        return

    numeric_value = _as_finite_number(value)
    if numeric_value is None or not numeric_value.is_integer():
        errors.append("Non-integer student_id")
    elif numeric_value <= 0:
        errors.append("Invalid student_id: must be positive integer")
    elif numeric_value in duplicate_ids:
        errors.append(f"Duplicate student_id ({int(numeric_value)})")


def _validate_present_fields(row: pd.Series, errors: List[str]) -> None:
    for field, (minimum, maximum) in NUMERIC_BOUNDS.items():
        value = row.get(field)
        if field not in row.index or _is_missing(value):
            continue

        numeric_value = _as_finite_number(value)
        if numeric_value is None:
            errors.append(f"Non-numeric {FIELD_LABELS[field]} value")
        elif numeric_value < minimum or numeric_value > maximum:
            label = FIELD_LABELS[field]
            errors.append(
                f"{label} out of bounds [{minimum:g}-{maximum:g}]: {numeric_value:g}"
            )
        elif field in INTEGER_FIELDS and not numeric_value.is_integer():
            errors.append(f"Non-integer {field} value")

    for field in ("enrollment_year", "total_credits"):
        value = row.get(field)
        if field not in row.index or _is_missing(value):
            continue
        numeric_value = _as_finite_number(value)
        if numeric_value is None:
            errors.append(f"Non-numeric {field} value")
        elif not numeric_value.is_integer():
            errors.append(f"Non-integer {field} value")

    email = row.get("email")
    if not _is_missing(email):
        email_value = str(email).strip()
        if "@" not in email_value or "." not in email_value.rsplit("@", 1)[-1]:
            errors.append(f"Invalid email format: '{email_value}'")


def _as_finite_number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _is_missing(value: object) -> bool:
    if value is None or value is pd.NA:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False
