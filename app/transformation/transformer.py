from typing import Any
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("transformer")


def transform_student_data(df: pd.DataFrame) -> pd.DataFrame:
    """Applies type casting and computes essential derived features:

    1. Casts student_id, age, enrollment_year, total_credits to appropriate integer types.
    2. Casts gpa and attendance_rate to float.
    3. Normalizes email addresses to lowercase.
    4. Derives 'performance_level' based on GPA.
    5. Derives 'attendance_status' based on attendance_rate.

    Args:
        df: Cleaned input DataFrame.

    Returns:
        pd.DataFrame: Transformed DataFrame with enriched derived columns.
    """
    if df.empty:
        return df

    logger.info("Applying data transformations and computing derived academic metrics...")
    transformed = df.copy()

    # 1. Type Casting
    if "student_id" in transformed.columns:
        transformed["student_id"] = pd.to_numeric(
            transformed["student_id"], errors="coerce"
        ).astype("Int64")

    if "age" in transformed.columns:
        transformed["age"] = pd.to_numeric(
            transformed["age"], errors="coerce"
        ).astype("Int64")

    if "enrollment_year" in transformed.columns:
        transformed["enrollment_year"] = pd.to_numeric(
            transformed["enrollment_year"], errors="coerce"
        ).astype("Int64")

    if "total_credits" in transformed.columns:
        transformed["total_credits"] = pd.to_numeric(
            transformed["total_credits"], errors="coerce"
        ).astype("Int64")

    if "gpa" in transformed.columns:
        transformed["gpa"] = pd.to_numeric(transformed["gpa"], errors="coerce")

    if "attendance_rate" in transformed.columns:
        transformed["attendance_rate"] = pd.to_numeric(
            transformed["attendance_rate"], errors="coerce"
        )

    if "email" in transformed.columns:
        transformed["email"] = transformed["email"].astype(str).str.lower().str.strip()
        transformed.loc[transformed["email"].isin(["nan", "none", "<na>", ""], ), "email"] = pd.NA

    # 2. Derived Feature: performance_level (based on GPA)
    if "gpa" in transformed.columns:
        transformed["performance_level"] = transformed["gpa"].apply(_compute_performance_level)

    # 3. Derived Feature: attendance_status (based on attendance_rate)
    if "attendance_rate" in transformed.columns:
        transformed["attendance_status"] = transformed["attendance_rate"].apply(_compute_attendance_status)

    logger.info("Transformation and feature engineering completed successfully.")
    return transformed


def _compute_performance_level(gpa: Any) -> str:
    """Classifies student performance based on GPA scale (0.0 - 4.0)."""
    if pd.isna(gpa):
        return "Not Available"
    try:
        val = float(gpa)
        if val >= 3.7:
            return "Excellent"
        if val >= 3.0:
            return "Very Good"
        if val >= 2.5:
            return "Good"
        if val >= 2.0:
            return "Satisfactory"
        return "Academic Probation"
    except (ValueError, TypeError):
        return "Not Available"


def _compute_attendance_status(attendance_rate: Any) -> str:
    """Classifies attendance compliance based on percentage attendance_rate (0 - 100)."""
    if pd.isna(attendance_rate):
        return "Not Available"
    try:
        val = float(attendance_rate)
        if val >= 85.0:
            return "Regular"
        if val >= 75.0:
            return "Needs Improvement"
        return "Critical Warning"
    except (ValueError, TypeError):
        return "Not Available"
