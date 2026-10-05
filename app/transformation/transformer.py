from typing import Any, List, Optional
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("transformer")


def transform_student_data(df: pd.DataFrame) -> pd.DataFrame:
    """Applies type casting, feature engineering, and NoSQL array serialization:

    1. Casts student_id, age, enrollment_year, total_credits to Nullable Int64.
    2. Casts gpa and attendance_rate to float.
    3. Normalizes email addresses to lowercase.
    4. Derives 'performance_level' based on GPA.
    5. Derives 'attendance_status' based on attendance_rate.
    6. Harmonizes missing city values from MongoDB 'address.city' if available.
    7. Serializes MongoDB array columns ('skills', 'courses', 'projects') into CSV-compatible formats.

    Args:
        df: Consolidated input DataFrame after multi-source integration.

    Returns:
        pd.DataFrame: Transformed DataFrame with enriched derived columns and serialized arrays.
    """
    if df.empty:
        return df

    logger.info("Applying data transformations, feature engineering, and array serialization...")
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
        transformed.loc[
            transformed["email"].isin(["nan", "none", "<na>", ""]), "email"
        ] = pd.NA

    # 2. Coalesce address.city into city for MongoDB-exclusive students
    if "address.city" in transformed.columns:
        if "city" in transformed.columns:
            transformed["city"] = transformed["city"].combine_first(transformed["address.city"])
        else:
            transformed["city"] = transformed["address.city"]

    # 3. Derived Feature: performance_level (based on GPA)
    if "gpa" in transformed.columns:
        transformed["performance_level"] = transformed["gpa"].apply(_compute_performance_level)

    # 4. Derived Feature: attendance_status (based on attendance_rate)
    if "attendance_rate" in transformed.columns:
        transformed["attendance_status"] = transformed["attendance_rate"].apply(_compute_attendance_status)

    # 5. MongoDB Array Serialization for CSV Export
    if "skills" in transformed.columns:
        transformed["skills"] = transformed["skills"].apply(_serialize_skills)

    if "courses" in transformed.columns:
        transformed["courses"] = transformed["courses"].apply(_serialize_courses)

    if "projects" in transformed.columns:
        transformed["projects"] = transformed["projects"].apply(_serialize_projects)

    logger.info("Transformation and feature engineering completed successfully.")
    return transformed


def _compute_performance_level(gpa: Any) -> str:
    """Classifies student academic standing based on GPA scale (0.0 - 4.0)."""
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


def _serialize_skills(val: Any) -> str:
    """Serializes a list of skills into a clean, pipe-separated string for CSV compatibility."""
    if not isinstance(val, (list, tuple)) or not val:
        return "N/A"
    cleaned_skills = [str(x).strip() for x in val if str(x).strip()]
    return " | ".join(cleaned_skills) if cleaned_skills else "N/A"


def _serialize_courses(val: Any) -> str:
    """Serializes a list of course dicts into a human-readable string: 'Name (Grade%)'."""
    if not isinstance(val, (list, tuple)) or not val:
        return "N/A"
    parts = []
    for item in val:
        if isinstance(item, dict):
            name = item.get("name", "Course")
            grade = item.get("grade")
            parts.append(f"{name} ({grade}%)" if grade is not None else name)
        elif item:
            parts.append(str(item))
    return " | ".join(parts) if parts else "N/A"


def _serialize_projects(val: Any) -> str:
    """Serializes a list of project dicts into a string: 'Project [Tech1, Tech2]'."""
    if not isinstance(val, (list, tuple)) or not val:
        return "N/A"
    parts = []
    for item in val:
        if isinstance(item, dict):
            name = item.get("name", "Project")
            techs = item.get("technologies", [])
            tech_str = ", ".join(techs) if techs else ""
            parts.append(f"{name} [{tech_str}]" if tech_str else name)
        elif item:
            parts.append(str(item))
    return " | ".join(parts) if parts else "N/A"
