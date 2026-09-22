from pathlib import Path
import pytest
import pandas as pd

from app.sources.csv_source import load_csv_source
from app.sources.database_source import load_database_source
from app.sources.api_source import fetch_api_source, get_mock_attendance_data
from app.transformation.cleaner import clean_student_data, _normalize_city
from app.transformation.transformer import (
    transform_student_data,
    _compute_performance_level,
    _compute_attendance_status,
)
from app.transformation.integration import integrate_sources
from app.validation.quality import validate_student_records


@pytest.fixture
def project_dirs():
    base = Path(__file__).resolve().parent.parent
    return {
        "csv": base / "data" / "raw" / "students.csv",
        "db": base / "database" / "students.db",
    }


# ==========================================
# 1. Extraction Layer Tests
# ==========================================

def test_csv_extraction(project_dirs):
    """Verifies that CSV extraction correctly loads raw demographic records."""
    df = load_csv_source(project_dirs["csv"])
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "student_id" in df.columns
    assert "city" in df.columns


def test_csv_extraction_file_not_found():
    """Verifies that missing CSV raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_csv_source("non_existent_path.csv")


def test_database_extraction_with_sql_join(project_dirs):
    """Verifies SQLite extraction executes SQL join and returns academic records."""
    df = load_database_source(project_dirs["db"])
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # Verify joined columns from academic_profiles and academic_grades
    assert "student_id" in df.columns
    assert "major" in df.columns
    assert "gpa" in df.columns


def test_api_extraction_fallback_and_error_handling():
    """Verifies API source error handling for unreachable endpoints and invalid JSON."""
    # Test unreachable endpoint triggering fallback
    df_fallback = fetch_api_source("http://127.0.0.1:9999/unreachable_api", timeout=1)
    assert isinstance(df_fallback, pd.DataFrame)
    assert not df_fallback.empty
    assert "attendance_rate" in df_fallback.columns


# ==========================================
# 2. Cleaning & Normalization Tests
# ==========================================

def test_text_and_city_normalization():
    """Tests normalization of city names, cases, and whitespace."""
    assert _normalize_city("  cairo  ") == "Cairo"
    assert _normalize_city("RIYADH") == "Riyadh"
    assert _normalize_city("new york") == "New York"
    assert _normalize_city("alex") == "Alexandria"

    raw_df = pd.DataFrame({
        " Student ID ": [101, 101],
        " Name ": [" ahmed  ali ", " ahmed  ali "],
        " City ": ["  cairo  ", "  cairo  "],
    })
    cleaned = clean_student_data(raw_df)
    assert "student_id" in cleaned.columns
    assert "name" in cleaned.columns
    assert "city" in cleaned.columns
    # Check deduplication
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["city"] == "Cairo"
    assert cleaned.iloc[0]["name"] == "Ahmed Ali"


# ==========================================
# 3. Transformation & Feature Engineering Tests
# ==========================================

def test_performance_level_computation():
    """Verifies performance_level derivation based on GPA ranges."""
    assert _compute_performance_level(3.9) == "Excellent"
    assert _compute_performance_level(3.4) == "Very Good"
    assert _compute_performance_level(2.7) == "Good"
    assert _compute_performance_level(2.2) == "Satisfactory"
    assert _compute_performance_level(1.5) == "Academic Probation"
    assert _compute_performance_level(None) == "Not Available"


def test_attendance_status_computation():
    """Verifies attendance_status derivation based on attendance_rate percentage."""
    assert _compute_attendance_status(90.0) == "Regular"
    assert _compute_attendance_status(78.5) == "Needs Improvement"
    assert _compute_attendance_status(60.0) == "Critical Warning"
    assert _compute_attendance_status(None) == "Not Available"


def test_transformation_pipeline():
    """Tests DataFrame type casting and derived column additions."""
    df = pd.DataFrame({
        "student_id": ["101"],
        "age": ["21"],
        "gpa": ["3.85"],
        "attendance_rate": ["92.0"],
        "email": ["USER@EXAMPLE.COM "],
    })
    transformed = transform_student_data(df)
    assert transformed.iloc[0]["email"] == "user@example.com"
    assert transformed.iloc[0]["performance_level"] == "Excellent"
    assert transformed.iloc[0]["attendance_status"] == "Regular"


# ==========================================
# 4. Multi-Source Integration Tests
# ==========================================

def test_integrate_three_sources():
    """Tests outer merge of CSV, DB, and API datasets using student_id."""
    df_csv = pd.DataFrame([{"student_id": 101, "name": "Alice", "city": "Cairo"}])
    df_db = pd.DataFrame([{"student_id": 101, "gpa": 3.8, "major": "CS"}])
    df_api = pd.DataFrame([{"student_id": 101, "attendance_rate": 95.0}])

    integrated = integrate_sources([df_csv, df_db, df_api], join_key="student_id")
    assert len(integrated) == 1
    row = integrated.iloc[0]
    assert row["student_id"] == 101
    assert row["name"] == "Alice"
    assert row["gpa"] == 3.8
    assert row["attendance_rate"] == 95.0


# ==========================================
# 5. Data Quality Validation & Error Isolation Tests
# ==========================================

def test_quality_validation_strict_rules():
    """Tests strict validation of Age, GPA, Attendance, and student_id."""
    test_records = pd.DataFrame([
        # 1. Valid Student
        {"student_id": 101, "age": 21, "gpa": 3.5, "attendance_rate": 88.0, "email": "valid@example.com"},
        # 2. Invalid Age (< 16)
        {"student_id": 102, "age": 14, "gpa": 3.2, "attendance_rate": 85.0, "email": "young@example.com"},
        # 3. Invalid Age (> 80)
        {"student_id": 103, "age": 95, "gpa": 3.0, "attendance_rate": 90.0, "email": "old@example.com"},
        # 4. Invalid GPA (> 4.0)
        {"student_id": 104, "age": 22, "gpa": 4.8, "attendance_rate": 80.0, "email": "highgpa@example.com"},
        # 5. Invalid Attendance (> 100)
        {"student_id": 105, "age": 20, "gpa": 3.1, "attendance_rate": 110.0, "email": "overatt@example.com"},
        # 6. Missing student_id
        {"student_id": None, "age": 22, "gpa": 3.0, "attendance_rate": 80.0, "email": "noid@example.com"},
    ])

    valid_df, rejected_df = validate_student_records(test_records)

    # Valid records checks
    assert len(valid_df) == 1
    assert valid_df.iloc[0]["student_id"] == 101
    assert "error_reason" not in valid_df.columns

    # Rejected records checks
    assert len(rejected_df) == 5
    assert "error_reason" in rejected_df.columns

    # Verify specific error messages captured in error_reason
    rejected_reasons = " | ".join(rejected_df["error_reason"].tolist())
    assert "Age out of bounds" in rejected_reasons
    assert "GPA out of bounds" in rejected_reasons
    assert "Attendance rate out of bounds" in rejected_reasons
    assert "Missing student_id" in rejected_reasons

