from pathlib import Path
import pytest
import pandas as pd
import mongomock

import main as pipeline_main

from app.sources.csv_source import load_csv_source
from app.sources.database_source import load_database_source
from app.sources import api_source
from app.sources.api_source import fetch_api_source, get_mock_attendance_data
from app.sources.mongodb_source import load_mongodb_source
from app.transformation.cleaner import clean_student_data, _normalize_city
from app.transformation.transformer import (
    transform_student_data,
    _compute_performance_level,
    _compute_attendance_status,
    _serialize_skills,
    _serialize_courses,
    _serialize_projects,
)
from app.transformation.integration import integrate_sources
from app.validation.quality import validate_source_records, validate_student_records


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
    assert "student_id" in df.columns
    assert "major" in df.columns
    assert "gpa" in df.columns


def test_api_extraction_fallback_and_error_handling():
    """Verifies API source error handling for unreachable endpoints and invalid JSON."""
    df_fallback = fetch_api_source("http://127.0.0.1:9999/unreachable_api", timeout=1)
    assert isinstance(df_fallback, pd.DataFrame)
    assert not df_fallback.empty
    assert "attendance_rate" in df_fallback.columns


def test_api_extraction_parses_successful_json(monkeypatch):
    """Verifies a successful API response is parsed without using fallback data."""
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def getcode(self):
            return 200

        def read(self):
            return b'{"data": [{"student_id": 101, "attendance_rate": 92.5}]}'

    monkeypatch.setattr(
        api_source.urllib.request,
        "urlopen",
        lambda request, timeout: FakeResponse(),
    )
    result = fetch_api_source("https://example.test/attendance")
    assert result.to_dict("records") == [
        {"student_id": 101, "attendance_rate": 92.5}
    ]


def test_mongodb_extraction_with_mock():
    """Verifies MongoDB extraction flattens nested documents and excludes _id using mongomock."""
    mock_client = mongomock.MongoClient()
    mock_db = mock_client["student_pipeline"]
    mock_coll = mock_db["student_extra"]

    mock_coll.insert_one({
        "student_id": 1001,
        "contact": {"phone": "+967771234567"},
        "address": {"city": "Sanaa", "country": "Yemen"},
        "skills": ["Python", "SQL", "MongoDB"],
        "projects": [{"name": "ETL Pipeline", "technologies": ["Python", "Pandas"]}],
    })

    df = load_mongodb_source(client_instance=mock_client)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "_id" not in df.columns
    assert "student_id" in df.columns
    assert "contact.phone" in df.columns
    assert "address.city" in df.columns
    assert df.iloc[0]["student_id"] == 1001
    assert df.iloc[0]["address.city"] == "Sanaa"


def test_mongodb_extraction_empty_collection():
    """Verifies MongoDB extraction returns empty DataFrame when collection is empty."""
    mock_client = mongomock.MongoClient()
    df = load_mongodb_source(client_instance=mock_client)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_mongodb_extraction_timeout_resilience():
    """Verifies MongoDB extraction handles unreachable server gracefully without crashing."""
    df_offline = load_mongodb_source(uri="mongodb://127.0.0.1:27099", timeout_ms=300)
    assert isinstance(df_offline, pd.DataFrame)
    assert df_offline.empty


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
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["city"] == "Cairo"
    assert cleaned.iloc[0]["name"] == "Ahmed Ali"


def test_cleaner_normalizes_each_source_without_stringifying_arrays():
    """Cleaning standardizes values while preserving MongoDB list fields."""
    raw = pd.DataFrame({
        " Student ID ": [101],
        " Address.City ": ["  sanaa   city "],
        "skills": [["Python", "SQL"]],
    })
    cleaned = clean_student_data(raw)
    assert cleaned.loc[0, "student_id"] == 101
    assert cleaned.loc[0, "address.city"] == "Sanaa City"
    assert cleaned.loc[0, "skills"] == ["Python", "SQL"]


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


def test_mongodb_array_serialization():
    """Verifies serialization of NoSQL arrays (skills, courses, projects) for CSV export."""
    # Skills serialization
    assert _serialize_skills(["Python", "SQL", "MongoDB"]) == "Python | SQL | MongoDB"
    assert _serialize_skills([]) == "N/A"
    assert _serialize_skills(None) == "N/A"

    # Courses serialization
    courses = [{"name": "Python", "grade": 92}, {"name": "Database", "grade": 88}]
    assert _serialize_courses(courses) == "Python (92%) | Database (88%)"
    assert _serialize_courses([]) == "N/A"

    # Projects serialization
    projects = [{"name": "Pipeline", "technologies": ["Python", "Pandas"]}]
    assert _serialize_projects(projects) == "Pipeline [Python, Pandas]"
    assert _serialize_projects([]) == "N/A"


def test_transformation_pipeline():
    """Tests DataFrame type casting, derived columns, and array serialization."""
    df = pd.DataFrame({
        "student_id": ["101"],
        "age": ["21"],
        "gpa": ["3.85"],
        "attendance_rate": ["92.0"],
        "email": ["USER@EXAMPLE.COM "],
        "skills": [["Python", "SQL"]],
    })
    transformed = transform_student_data(df)
    assert transformed.iloc[0]["email"] == "user@example.com"
    assert transformed.iloc[0]["performance_level"] == "Excellent"
    assert transformed.iloc[0]["attendance_status"] == "Regular"
    assert transformed.iloc[0]["skills"] == "Python | SQL"


# ==========================================
# 4. Multi-Source Integration Tests (4 Sources)
# ==========================================

def test_integrate_four_sources():
    """Tests outer merge of CSV, DB, API, and MongoDB datasets using student_id."""
    df_csv = pd.DataFrame([{"student_id": 101, "name": "Alice", "city": "Cairo"}])
    df_db = pd.DataFrame([{"student_id": 101, "gpa": 3.8, "major": "CS"}])
    df_api = pd.DataFrame([{"student_id": 101, "attendance_rate": 95.0}])
    df_mongo = pd.DataFrame([{
        "student_id": 101,
        "contact.phone": "+96777112233",
        "skills": ["Python", "SQL"]
    }])

    integrated = integrate_sources([df_csv, df_db, df_api, df_mongo], join_key="student_id")
    assert len(integrated) == 1
    row = integrated.iloc[0]
    assert row["student_id"] == 101
    assert row["name"] == "Alice"
    assert row["gpa"] == 3.8
    assert row["attendance_rate"] == 95.0
    assert row["contact.phone"] == "+96777112233"


def test_integration_rejects_sources_without_unique_valid_keys():
    """Integration fails explicitly instead of silently concatenating bad sources."""
    with pytest.raises(ValueError, match="missing required join key"):
        integrate_sources([pd.DataFrame([{"name": "No key"}])])

    duplicate_ids = pd.DataFrame({"student_id": [101, 101]})
    with pytest.raises(ValueError, match="duplicate 'student_id'"):
        integrate_sources([duplicate_ids])


# ==========================================
# 5. Data Quality Validation & Error Isolation Tests
# ==========================================

def test_source_validation_rejects_bad_keys_and_present_invalid_values():
    """Invalid rows are isolated per source before integration."""
    source = pd.DataFrame([
        {"student_id": 101, "age": 21},
        {"student_id": 102, "age": 22},
        {"student_id": 102, "age": 23},
        {"student_id": 103, "age": 15},
        {"student_id": None, "age": 20},
        {"student_id": 104, "gpa": "not-a-number"},
        {"student_id": 105, "score": 101},
    ])
    valid, rejected = validate_source_records(source, "CSV")
    assert valid["student_id"].tolist() == [101]
    assert len(rejected) == 6
    assert rejected["source_name"].eq("CSV").all()
    assert rejected["rejection_stage"].eq("source_validation").all()
    reasons = " | ".join(rejected["error_reason"].tolist())
    assert "Duplicate student_id (102)" in reasons
    assert "Age out of bounds [16-80]" in reasons
    assert "Missing student_id" in reasons
    assert "Non-numeric GPA value" in reasons
    assert "Score out of bounds [0-100]" in reasons


def test_quality_validation_strict_rules():
    """Tests strict validation of Age, GPA, Attendance, and student_id."""
    test_records = pd.DataFrame([
        # 1. Valid Student (with optional MongoDB fields)
        {"student_id": 101, "age": 21, "gpa": 3.5, "attendance_rate": 88.0, "email": "valid@example.com", "contact.phone": "+96777111111", "skills": "Python"},
        # 2. Invalid Age (< 16)
        {"student_id": 102, "age": 14, "gpa": 3.2, "attendance_rate": 85.0, "email": "young@example.com"},
        # 3. Invalid Age (> 80)
        {"student_id": 103, "age": 95, "gpa": 3.0, "attendance_rate": 90.0, "email": "old@example.com"},
        # 4. Invalid GPA (> 4.0)
        {"student_id": 104, "age": 22, "gpa": 4.8, "attendance_rate": 80.0, "email": "highgpa@example.com"},
        # 5. Invalid Attendance (> 100)
        {"student_id": 105, "age": 20, "gpa": 3.1, "attendance_rate": 110.0, "email": "overatt@example.com"},
        # 6. Missing student_id (e.g. from malformed MongoDB or CSV)
        {"student_id": None, "age": 22, "gpa": 3.0, "attendance_rate": 80.0, "email": "noid@example.com"},
        # 7. Valid Student with missing optional MongoDB fields (Must PASS!)
        {"student_id": 106, "age": 23, "gpa": 3.7, "attendance_rate": 90.0, "email": "nophone@example.com", "contact.phone": None, "skills": "N/A"},
    ])

    valid_df, rejected_df = validate_student_records(test_records)

    # Valid records checks: Students 101 and 106 have all required fields.
    assert len(valid_df) == 2
    valid_ids = valid_df["student_id"].tolist()
    assert 101 in valid_ids
    assert 106 in valid_ids
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


def test_final_validation_rejects_missing_required_fields():
    """Required analytics fields are rejected rather than statistically imputed."""
    records = pd.DataFrame([
        {"student_id": 201, "age": 20, "gpa": 3.2, "attendance_rate": 90},
        {"student_id": 202, "age": 21, "gpa": None, "attendance_rate": 85},
        {"student_id": 203, "age": 22, "gpa": 3.0, "attendance_rate": None},
    ])
    valid, rejected = validate_student_records(records)
    assert valid["student_id"].tolist() == [201]
    assert len(rejected) == 2
    reasons = " | ".join(rejected["error_reason"].tolist())
    assert "Missing required field: gpa" in reasons
    assert "Missing required field: attendance_rate" in reasons


def test_pipeline_runs_all_layers_and_writes_auditable_outputs(tmp_path, monkeypatch):
    """Exercises cleaning, source gates, integration, final gates, and CSV load."""
    monkeypatch.setattr(pipeline_main, "project_root", tmp_path)
    monkeypatch.setattr(
        pipeline_main,
        "load_csv_source",
        lambda path: pd.DataFrame([
            {
                "student_id": 101,
                "name": " alice   smith ",
                "age": 21,
                "city": " cairo ",
                "email": "ALICE@example.com",
            },
            {
                "student_id": 102,
                "name": "Bob Jones",
                "age": 22,
                "city": "Giza",
                "email": "bob@example.com",
            },
        ]),
    )
    monkeypatch.setattr(
        pipeline_main,
        "load_database_source",
        lambda path: pd.DataFrame([
            {
                "student_id": 101,
                "major": "Computer Science",
                "enrollment_year": 2022,
                "gpa": 3.5,
                "total_credits": 90,
            },
            {
                "student_id": 102,
                "major": "Mathematics",
                "enrollment_year": 2021,
                "gpa": 4.8,
                "total_credits": 80,
            },
        ]),
    )
    monkeypatch.setattr(
        pipeline_main,
        "fetch_api_source",
        lambda endpoint_url: pd.DataFrame([
            {"student_id": 101, "attendance_rate": 90.0},
            {"student_id": 102, "attendance_rate": 88.0},
        ]),
    )
    monkeypatch.setattr(
        pipeline_main,
        "load_mongodb_source",
        lambda: pd.json_normalize([
            {
                "student_id": 101,
                "contact": {"phone": "+967770000001"},
                "address": {"city": " CAIRO "},
                "skills": ["Python", "SQL"],
            },
            {
                "student_id": 102,
                "address": {"city": "GIZA"},
                "skills": ["Math"],
            },
        ]),
    )

    pipeline_main.run_pipeline()

    accepted = pd.read_csv(tmp_path / "data" / "processed" / "final_dataset.csv")
    rejected = pd.read_csv(tmp_path / "data" / "rejected" / "rejected_records.csv")
    assert accepted["student_id"].tolist() == [101]
    assert accepted.loc[0, "address.city"] == "Cairo"
    assert accepted.loc[0, "skills"] == "Python | SQL"
    assert {"source_name", "rejection_stage", "error_reason"}.issubset(
        rejected.columns
    )
    assert set(rejected["rejection_stage"]) == {
        "source_validation",
        "final_validation",
    }
