import sqlite3
from pathlib import Path
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("database_source")

DEFAULT_STUDENT_QUERY = """
SELECT 
    p.student_id,
    p.major,
    p.enrollment_year,
    g.gpa,
    g.total_credits
FROM academic_profiles p
INNER JOIN academic_grades g ON p.student_id = g.student_id
"""


def load_database_source(
    db_path: str | Path,
    query: str = DEFAULT_STUDENT_QUERY,
) -> pd.DataFrame:
    """Extracts student academic records from an SQLite database using relational SQL queries.

    Args:
        db_path: Path to the SQLite database file.
        query: SQL query joining relational tables on student_id.

    Returns:
        pd.DataFrame: DataFrame containing academic records.

    Raises:
        FileNotFoundError: If the database file does not exist.
        sqlite3.Error: If a database error occurs during query execution.
    """
    path = Path(db_path)
    if not path.exists():
        logger.error(f"Database file not found at: {path}")
        raise FileNotFoundError(f"Database not found: {path}")

    logger.info(f"Connecting to database '{path.name}' and executing relational SQL join on student_id...")
    try:
        with sqlite3.connect(path) as conn:
            df = pd.read_sql_query(query, conn)

        logger.info(f"Database extraction complete: retrieved {len(df)} records across joined tables.")
        return df
    except sqlite3.Error as e:
        logger.error(f"Database query execution failed: {e}")
        raise
