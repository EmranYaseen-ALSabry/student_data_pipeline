from typing import Any
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("cleaner")

# City normalization mapping for common variations
CITY_NORMALIZATION_MAP = {
    "cairo": "Cairo",
    "el cairo": "Cairo",
    "al qahirah": "Cairo",
    "alex": "Alexandria",
    "alexandria": "Alexandria",
    "giza": "Giza",
    "al giza": "Giza",
    "riyadh": "Riyadh",
    "ar riyad": "Riyadh",
    "jeddah": "Jeddah",
    "jiddah": "Jeddah",
    "dubai": "Dubai",
    "new york": "New York",
}


def clean_student_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans raw student data:

    1. Normalizes column names (snake_case, stripped).
    2. Strips surrounding and repeated whitespace from string values without
       converting structured values such as MongoDB arrays into strings.
    3. Standardizes city casing and common spelling variants.
    4. Normalizes student names (title case, single spaces).
    5. Deduplicates identical records.

    Args:
        df: Input DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to cleaner.")
        return df

    logger.info(f"Initiating data cleaning on {len(df)} incoming records...")
    cleaned = df.copy()

    # 1. Standardize column names to lower_snake_case
    cleaned.columns = (
        cleaned.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace("-", "_")
    )

    # 2. Strip whitespace from string/object columns
    str_cols = cleaned.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        cleaned[col] = cleaned[col].map(_clean_text_value)

    # 3. Standardize City Column
    for city_column in ("city", "address.city"):
        if city_column in cleaned.columns:
            cleaned[city_column] = cleaned[city_column].apply(_normalize_city)

    # 4. Standardize Name Column
    if "name" in cleaned.columns:
        cleaned["name"] = cleaned["name"].apply(_normalize_name)

    # 5. Remove exact duplicate rows
    initial_count = len(cleaned)
    row_signatures = cleaned.apply(
        lambda row: tuple(repr(value) for value in row),
        axis=1,
    )
    cleaned = cleaned.loc[~row_signatures.duplicated()].copy()
    duplicates_removed = initial_count - len(cleaned)
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate records.")

    logger.info(f"Cleaning complete. Output record count: {len(cleaned)}.")
    return cleaned


def _clean_text_value(value: Any) -> Any:
    """Normalize strings while preserving nulls and non-string values."""
    if not isinstance(value, str):
        return value

    normalized = " ".join(value.split())
    if normalized.lower() in {"", "nan", "none", "<na>"}:
        return pd.NA
    return normalized


def _normalize_city(val: Any) -> Any:
    """Normalizes city names by trimming, converting to title case, and resolving aliases."""
    if pd.isna(val):
        return pd.NA
    s = str(val).strip()
    if not s:
        return pd.NA
    lookup = s.lower()
    return CITY_NORMALIZATION_MAP.get(lookup, s.title())


def _normalize_name(val: Any) -> Any:
    """Normalizes human names with consistent title casing and single spaces."""
    if pd.isna(val):
        return pd.NA
    s = str(val).strip()
    if not s:
        return pd.NA
    # Collapse multiple spaces and apply title case
    words = [w.capitalize() for w in s.split()]
    return " ".join(words)
