from typing import List
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("integration")


def integrate_sources(
    sources: List[pd.DataFrame],
    join_key: str = "student_id",
    how: str = "outer",
) -> pd.DataFrame:
    """Integrates heterogeneous data sources (CSV, SQLite, REST API, MongoDB) into a unified dataset.

    Key Engineering Principles:
    - Multi-Source Outer Join: Ensures no student is lost even if present in only one source.
    - Key Hygiene: Requires a valid, unique join key in every non-empty source.
    - Overlapping Column Coalescing: Harmonizes overlapping attributes using `combine_first` to prevent duplicate '_x' / '_y' clutter.

    Args:
        sources: List of DataFrames to integrate (e.g. [df_csv, df_db, df_api, df_mongo]).
        join_key: Primary key column for joining (default: 'student_id').
        how: Merge strategy (default: 'outer' to capture all candidate records).

    Returns:
        pd.DataFrame: Consolidated DataFrame containing merged attributes across all sources.
    """
    valid_dfs = [df.copy() for df in sources if df is not None and not df.empty]
    if not valid_dfs:
        logger.warning("No non-empty DataFrames provided for integration.")
        return pd.DataFrame()

    for idx, df in enumerate(valid_dfs, start=1):
        if join_key not in df.columns:
            raise ValueError(
                f"Source #{idx} is missing required join key '{join_key}'."
            )

        numeric_key = pd.to_numeric(df[join_key], errors="coerce")
        valid_key = numeric_key.map(
            lambda value: pd.notna(value)
            and float(value).is_integer()
            and value > 0
        )
        if not valid_key.all():
            raise ValueError(
                f"Source #{idx} contains missing or non-integer '{join_key}' values."
            )
        if numeric_key.duplicated(keep=False).any():
            raise ValueError(
                f"Source #{idx} contains duplicate '{join_key}' values."
            )
        df[join_key] = numeric_key.astype("Int64")

    logger.info(
        f"Initiating multi-source integration across {len(valid_dfs)} datasets on primary key '{join_key}' using '{how}' join..."
    )

    # Sequential outer merge
    integrated = valid_dfs[0]
    logger.info(f"Source #1 initialized with {len(integrated)} records and columns: {list(integrated.columns)}")

    for idx, next_df in enumerate(valid_dfs[1:], start=2):
        incoming_cols = list(next_df.columns)
        logger.info(f"Merging Source #{idx} ({len(next_df)} records, columns: {incoming_cols})...")

        overlapping_cols = [
            c for c in next_df.columns if c in integrated.columns and c != join_key
        ]

        if overlapping_cols:
            integrated = pd.merge(
                integrated,
                next_df,
                on=join_key,
                how=how,
                suffixes=("", "_incoming"),
            )
            for col in overlapping_cols:
                integrated[col] = integrated[col].combine_first(
                    integrated[f"{col}_incoming"]
                )
                integrated.drop(columns=[f"{col}_incoming"], inplace=True)
        else:
            integrated = pd.merge(integrated, next_df, on=join_key, how=how)

    logger.info(
        f"Multi-source integration complete: Produced unified dataset of {len(integrated)} records and {len(integrated.columns)} attributes."
    )
    return integrated
