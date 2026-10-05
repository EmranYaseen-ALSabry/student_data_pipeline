from typing import List, Optional
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
    - Key Hygiene: Sanitizes and standardizes the join_key to Nullable Integer (Int64) across all sources.
    - Pre-merge Deduplication: Deduplicates non-null keys within each source before merging to prevent Cartesian product explosions.
    - Overlapping Column Coalescing: Harmonizes overlapping attributes using `combine_first` to prevent duplicate '_x' / '_y' clutter.
    - Error Isolation Preservation: Preserves records missing the primary key (`join_key is NaN`) so downstream Quality Gates can isolate them.

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

    logger.info(
        f"Initiating multi-source integration across {len(valid_dfs)} datasets on primary key '{join_key}' using '{how}' join..."
    )

    # 1. Pre-process and sanitize join key for each source
    prepared_dfs = []
    for idx, df in enumerate(valid_dfs, start=1):
        if join_key in df.columns:
            # Standardize key type to Nullable Integer (Int64)
            df[join_key] = pd.to_numeric(df[join_key], errors="coerce").astype("Int64")

            # Deduplicate non-null keys within the same source to prevent Cartesian product during merge
            non_null_mask = df[join_key].notna()
            non_null_dedup = df[non_null_mask].drop_duplicates(subset=[join_key], keep="first")
            null_entries = df[~non_null_mask]
            df = pd.concat([non_null_dedup, null_entries], ignore_index=True)

        prepared_dfs.append(df)

    # 2. Sequential Outer Merge
    integrated = prepared_dfs[0]
    logger.info(f"Source #1 initialized with {len(integrated)} records and columns: {list(integrated.columns)}")

    for idx, next_df in enumerate(prepared_dfs[1:], start=2):
        incoming_cols = list(next_df.columns)
        logger.info(f"Merging Source #{idx} ({len(next_df)} records, columns: {incoming_cols})...")

        if join_key in integrated.columns and join_key in next_df.columns:
            # Identify overlapping columns other than the primary join_key
            overlapping_cols = [
                c for c in next_df.columns if c in integrated.columns and c != join_key
            ]

            if overlapping_cols:
                # Merge with suffixes and coalesce overlapping fields
                integrated = pd.merge(
                    integrated,
                    next_df,
                    on=join_key,
                    how=how,
                    suffixes=("", "_incoming"),
                )
                for col in overlapping_cols:
                    # Fill nulls from the incoming source
                    integrated[col] = integrated[col].combine_first(integrated[f"{col}_incoming"])
                    integrated.drop(columns=[f"{col}_incoming"], inplace=True)
            else:
                integrated = pd.merge(integrated, next_df, on=join_key, how=how)
        else:
            logger.warning(
                f"Join key '{join_key}' missing in Source #{idx}; concatenating records instead."
            )
            integrated = pd.concat([integrated, next_df], ignore_index=True)

    # 3. Final consolidation
    if join_key in integrated.columns:
        # Keep non-null IDs unique, preserve null IDs for the validation layer
        non_null_mask = integrated[join_key].notna()
        dedup_valid = integrated[non_null_mask].drop_duplicates(subset=[join_key], keep="first")
        null_records = integrated[~non_null_mask]
        integrated = pd.concat([dedup_valid, null_records], ignore_index=True)

    logger.info(
        f"Multi-source integration complete: Produced unified dataset of {len(integrated)} records and {len(integrated.columns)} attributes."
    )
    return integrated
