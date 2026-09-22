from typing import List, Optional
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("integration")


def integrate_sources(
    sources: List[pd.DataFrame],
    join_key: str = "student_id",
    how: str = "outer",
) -> pd.DataFrame:
    """Integrates multiple data sources (e.g. CSV, Database, REST API) using a common join key.

    Args:
        sources: List of DataFrames to integrate.
        join_key: Primary key column for joining (default: 'student_id').
        how: Type of merge ('outer', 'inner', 'left'). Default is 'outer' to capture all records.

    Returns:
        pd.DataFrame: Merged consolidated dataset.
    """
    valid_dfs = [df.copy() for df in sources if df is not None and not df.empty]
    if not valid_dfs:
        logger.warning("No non-empty DataFrames provided for integration.")
        return pd.DataFrame()

    logger.info(f"Initiating multi-source integration across {len(valid_dfs)} datasets on key '{join_key}'...")

    # Ensure join key is standardized across dataframes
    for df in valid_dfs:
        if join_key in df.columns:
            df[join_key] = pd.to_numeric(df[join_key], errors="coerce").astype("Int64")

    # Start with the first dataframe
    integrated = valid_dfs[0]

    for idx, next_df in enumerate(valid_dfs[1:], start=2):
        if join_key in integrated.columns and join_key in next_df.columns:
            # Overlapping columns other than join_key
            overlapping_cols = [
                c for c in next_df.columns if c in integrated.columns and c != join_key
            ]
            if overlapping_cols:
                # Suffix and coalesce overlapping columns
                integrated = pd.merge(
                    integrated,
                    next_df,
                    on=join_key,
                    how=how,
                    suffixes=("", "_next"),
                )
                for col in overlapping_cols:
                    integrated[col] = integrated[col].combine_first(integrated[f"{col}_next"])
                    integrated.drop(columns=[f"{col}_next"], inplace=True)
            else:
                integrated = pd.merge(integrated, next_df, on=join_key, how=how)
        else:
            logger.warning(f"Join key '{join_key}' missing in source dataset #{idx}; concatenating rows instead.")
            integrated = pd.concat([integrated, next_df], ignore_index=True)

    # Final cleanup: drop duplicate records with exact matching join_key if any duplicate rows exist
    if join_key in integrated.columns:
        # Keep non-null IDs deduplicated, keep null IDs for validation failure tracking
        non_null_mask = integrated[join_key].notna()
        dedup_valid = integrated[non_null_mask].drop_duplicates(subset=[join_key], keep="first")
        null_records = integrated[~non_null_mask]
        integrated = pd.concat([dedup_valid, null_records], ignore_index=True)

    logger.info(f"Multi-source integration complete. Consolidated record count: {len(integrated)}.")
    return integrated
