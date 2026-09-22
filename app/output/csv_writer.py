from pathlib import Path
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("csv_writer")


def write_csv_output(df: pd.DataFrame, output_path: str | Path) -> None:
    """Exports a DataFrame to a CSV file with UTF-8 encoding and directory auto-creation.

    Args:
        df: DataFrame to persist.
        output_path: Destination path for CSV output.
    """
    path = Path(output_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if df is None:
            df = pd.DataFrame()
        logger.info(f"Persisting {len(df)} records to target file: {path.name}")
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info(f"Successfully saved file: {path}")
    except Exception as e:
        logger.error(f"Failed to export data to {path}: {e}")
        raise
