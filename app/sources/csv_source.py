from pathlib import Path
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("csv_source")


def load_csv_source(file_path: str | Path) -> pd.DataFrame:
    """Extracts student demographic records from a CSV file.

    Args:
        file_path: Absolute or relative path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing loaded demographic records.

    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        pd.errors.EmptyDataError: If the CSV file is completely empty.
    """
    path = Path(file_path)
    if not path.exists():
        logger.error(f"CSV file not found at: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    logger.info(f"Extracting demographic data from CSV: {path.name}")
    try:
        df = pd.read_csv(path)
        logger.info(f"CSV extraction complete: loaded {len(df)} rows and {len(df.columns)} columns.")
        return df
    except pd.errors.EmptyDataError:
        logger.warning(f"CSV file at {path} is empty.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error while reading CSV {path}: {e}")
        raise
