import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    ServerSelectionTimeoutError,
)

from app.utils.logger import setup_logger

# Load environment variables from .env file located at project root
project_root = Path(__file__).resolve().parent.parent.parent
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

logger = setup_logger("mongodb_source")

DEFAULT_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DEFAULT_DATABASE = os.getenv("MONGODB_DATABASE", "student_pipeline")
DEFAULT_COLLECTION = os.getenv("MONGODB_COLLECTION", "student_extra")
DEFAULT_TIMEOUT_MS = int(os.getenv("MONGODB_TIMEOUT_MS", "3000"))


def load_mongodb_source(
    uri: Optional[str] = None,
    database_name: Optional[str] = None,
    collection_name: Optional[str] = None,
    timeout_ms: Optional[int] = None,
    client_instance: Optional[Any] = None,
) -> pd.DataFrame:
    """Extracts additional student profile documents from MongoDB.

    Responsibilities:
    - Reads connection credentials via environment variables or parameters.
    - Connects to MongoDB securely with a strict server selection timeout.
    - Queries raw documents from the specified collection.
    - Flattens nested document hierarchies into a tabular format using pandas.json_normalize.
    - Discards the MongoDB internal '_id' attribute.
    - Closes connection resources safely.
    - Captures and logs connection/configuration failures without crashing the application.

    Note:
    - In accordance with Separation of Concerns, this module performs EXTRACTION ONLY.
    - Data cleaning, array serialization, and validation are delegated to their respective layers.

    Args:
        uri: MongoDB connection URI string (e.g. mongodb://localhost:27017).
        database_name: Target database name.
        collection_name: Target collection name.
        timeout_ms: Timeout in milliseconds for server discovery and selection.
        client_instance: Optional pre-configured client (useful for unit testing with mongomock).

    Returns:
        pd.DataFrame: Tabular DataFrame representing extracted MongoDB documents,
                      or an empty DataFrame if extraction failed or collection is empty.
    """
    conn_uri = uri or DEFAULT_URI
    db_name = database_name or DEFAULT_DATABASE
    coll_name = collection_name or DEFAULT_COLLECTION
    timeout = timeout_ms if timeout_ms is not None else DEFAULT_TIMEOUT_MS

    logger.info(
        f"Initiating MongoDB extraction: DB='{db_name}', Collection='{coll_name}', Timeout={timeout}ms"
    )

    client: Optional[MongoClient] = client_instance
    own_client = False

    try:
        if client is None:
            own_client = True
            client = MongoClient(
                conn_uri,
                serverSelectionTimeoutMS=timeout,
                connectTimeoutMS=timeout,
            )
            # Force server discovery check
            client.admin.command("ping")
            logger.info("Successfully connected and authenticated with MongoDB server.")

        db = client[db_name]
        collection = db[coll_name]

        # Retrieve documents excluding internal '_id'
        cursor = collection.find({}, {"_id": 0})
        documents: List[Dict[str, Any]] = list(cursor)

        if not documents:
            logger.warning(
                f"MongoDB collection '{coll_name}' is empty. Returning empty DataFrame."
            )
            return pd.DataFrame()

        logger.info(
            f"Successfully retrieved {len(documents)} documents from MongoDB collection '{coll_name}'."
        )

        # Normalize nested documents (e.g., contact.phone, address.city, guardian.name)
        df = pd.json_normalize(documents)
        logger.info(
            f"Document flattening complete: structured {len(df)} rows across {len(df.columns)} columns."
        )
        return df

    except ServerSelectionTimeoutError as timeout_err:
        logger.error(
            f"[MongoDB ServerSelectionTimeoutError] Could not locate or connect to MongoDB server within {timeout}ms: {timeout_err}"
        )
        return pd.DataFrame()

    except ConnectionFailure as conn_err:
        logger.error(
            f"[MongoDB ConnectionFailure] Network or authentication failure connecting to MongoDB: {conn_err}"
        )
        return pd.DataFrame()

    except ConfigurationError as config_err:
        logger.error(
            f"[MongoDB ConfigurationError] Invalid connection URI or MongoDB client configuration: {config_err}"
        )
        return pd.DataFrame()

    except Exception as e:
        logger.error(
            f"[MongoDB Extraction Error] An unexpected error occurred while extracting documents: {e}"
        )
        return pd.DataFrame()

    finally:
        # Guarantee resource release
        if own_client and client is not None:
            try:
                client.close()
                logger.info("MongoDB client connection closed cleanly.")
            except Exception as close_err:
                logger.warning(f"Error while closing MongoDB client connection: {close_err}")
