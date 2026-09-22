from typing import Any, Dict, Optional
import json
import socket
import urllib.error
import urllib.parse
import urllib.request
import pandas as pd
from app.utils.logger import setup_logger

logger = setup_logger("api_source")


def get_mock_attendance_data() -> pd.DataFrame:
    """Returns fallback mock attendance records when external REST API is unreachable."""
    return pd.DataFrame([
        {"student_id": 101, "attendance_rate": 92.5},
        {"student_id": 102, "attendance_rate": 88.0},
        {"student_id": 103, "attendance_rate": 72.0},
        {"student_id": 104, "attendance_rate": 96.0},
        {"student_id": 105, "attendance_rate": 115.0}, # Out-of-bounds error (>100)
        {"student_id": 106, "attendance_rate": 84.0},
        {"student_id": 107, "attendance_rate": 90.0},
        {"student_id": 108, "attendance_rate": 65.0},
        {"student_id": 109, "attendance_rate": 82.0},
        {"student_id": 110, "attendance_rate": -10.0}, # Negative attendance error
        {"student_id": 111, "attendance_rate": 80.0},
        {"student_id": 112, "attendance_rate": 91.0},
    ])


def fetch_api_source(
    endpoint_url: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 5,
    use_fallback_on_error: bool = True,
) -> pd.DataFrame:
    """Fetches student attendance records from a REST API endpoint.

    Implements enterprise-grade error handling for:
    - Connection Error (URLError, ConnectionRefused, DNS resolution failure)
    - Timeout Error (Socket timeout, Read timeout)
    - Invalid JSON (Corrupt payload, malformed response)
    - HTTP Status Errors (4xx, 5xx)

    Args:
        endpoint_url: API URL. If None or unreachable, fallback provider is triggered.
        params: Query parameters.
        headers: Request HTTP headers.
        timeout: Socket timeout in seconds.
        use_fallback_on_error: Whether to return fallback data on network failure.

    Returns:
        pd.DataFrame: Student attendance records (student_id, attendance_rate).
    """
    if not endpoint_url:
        logger.info("No remote API endpoint provided. Using fallback attendance records.")
        return get_mock_attendance_data()

    logger.info(f"Connecting to REST API endpoint: {endpoint_url}")
    url = endpoint_url
    if params:
        query_string = urllib.parse.urlencode(params)
        url = f"{url}?{query_string}"

    req = urllib.request.Request(url, headers=headers or {"User-Agent": "StudentPipeline/1.0"})

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status_code = response.getcode()
            logger.info(f"Received HTTP {status_code} from API endpoint.")
            raw_data = response.read().decode("utf-8")

        # Handle Invalid JSON error
        try:
            payload = json.loads(raw_data)
        except (json.JSONDecodeError, ValueError) as json_err:
            logger.error(f"[Invalid JSON Error] Failed to parse API response as valid JSON: {json_err}")
            if use_fallback_on_error:
                logger.info("Activating fallback attendance provider due to invalid JSON payload.")
                return get_mock_attendance_data()
            return pd.DataFrame()

        # Parse records
        if isinstance(payload, dict):
            records = payload.get("data", payload.get("attendance", [payload]))
        elif isinstance(payload, list):
            records = payload
        else:
            records = []

        df = pd.DataFrame(records)
        logger.info(f"API extraction complete: retrieved {len(df)} attendance records.")
        return df

    except (urllib.error.HTTPError) as http_err:
        logger.error(f"[HTTP Error] Server returned status code {http_err.code}: {http_err.reason}")
    except (urllib.error.URLError, ConnectionError) as conn_err:
        logger.error(f"[Connection Error] Unable to establish connection to API server: {conn_err}")
    except (socket.timeout, TimeoutError) as timeout_err:
        logger.error(f"[Timeout Error] Connection timed out after {timeout} seconds: {timeout_err}")
    except Exception as e:
        logger.error(f"[Unexpected Error] API request failed: {e}")

    if use_fallback_on_error:
        logger.info("Activating fallback attendance provider to maintain pipeline continuity.")
        return get_mock_attendance_data()

    return pd.DataFrame()
