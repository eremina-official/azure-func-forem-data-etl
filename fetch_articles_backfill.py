import logging
import json
import time
from datetime import datetime, timezone
from typing import Any
import os
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

# --- Configuration ---
API_URL = "https://dev.to/api/articles/latest"
PER_PAGE = 300
SLEEP_DELAY = 1
MAX_RETRIES = 3  # retries per API call
CONTAINER_NAME = "forem-data"
BACKFILL_PAGE_BLOB = "backfill_page.json"  # single blob to track latest timestamp
MAX_FILE_SIZE_MB = 128  # flush if exceeds
backfill_timestamp = os.getenv("BACKFILL_MODE", "")
MAX_PAGES_PER_RUN = 10  # fetch at most 10 pages per timer trigger


# Get connection string from Azure Function App settings
def get_blob_client():
    conn_str = os.getenv("BLOB_CONN_STR")
    if not conn_str:
        raise ValueError("BLOB_CONN_STR is not set")

    return BlobServiceClient.from_connection_string(conn_str)


blob_service_client = get_blob_client()
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


def load_backfill_page() -> int:
    """Load last processed page from blob, default to 1."""
    try:
        blob_client = container_client.get_blob_client(BACKFILL_PAGE_BLOB)
        data = blob_client.download_blob().readall()
        page = json.loads(data).get("page", 1)
        return max(1, page)
    except Exception:
        return 1


def save_backfill_page(page: int | None) -> None:
    if not page:
        return
    blob_client = container_client.get_blob_client(BACKFILL_PAGE_BLOB)
    blob_client.upload_blob(
        json.dumps({"page": page}),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )


def fetch_page(page: int) -> list[dict[str, Any]]:
    """Fetch one page of articles with retry logic."""
    params = {"per_page": PER_PAGE, "page": page}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            logging.warning(f"API request failed (attempt {attempt}): {exc}")
            if attempt == MAX_RETRIES:
                logging.error("Max retries reached for page %s. Aborting fetch.", page)
            else:
                time.sleep(2**attempt)
        except json.JSONDecodeError as exc:
            logging.error(f"Failed to parse JSON response: {exc}")
            break
    return []


def collect_new_articles() -> None:
    start_page = load_backfill_page()
    page = start_page
    pages_fetched = 0
    buffer: list[dict[str, Any]] = []
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_name = f"{today_str}/backfill_{timestamp}.json"

    while pages_fetched < MAX_PAGES_PER_RUN:
        articles = fetch_page(page)
        logging.info(f"fetch {page} length:{len(articles)}")
        if not articles:
            logging.info("No new articles found.")
            break

        buffer.extend(articles)

        page += 1
        pages_fetched += 1
        time.sleep(SLEEP_DELAY)

    # save buffer and next page for next run
    if buffer:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            json.dumps(buffer, indent=2),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )
        logging.info(f"Final flush {len(buffer)} articles to blob {blob_name}")

    save_backfill_page(page)
    logging.info(f"Backfill paused, next run will start from page {page}")


def main_fetch_backfill() -> None:
    logging.info("Azure Function started in backfill mode.")

    collect_new_articles()

    logging.info("Azure Function completed successfully.")
