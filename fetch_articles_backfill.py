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
SLEEP_DELAY = 2
MAX_RETRIES = 3  # retries per API call
CONTAINER_NAME = "forem-data"
BACKFILL_PAGE_BLOB = "backfill_page.json"  # single blob to track latest timestamp
MAX_FILE_SIZE_MB = 128  # flush if exceeds
backfill_timestamp = os.getenv("BACKFILL_MODE", "")


# Get connection string from Azure Function App settings
def get_blob_client():
    conn_str = os.getenv("BLOB_CONN_STR")
    if not conn_str:
        raise ValueError("BLOB_CONN_STR is not set")

    return BlobServiceClient.from_connection_string(conn_str)


blob_service_client = get_blob_client()
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


def save_backfill_page(page: int | None) -> None:
    if not page:
        return
    blob_client = container_client.get_blob_client(BACKFILL_PAGE_BLOB)
    blob_client.upload_blob(
        json.dumps({"page": page}),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )


def check_file_size_and_flush(
    blob_name: str, buffer: list[dict[str, Any]]
) -> tuple[str, list[dict[str, Any]]]:
    """Flush buffer to blob if it exceeds MAX_FILE_SIZE_MB."""
    data_bytes = json.dumps(buffer, indent=2).encode("utf-8")
    size_mb = len(data_bytes) / (1024 * 1024)
    if size_mb >= MAX_FILE_SIZE_MB:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        new_blob_name = f"{blob_name}_flushed_{timestamp}.json"
        blob_client = container_client.get_blob_client(new_blob_name)
        blob_client.upload_blob(
            data_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )
        logging.info(
            f"Flushed {len(buffer)} articles to blob {new_blob_name} ({size_mb:.2f} MB)"
        )
        return new_blob_name, []
    return blob_name, buffer


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


def collect_new_articles(latest_timestamp: datetime | None) -> None:
    page = 1
    max_ts_seen = latest_timestamp
    buffer: list[dict[str, Any]] = []
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_name = f"{today_str}/backfill_{timestamp}.json"

    while True:
        articles = fetch_page(page)
        logging.info(f"fetch {page} length:{len(articles)}")
        if not articles:
            logging.info("No new articles found.")
            break

        for article in articles:
            try:
                published_at = datetime.fromisoformat(
                    article["published_at"].replace("Z", "+00:00")
                )
            except (KeyError, ValueError, TypeError) as exc:
                logging.warning("Skipping article due to parsing error: %s", exc)
                continue

            if latest_timestamp and published_at <= latest_timestamp:
                logging.info("Reached already processed articles. Stopping.")
                save_backfill_page(page)
                # exit entire backfill, not just the inner loop
                if buffer:
                    # flush remaining buffer
                    blob_client = container_client.get_blob_client(blob_name)
                    blob_client.upload_blob(
                        json.dumps(buffer, indent=2),
                        overwrite=True,
                        content_settings=ContentSettings(
                            content_type="application/json"
                        ),
                    )
                    logging.info(
                        f"Final flush {len(buffer)} articles to blob {blob_name}"
                    )
                return

            # Append to buffer and check flush
            buffer.append(article)
            blob_name, buffer = check_file_size_and_flush(blob_name, buffer)

            if not max_ts_seen or published_at > max_ts_seen:
                max_ts_seen = published_at

        page += 1
        time.sleep(SLEEP_DELAY)

    # Flush remaining buffer if backfill
    if buffer:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            json.dumps(buffer, indent=2),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )
        logging.info(f"Final flush {len(buffer)} articles to blob {blob_name}")


def main_fetch_backfill() -> None:
    logging.info("Azure Function started in backfill mode.")
    latest_timestamp = None
    if backfill_timestamp:
        try:
            timestamp = "2026-02-13T13:45:22+00:00"
            # timestamp = '2025-01-14T13:52:00+00:00'
            latest_timestamp = datetime.fromisoformat(timestamp)
        except ValueError:
            logging.warning("Invalid BACKFILL_MODE timestamp")
            return

    logging.info(f"latest timestamp: {latest_timestamp}")
    collect_new_articles(latest_timestamp)

    logging.info("Azure Function completed successfully.")
