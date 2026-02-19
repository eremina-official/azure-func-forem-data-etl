import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

# --- Configuration ---
API_URL = "https://dev.to/api/articles/latest"
PER_PAGE = 300
SLEEP_DELAY = 1  # seconds between page fetches to respect API limits
MAX_RETRIES = 3  # retries per API call
CONTAINER_NAME = "forem-data"
LATEST_TIMESTAMP_BLOB = "latest_timestamp.json"  # single blob to track latest timestamp


# Get connection string from Azure Function App settings
def get_blob_client():
    conn_str = os.getenv("BLOB_CONN_STR")
    if not conn_str:
        raise ValueError("BLOB_CONN_STR is not set")

    return BlobServiceClient.from_connection_string(conn_str)


blob_service_client = get_blob_client()
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


def load_latest_timestamp() -> datetime | None:
    """Read the latest timestamp from blob storage."""
    try:
        blob_client = container_client.get_blob_client(LATEST_TIMESTAMP_BLOB)
        data = blob_client.download_blob().readall()
        latest_ts_str = json.loads(data).get("latest_timestamp")
        if latest_ts_str:
            return datetime.fromisoformat(latest_ts_str)
    except Exception:
        logging.info("no timestamp")
        return None
    return None


def save_latest_timestamp(timestamp: datetime | None) -> None:
    if not timestamp:
        return
    blob_client = container_client.get_blob_client(LATEST_TIMESTAMP_BLOB)
    blob_client.upload_blob(
        json.dumps({"latest_timestamp": timestamp.isoformat()}),
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


def collect_new_articles(
    latest_timestamp: datetime | None,
) -> tuple[list[dict[str, Any]], datetime | None, int]:
    new_articles: list[dict[str, Any]] = []
    page = 1
    max_ts_seen = latest_timestamp
    last_page_fetched = 0

    while True:
        articles = fetch_page(page)
        logging.info(f"fetch {page} length:{len(articles)}")
        if not articles:
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
                return new_articles, max_ts_seen, last_page_fetched

            new_articles.append(article)
            if not max_ts_seen or published_at > max_ts_seen:
                max_ts_seen = published_at

        last_page_fetched = page
        page += 1
        time.sleep(SLEEP_DELAY)

    return new_articles, max_ts_seen, last_page_fetched


def save_articles(
    new_articles: list[dict[str, Any]],
    max_ts_seen: datetime | None,
    last_page_fetched: int,
) -> None:
    """Save articles to Azure Blob Storage (flat naming)."""
    file_page = last_page_fetched or 1
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    blob_name = f"{today_str}/page={file_page}_{timestamp}.json"  # flat naming that mimics folders

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(
        json.dumps(new_articles, indent=2),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logging.info(f"Saved {len(new_articles)} new articles to blob {blob_name}")

    save_latest_timestamp(max_ts_seen)


def main() -> None:
    logging.info("Azure Function started.")
    latest_timestamp = load_latest_timestamp()

    logging.info(f"latest timestamp: {latest_timestamp}")
    new_articles, max_ts_seen, last_page_fetched = collect_new_articles(
        latest_timestamp
    )

    if not new_articles:
        logging.info("No new articles found.")
        return

    save_articles(new_articles, max_ts_seen, last_page_fetched)
    logging.info("Azure Function completed successfully.")
