# Azure Function: Forem Data ETL

Simple Azure Function that fetches the latest DEV (Forem) articles via `https://dev.to/api/articles/latest` and stores each batch in an Azure Blob Storage container. The function keeps track of the newest processed timestamp in `latest_timestamp.json` so repeat executions only ingest fresh content.

## Configuration
- `BLOB_CONN_STR`: Azure Storage connection string used by the function runtime.
- `BACKFILL_MODE` (optional): when set to `true`, writes a rolling backfill blob instead of the incremental timestamp-based workflow.
- Update `local.settings.json` locally or the Function App configuration in Azure with the values above.

## Running Locally
1. Install dependencies: `pip install -r requirements.txt` (or use the provided virtualenv).
2. Start the Azure Functions host: `func start`.
3. Trigger the HTTP function exposed in `function_app.py`; it will call either `fetch_articles.main` or `fetch_articles_backfill.main` depending on your environment settings.

## Deployment Notes
- Ensure the storage container (`forem-data`) already exists in the target account.
- Grant the Function App permission to write blobs and update configuration settings before publishing.
