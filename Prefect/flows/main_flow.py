import os
from dotenv import load_dotenv
from prefect import flow

load_dotenv()

from tasks.authorization import get_access_token
from tasks.bigquery_setup import create_bq_dataset, create_external_table
from tasks.activities import get_data_and_upload_to_gcs

from google.cloud import storage, bigquery

@flow
def main_flow():
    """
    Prefect flow. Entry point for the Strava analytics pipeline.

    Builds the Strava OAuth + date-range parameter dict from environment
    variables and hands off to ingest_flow.
    """

    params = {
        "client_id": os.getenv('STRAVA_CLIENT_ID'),
        "client_secret": os.getenv('STRAVA_CLIENT_SECRET'),
        "refresh_token": os.getenv('STRAVA_REFRESH_TOKEN'),
        "grant_type": 'refresh_token',
        "after": "2026-05-01",
        "before": "2026-05-31"
    }

    ingest_flow(params)

@flow
def ingest_flow(params: dict):
    """
    Prefect flow. Runs the raw ingestion layer end-to-end:
    fetches activities from Strava and uploads them to GCS, then
    ensures the BigQuery bronze dataset and external table exist
    on top of the GCS raw files.

    Args:
        params: Dict containing Strava OAuth credentials (client_id, client_secret,
                refresh_token, grant_type) and date range keys 'before' and 'after'
                in 'YYYY-MM-DD' format.
    """

    project=os.getenv("PROJECT")
    storage_client = storage.Client(project=project)
    bigquery_client = bigquery.Client(project=project)

    bucket = storage_client.bucket(os.getenv("BUCKET"))

    get_data_and_upload_to_gcs(
        storage_client=storage_client,
        access_token=get_access_token(params), 
        before=params["before"], 
        after=params["after"]
    )
    create_bq_dataset(bigquery_client, dataset_name="bronze_layer")
    create_external_table(bucket, bigquery_client)


if __name__ == "__main__":
    main_flow()