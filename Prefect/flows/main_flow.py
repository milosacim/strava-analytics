import os
from dotenv import load_dotenv
from prefect import flow

load_dotenv()

from tasks.runner import run_dbt_reporting, run_dbt_staging
from tasks.authorization import get_access_token
from tasks.bigquery_setup import create_bq_dataset, create_external_table
from tasks.activities import get_data_and_upload_activities_to_gcs
from tasks.athlete import get_data_and_upload_athlete_to_gcs

from google.cloud import storage, bigquery

from schemas.activities import ACTIVITIES_SCHEMA
from schemas.athlete import ATHLETE_SCHEMA

project=os.getenv("PROJECT")
bigquery_client = bigquery.Client(project=project)
storage_client = storage.Client(project=project)

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
        "after": "2026-04-30",
        "before": "2026-06-01"
    }

    ingest_flow(params)
    staging_flow(bigquery_client)
    reporting_flow(bigquery_client)

@flow
def reporting_flow(client):
    create_bq_dataset(client, dataset_name="gold_layer")
    run_dbt_reporting()

@flow
def staging_flow(client):
    create_bq_dataset(client, dataset_name="silver_layer")
    run_dbt_staging()

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

    bucket = storage_client.bucket(os.getenv("BUCKET"))

    token = get_access_token(params)

    get_data_and_upload_activities_to_gcs(
        storage_client=storage_client,
        access_token=token,
        before=params["before"], 
        after=params["after"]
    )

    get_data_and_upload_athlete_to_gcs(
        storage_client=storage_client,
        access_token=token,
    )

    create_bq_dataset(bigquery_client, dataset_name="bronze_layer")

    create_external_table(
        bucket=bucket, 
        bigquery_client=bigquery_client,
        schema=ATHLETE_SCHEMA, 
        folder="raw_data/athlete", 
        name="athlete", 
        
    )

    create_external_table(
        bucket=bucket, 
        bigquery_client=bigquery_client,
        schema=ACTIVITIES_SCHEMA, 
        folder="raw_data/activities", 
        name="activities"
        
    )




if __name__ == "__main__":
    main_flow()