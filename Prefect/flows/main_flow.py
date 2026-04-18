import os
from dotenv import load_dotenv
from prefect import flow
from tasks.load_to_bigquery import create_bq_dataset, load_to_bronze_table
from tasks.activities import get_data_from_strava


@flow
def main_flow():
    """
    Prefect flow. Entry point for the Strava analytics pipeline.

    Args:
        request_params: Dict containing Strava OAuth credentials and date range.
                        Keys: client_id, client_secret, refresh_token, grant_type,
                        before, after (dates in 'YYYY-MM-DD' format).
    """
    
    _token_params = {
        "client_id": os.getenv('STRAVA_CLIENT_ID'),
        "client_secret": os.getenv('STRAVA_CLIENT_SECRET'),
        "refresh_token": os.getenv('STRAVA_REFRESH_TOKEN'),
        "grant_type": 'refresh_token',
        "after": "2026-04-01",
        "before": "2026-04-30"
    }
    ingest_flow(_token_params)


@flow
def ingest_flow(request_params: dict):
    create_bq_dataset(dataset_name="bronze_layer")
    get_data_from_strava(request_params)
    load_to_bronze_table()

if __name__ == "__main__":
    load_dotenv()
    main_flow()