from datetime import date, datetime
import os
from dotenv import load_dotenv
from prefect import flow
from tasks.activities import get_data_from_strava


@flow
def main_flow(request_params: dict):
    """
    Prefect flow. Entry point for the Strava analytics pipeline.

    Triggers the get_data_from_strava task which handles OAuth token exchange
    and uploads activities data to GCS.

    Args:
        request_params: Dict containing Strava OAuth credentials and date range.
                        Keys: client_id, client_secret, refresh_token, grant_type,
                        before, after (dates in 'YYYY-MM-DD' format).
    """
    get_data_from_strava(request_params)


if __name__ == "__main__":

    load_dotenv()
    
    _token_params = {
        "client_id": os.getenv('STRAVA_CLIENT_ID'),
        "client_secret": os.getenv('STRAVA_CLIENT_SECRET'),
        "refresh_token": os.getenv('STRAVA_REFRESH_TOKEN'),
        "grant_type": 'refresh_token',
        "after": "2026-04-01",
        "before": "2026-06-30"
    }

    main_flow(_token_params)