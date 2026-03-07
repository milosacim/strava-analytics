import os
from dotenv import load_dotenv
from prefect import flow
from tasks.activities import get_data_from_strava


@flow
def main_flow(request_params: dict):
    get_data_from_strava(request_params)


if __name__ == "__main__":

    load_dotenv()
    
    _token_params = {
        "client_id": os.getenv('STRAVA_CLIENT_ID'),
        "client_secret": os.getenv('STRAVA_CLIENT_SECRET'),
        "refresh_token": os.getenv('STRAVA_REFRESH_TOKEN'),
        "grant_type": 'refresh_token'
    }

    main_flow(_token_params)