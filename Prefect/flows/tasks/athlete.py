import os

from prefect.cache_policies import NO_CACHE
import requests
import json

from prefect import get_run_logger
from prefect.assets import materialize
from requests.exceptions import RequestException

BUCKET = os.getenv("BUCKET")

@materialize(f"gs://{BUCKET}", cache_policy=NO_CACHE)
def get_data_and_upload_athlete_to_gcs(storage_client, access_token: str):
    """
    Fetches Strava athlete and uploads newline-delimited JSON to GCS.

    Args:
        storage_client: An authenticated google.cloud.storage.Client.
        access_token: A valid Strava API access token.

    Raises:
        RequestException: If the Strava API request fails.
    """

    logger = get_run_logger()

    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.get(
            url=f"https://www.strava.com/api/v3/athlete",
            headers=headers
        )
        if response.status_code == 200:
            
            athlete = response.json()
            data = json.dumps(athlete)
    
            bucket = storage_client.bucket(BUCKET)
            blob = bucket.blob(f"raw_data/athlete/athlete.json")

            if blob.exists(storage_client):

                blob.delete()
                blob.upload_from_string(
                    data=data,
                    content_type="application/json"
                )
            else:
                blob.upload_from_string(
                    data=data,
                    content_type="application/json"
                )
        else:
            response.raise_for_status()
            
    except RequestException as e:
        logger.warning(f"There was an error while processing the request. \n {e}")
        raise