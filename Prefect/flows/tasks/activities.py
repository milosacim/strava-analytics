import os

from prefect.cache_policies import NO_CACHE
import requests
import json

from prefect import get_run_logger, task
from prefect.assets import materialize
from requests.exceptions import RequestException
from datetime import datetime

BUCKET = os.getenv("BUCKET")

@materialize(f"gs://{BUCKET}", cache_policy=NO_CACHE)
def get_data_and_upload_activities_to_gcs(storage_client, access_token: str, before: str = None, after: str = None):
    """
    Fetches Strava activities for a date range and uploads them to GCS as
    newline-delimited JSON. Skips the upload if the target blob already exists.

    Args:
        storage_client: An authenticated google.cloud.storage.Client.
        access_token: A valid Strava API access token.
        before: Upper bound date string in 'YYYY-MM-DD' format.
        after: Lower bound date string in 'YYYY-MM-DD' format.

    Raises:
        RequestException: If the Strava API request fails.
    """

    logger = get_run_logger()

    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.get(
            url=f"https://www.strava.com/api/v3/athlete/activities",
            headers=headers,
            params={
                "before": datetime.strptime(before, '%Y-%m-%d').timestamp(), 
                "after": datetime.strptime(after, '%Y-%m-%d').timestamp(), 
                "page": 1, 
                "per_page": 60
            }
        )
        if response.status_code == 200:

            data = "\n".join(json.dumps(row) for row in response.json())
    
            bucket = storage_client.bucket(BUCKET)
            blob = bucket.blob(f"raw_data/activities/activities{after.replace('-', '_')}_{before.replace('-', '_')}.json")

            if not blob.exists(storage_client):

                blob.upload_from_string(
                    data=data,
                    content_type="application/json"
                )

            else:
                logger.info(f"File {blob.name} is already uploaded...")
        else:
            response.raise_for_status()
            
    except RequestException as e:
        logger.warning(f"There was an error while processing the request. \n {e}")
        raise