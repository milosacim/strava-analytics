import requests
import json

from prefect import get_run_logger, task
from prefect.assets import materialize
from requests.exceptions import RequestException
from google.cloud import storage
from datetime import datetime

from tasks.autorization import get_access_token


@materialize("gs://my-strava-data-files")
def get_data_and_upload_to_gcs(access_token: str, before: str = None, after: str = None):
    """
    Fetches Strava activities for a date range and uploads them to GCS as JSON.s

    Args:
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

            bucket_name = "my-strava-data-files"
    
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"raw_data/activities{after.replace('-', '_')}_{before.replace('-', '_')}.json")

            is_uploaded = bucket.get_blob(blob_name=blob.name)

            if is_uploaded == None:

                blob.upload_from_string(
                    data=data,
                    content_type="application/json"
                )

                blob.metadata = {"Loaded": "False"}
                blob.patch(client=client)
            else:
                logger.info(f"File {blob.name} is already uploaded...")
        else:
            response.raise_for_status()
            
    except RequestException as e:
        logger.warning(f"There was an error while processing the request. \n {e}")
        raise

@task
def get_data_from_strava(params: dict):
    """
    Prefect task. Orchestrates token retrieval and activity upload to GCS.

    Args:
        params: Dict containing Strava OAuth credentials (client_id, client_secret,
                refresh_token, grant_type) and date range keys 'before' and 'after'
                in 'YYYY-MM-DD' format.
    """
    token = get_access_token(params)
    get_data_and_upload_to_gcs(token, before=params["before"], after=params["after"])