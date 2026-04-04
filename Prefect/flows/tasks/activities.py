import requests
import json

from prefect import task
from prefect.assets import materialize
from requests.exceptions import RequestException
from google.cloud import storage
from datetime import datetime

from tasks.autorization import get_access_token

@materialize("gs://my-strava-data-files")
def get_data_and_upload_to_gcs(access_token: str, before: str = None, after: str = None):

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

            data = response.json()

            bucket_name = "my-strava-data-files"
    
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"raw_data/activities{after.replace('-', '_')}_{before.replace('-', '_')}.json")

            blob.upload_from_string(
                data=json.dumps(data, indent=2),
                content_type="application/json"
            )
        else:
            response.raise_for_status()
    except RequestException as e:
        print(f"There was an error while processing the request. \n {e}")
        raise

@task
def get_data_from_strava(params: dict):
    token = get_access_token(params)
    get_data_and_upload_to_gcs(token, before=params["before"], after=params["after"])