from prefect import task
from prefect.assets import materialize
import requests
import json
from google.cloud import storage

from flows.tasks.autorization import get_access_token

@task
def get_activities(access_token):
    response = requests.get(url="https://www.strava.com/api/v3/athlete/activities", params={"access_token": access_token})
    return json.loads(response.content)

@materialize("gs://my-strava-data-files")
def upload_to_gcs(bucket_name: str, data: str):
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("raw_data/test.json")

    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type="application/json"
    )

@task
def get_data_from_strava(params: dict):
    token = get_access_token(params)
    
    data = get_activities(token)

    upload_to_gcs("my-strava-data-files", data)