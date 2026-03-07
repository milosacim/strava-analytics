from prefect import task
import requests
import json

@task
def get_access_token(params: dict) -> str:

    response = requests.post(url="http://www.strava.com/oauth/token", params=params)
    access_token = json.loads(response.content)["access_token"]
    return access_token