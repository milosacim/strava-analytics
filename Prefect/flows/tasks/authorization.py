import requests

from requests.exceptions import RequestException
from prefect import get_run_logger, task    


@task
def get_access_token(params: dict) -> str:
    """
    Prefect task. Exchanges a Strava refresh token for a short-lived access token.

    Args:
        params: Dict containing Strava OAuth credentials and date range fields.
                Required keys: client_id, client_secret, refresh_token, grant_type.

    Returns:
        A Strava API access token string.

    Raises:
        RequestException: If the token request fails.
    """

    logger = get_run_logger()

    try:
        response = requests.post(
            url="https://www.strava.com/oauth/token", 
            data=params
        )
        response.raise_for_status()
        return response.json()["access_token"]
    except RequestException as e:
        logger.warning(f"There was an error while processing the request. \n {e}")
        raise