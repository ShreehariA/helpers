import json
import logging
import requests
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def sat_auth(sat_oauth_link: str, secrets: dict, timeout: int = 900) -> str:
    """
    Get auth token from SAT.
    """
    OAUTH_LINK = sat_oauth_link
    HTTP_REQUEST_TIMEOUT = timeout
    CLIENT_ID = secrets["CLIENT_ID"]
    CLIENT_SECRET = secrets["CLIENT_SECRET"]
    
    sat_params = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "sphere:api:read"
    }
    sat_headers = {
        "X-Client-Id": CLIENT_ID,
        "X-Client-Secret": CLIENT_SECRET,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    logger.info("sending request to SAT to get OAuth token")

    response = requests.post(
        url=OAUTH_LINK,
        data=sat_params,
        headers=sat_headers,
        timeout=HTTP_REQUEST_TIMEOUT
    )
    logger.info(f"received response with status code {response.status_code}")

    if response.status_code == 200:
        data = response.json()
    else:
        data = ""
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_error:
        print(f"HTTP error occurred: {http_error}")
    return data['access_token']

def call_lookup_api(url: str, user_guid: str, sat_token: str) -> pd.DataFrame:
    """
    Calls the lookup API using the given URL, user GUID, and SAT token, and returns a DataFrame of the results.
    """
    logger.info(f"calling lookup API for URL: {url}")

    try:
        headers = {
            'Authorization': f"Bearer {sat_token}",
            'source': 'dream',
            'user-id': user_guid,
        }
        response = requests.get(
            url = url,
            headers = headers
        )

        data = response.text
        logger.info(f"successfully reserved response with {len(data)} items.")

        json_data = json.loads(data[10:-46])
        temp_dict = {key: value for item in json_data for key, value in item.items()}
        df = pd.DataFrame(temp_dict.items(), columns=['ID', 'HASH'])
        logger.info(f"successfully converted to dataframe of length: {len(df)}")

        return df
    except Exception as e:
        print(f"error while calling lookup:{e}")
        raise(f"{e}\nresponse{data}")