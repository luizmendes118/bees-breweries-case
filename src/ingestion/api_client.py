import requests
import json
import os
import time

from src.utils.config import BASE_API_URL, PAGE_SIZE, MAX_PAGES, BRONZE_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 5


def fetch_breweries():

    all_data = []

    for page in range(1, MAX_PAGES + 1):

        params = {
            "per_page": PAGE_SIZE,
            "page": page
        }

        logger.info(f"Fetching page {page}")

        retries = 0

        while retries < MAX_RETRIES:

            try:

                response = requests.get(
                    BASE_API_URL,
                    params=params,
                    timeout=10
                )

                if response.status_code != 200:
                    raise Exception(f"API request failed: {response.status_code}")

                data = response.json()

                if not data:
                    break

                all_data.extend(data)

                break

            except Exception as e:

                retries += 1

                logger.warning(
                    f"Error fetching page {page}. Retry {retries}/{MAX_RETRIES}"
                )

                if retries == MAX_RETRIES:
                    raise Exception("Maximum retries reached for API request")

                time.sleep(RETRY_DELAY)

    logger.info(f"Total records fetched: {len(all_data)}")

    return all_data


def save_bronze(data):

    os.makedirs(BRONZE_PATH, exist_ok=True)

    file_path = f"{BRONZE_PATH}/breweries_raw.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    logger.info(f"Bronze data saved at {file_path}")
