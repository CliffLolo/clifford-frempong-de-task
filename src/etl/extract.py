# # extract.py
# import requests
# import yaml
# import time
# from src.etl.utils.logger import get_logger

# logger = get_logger()

# def load_config():
#     with open('/Users/clifflolo/Desktop/Hubtel/nytttttttt/config/config.yml', 'r') as file:
#         return yaml.safe_load(file)

# config = load_config()
# API_KEY = config['api']['key']
# BASE_URL = config['api']['base_url']

# def extract_data(published_date):
#     params = {
#         'published_date': published_date,
#         'api-key': API_KEY
#     }
#     try:
#         logger.info(f"Extracting data for {published_date}...")
#         response = requests.get(BASE_URL, params=params)

#         if response.status_code == 200:
#             logger.info(f"Data extraction successful for {published_date}")
#             return response.json()
#         elif response.status_code == 429:
#             logger.warning(f"Rate limit hit for {published_date}. Retrying after 15 seconds...")
#             time.sleep(15)
#             return extract_data(published_date)
#         else:
#             logger.error(f"Error fetching data for {published_date}: {response.status_code} - {response.text}")
#             raise Exception(f"Error fetching data for {published_date}")
#     except Exception as e:
#         logger.error(f"Failed to extract data for {published_date}: {e}")
#         raise



# # extract.py
# import requests
# import yaml
# import time
# from datetime import datetime, timedelta
# from src.etl.utils.logger import get_logger
# import os

# logger = get_logger()

# # def load_config():
# #     with open('config/config.yml', 'r') as file:
# #         return yaml.safe_load(file)

# # config = load_config()



# def load_config():
#     # Check if running inside Docker using DOCKER_ENV variable
#     config_path = '/app/config/config.yml' if os.getenv('DOCKER_ENV') else 'config/config.yml'

#     with open(config_path, 'r') as file:
#         return yaml.safe_load(file)

# config = load_config()


# API_KEY = config['api']['key']
# BASE_URL = config['api']['base_url']

# def extract_data(published_date):
#     params = {
#         'published_date': published_date,
#         'api-key': API_KEY
#     }
#     try:
#         logger.info(f"Extracting data for {published_date}...")
#         response = requests.get(BASE_URL, params=params)
        
#         if response.status_code == 200:
#             data = response.json()
#             # Validate expected fields
#             if 'results' not in data:
#                 raise ValueError("Missing 'results' in API response")
#             logger.info(f"Data extraction successful for {published_date}")
#             return data
#         elif response.status_code == 429:
#             logger.warning(f"Rate limit hit for {published_date}. Retrying after 15 seconds...")
#             time.sleep(15)
#             return extract_data(published_date)
#         else:
#             logger.error(f"Error fetching data: {response.status_code} - {response.text}")
#             raise Exception(f"API error: {response.status_code}")
#     except Exception as e:
#         logger.error(f"Failed to extract data: {e}")
#         raise


import requests
import time
from datetime import datetime, timedelta
from src.etl.utils.logger import get_logger
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

logger = get_logger()

# Load API credentials strictly from .env
API_KEY = os.getenv('API_KEY')
BASE_URL = os.getenv('BASE_URL')

# Ensure API_KEY and BASE_URL are set
if not API_KEY or not BASE_URL:
    logger.error("API_KEY or BASE_URL is missing in the .env file.")
    raise ValueError("API_KEY or BASE_URL is not set in the .env file.")

def extract_data(published_date):
    params = {
        'published_date': published_date,
        'api-key': API_KEY
    }
    try:
        logger.info(f"Extracting data for {published_date}...")
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            # Validate expected fields
            if 'results' not in data:
                raise ValueError("Missing 'results' in API response")
            logger.info(f"Data extraction successful for {published_date}")
            return data
        elif response.status_code == 429:
            logger.warning(f"Rate limit hit for {published_date}. Retrying after 15 seconds...")
            time.sleep(15)
            return extract_data(published_date)
        else:
            logger.error(f"Error fetching data: {response.status_code} - {response.text}")
            raise Exception(f"API error: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to extract data: {e}")
        raise
