# import psycopg2
# import yaml
# import os

# with open('/Users/clifflolo/Desktop/Hubtel/nytttttttt/config/config.yml', 'r') as file:
#     config = yaml.safe_load(file)

# def get_db_connection():
#     conn = psycopg2.connect(
#         dbname=config['database']['dbname'],
#         user=config['database']['user'],
#         # password=config['database']['password'],
#         password=config['database'],
#         host=config['database']['host'],
#         port=config['database']['port']
#     )
#     return conn



# import psycopg2
# import yaml
# import os

# def load_config():
#     # Detect if running in Docker
#     # if os.getenv('DOCKER_ENV'):
#     #     config_path = '/app/config/config.yml'  # Docker path
#     # else:
#     #     config_path = '/Users/clifflolo/Desktop/Hubtel/nytttttttt/config/config.yml'  # Local path

#     config_path = '/app/config/config.yml' if os.getenv('DOCKER_ENV') else 'config/config.yml'

#     with open(config_path, 'r') as file:
#         return yaml.safe_load(file)


# config = load_config()

# def get_db_connection():
#     conn = psycopg2.connect(
#         dbname=config['database']['dbname'],
#         user=config['database']['user'],
#         password=config['database'],  
#         host=config['database']['host'],
#         port=config['database']['port']
#     )
#     return conn




import psycopg2
import yaml
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    return conn
