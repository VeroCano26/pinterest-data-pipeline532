import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

random.seed(100)

class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def send_data_to_api(stream_name, data):
    # Convert datetime objects to strings
    data = convert_datetimes_to_strings(data)

    # Update the API endpoint URL
    api_url = f"https://q6unwvg5fj.execute-api.us-east-1.amazonaws.com/test/streams/{stream_name}/record"

    # Update the payload structure
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": data,
        "PartitionKey": "desired-name"
    }, default=str)  # Use default=str to handle datetime serialization

    headers = {'Content-Type': 'application/json'}
    response = requests.put(api_url, headers=headers, data=payload, timeout=10)

    # Print the status code
    print(f"Status Code: {response.status_code}")

    # Check if the request was successful
    if response.status_code == 200:
        print("Request successful!")
    else:
        print("Request failed.")
        print(response.text)  # Print the response content for debugging if needed

def convert_datetimes_to_strings(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            # Convert datetime to ISO 8601 format
            data[key] = value.isoformat()
    return data

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            # Send data to corresponding API endpoints
            send_data_to_api("pin", pin_result)
            send_data_to_api("geo", geo_result)
            send_data_to_api("user", user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
