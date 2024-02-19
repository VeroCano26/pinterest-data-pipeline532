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

def send_to_kafka_topic(topic_name, data):
    # Convert datetime objects to strings
    data = convert_datetimes_to_strings(data)
    
    # Update the Kafka topic names
    kafka_topic_name = "0e6999790cc9." + topic_name
    
    invoke_url = "https://q6unwvg5fj.execute-api.us-east-1.amazonaws.com/test/topics/" + kafka_topic_name
    payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]
    })

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.post(invoke_url, headers=headers, data=payload)
    print(f"Message sent to Kafka topic '{kafka_topic_name}': {response.status_code} - {response.text}")

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
            # Fetch data from your tables as before
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

            # Send data to corresponding Kafka topics
            send_to_kafka_topic("pin", pin_result)
            send_to_kafka_topic("geo", geo_result)
            send_to_kafka_topic("user", user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')


