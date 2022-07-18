import requests

import json
import logging
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

from config import CLIENT_ID, CLIENT_SECRET, BROKER_NAME, BROKER_PORT

BASE_URL = "https://apitransporte.buenosaires.gob.ar"
ENDPOINT = "ecobici/gbfs/stationStatus"
FINAL_URL = f"{BASE_URL}/{ENDPOINT}?client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}"

TOPIC = "ecobici_json"

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

while True:
    logging.info("Starting pipeline")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[f"{BROKER_NAME}:{BROKER_PORT}"],
            value_serializer=lambda m: json.dumps(m).encode('ascii')
            )

        response = requests.get(FINAL_URL)

        if response.status_code == 200:
            response = response.json()

            last_update = response["last_updated"]
            last_update = datetime.utcfromtimestamp(last_update)
            last_update = last_update.strftime("%Y-%m-%d %H:%M:%S")

            data = response["data"]["stations"]
            try:
                for event in data:
                    producer.send(TOPIC, event)
                logging.info("Sended data")
            except KafkaTimeoutError as e:
                logging.error(e)
            except:
                logging.error(e)
        else:
            logging.error(f"Codigo de error HTTP: {response.status_code}")    

    except NoBrokersAvailable as e:
        logging.error(e)
    finally:
        time.sleep(120)