import pandas as pd
import requests

import logging
import os
from datetime import datetime

from config import CLIENT_ID, CLIENT_SECRET

BASE_URL = "https://apitransporte.buenosaires.gob.ar"
ENDPOINT = "ecobici/gbfs/stationInformation"
FINAL_URL = f"{BASE_URL}/{ENDPOINT}?client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}"

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

response = requests.get(FINAL_URL)

if response.status_code == 200:
    logging.info(f"HTTP Status code: {response.status_code}")
    response = response.json()

    last_update = response["last_updated"]
    last_update = datetime.utcfromtimestamp(last_update)
    update_date, update_time = last_update.strftime("%Y-%m-%d"), last_update.strftime("%H-%M")
    
    data_dir = f"data/stations/{update_date}/{update_time}"
    logging.info(f"Creando directorio {data_dir}")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    logging.info("Procesando datos")
    data = response["data"]["stations"]
    df_stations = pd.DataFrame(data)
    df_stations.to_csv(f"{data_dir}/stations.csv", index=None)
    
    logging.info(f"Los datos han sido almacenados en {data_dir}")
else:
    logging.error(f"Codigo de error HTTP: {response.status_code}")

