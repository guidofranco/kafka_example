import pandas as pd
import requests

import os
from datetime import datetime

from config import CLIENT_ID, CLIENT_SECRET

BASE_URL = "https://apitransporte.buenosaires.gob.ar"
ENDPOINT = "ecobici/gbfs/stationInformation"
FINAL_URL = f"{BASE_URL}/{ENDPOINT}?client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}"

response = requests.get(FINAL_URL)

if response.status_code == 200:
    response = response.json()

    last_update = response["last_updated"]
    last_update = datetime.utcfromtimestamp(last_update)
    update_date, update_time = last_update.strftime("%Y-%m-%d"), last_update.strftime("%H-%M")
    
    data_dir = f"data/stations/{update_date}/{update_time}"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    data = response["data"]["stations"]
    df_stations = pd.DataFrame(data)
    df_stations.to_csv(f"{data_dir}/stations.csv", index=None)

    print("Success")
else:
    print(response.status_code)
