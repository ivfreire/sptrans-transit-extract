#   ===================================
#   Extracts data from Olho Aberto API.
#   ===================================

import os
import json
import requests
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_gbq as gbq

from google.cloud import pubsub_v1
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# =========================================================================== #

BASE_URL = 'https://olhovivo.sptrans.com.br/'
EXPORT_COLUMNS = ['ingestion_time', 'route_id', 'trip_code', 'direction_id',
                  'bus_prefix', 'is_accessible', 'timestamp', 'lat', 'lon']
SHARDS_COUNT = 10

MONGODB_URI = os.getenv('MONGODB_URI')

# --------------------------------------------------------------------------- #

def create_session() -> requests.Session:
    """
    Creates a new session for making API requests.
    Returns:
        A requests.Session object.
    """
    
    session = requests.Session()
    session.get(BASE_URL)
    
    return session

# --------------------------------------------------------------------------- #

def _request(session) -> pd.DataFrame:
    request = session.get(url=f'{BASE_URL}/data/Posicao')

    if request.status_code != 200:
        raise Exception(f"Failed to fetch data: {request.status_code}, {request.text}")
    
    data = request.json()

    return pd.DataFrame(data['l'])

# --------------------------------------------------------------------------- #

def _format(info_df: pd.DataFrame) -> pd.DataFrame:
    info_df.columns = ['route_id', 'trip_code', 'direction_id', 'to', 'from', 'bus_count', 'buses']

    trips_df = info_df.explode('buses').reset_index(drop=True)
    trips_df['trip_id'] = trips_df['route_id'].astype(str) + '-' + (trips_df['direction_id'] - 1).astype(str)

    return trips_df

# --------------------------------------------------------------------------- #

def extract_bus(trips_df: pd.DataFrame) -> pd.DataFrame:
    bus_info_df = pd.json_normalize(trips_df['buses'])
    bus_info_df.columns = ['bus_prefix', 'b', 'is_accessible', 'timestamp', 'lat', 'lon', 'sv', 'is']

    bus_info_df = pd.concat([
        trips_df.drop(columns=['buses']),
        bus_info_df
    ], axis=1)

    bus_info_df['timestamp'] = pd.to_datetime(bus_info_df['timestamp'])
    bus_info_df['ingestion_time'] = datetime.utcnow()

    return bus_info_df

# --------------------------------------------------------------------------- #

def send_pubsub(buses_df: pd.DataFrame) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('sptransit', 'sptrans-transit-positions')

    def send(sliced_df: pd.DataFrame) -> None:
        data = sliced_df[['trip_id', 'bus_prefix', 'lat', 'lon', 'timestamp']]
        data['timestamp'] = data['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        data = json.dumps(data.to_dict())

        print(f"Publishing to {topic_path}: {len(data)} bytes")
        publisher.publish(topic_path, data.encode('ASCII'))

    shards = np.array_split(buses_df, SHARDS_COUNT)

    for shard in shards:
        send(shard)

# --------------------------------------------------------------------------- #

def upload_to_gbq(info_df: pd.DataFrame) -> None:
    gbq.to_gbq(
        info_df,
        destination_table='sptransit.transit.bus_position_v2',
        project_id='sptransit',
        if_exists='append'
    )

# --------------------------------------------------------------------------- #

def upload_to_mongodb(info_df: pd.DataFrame) -> None:
    collection = MongoClient(MONGODB_URI)["transit"]["bus"]
    collection.insert_many(info_df.to_dict("records"))

# --------------------------------------------------------------------------- #

def main():
    session = create_session()

    info_df = _request(session)
    info_df = _format(info_df)
    info_df = extract_bus(info_df)

    upload_to_gbq(info_df[EXPORT_COLUMNS])
    upload_to_mongodb(info_df[EXPORT_COLUMNS])

# --------------------------------------------------------------------------- #

if __name__ == '__main__':
    main()

# =========================================================================== #
