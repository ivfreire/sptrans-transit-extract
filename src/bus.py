#   ===================================
#   Extracts data from Olho Aberto API.
#   ===================================

import requests
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_gbq as gbq

# =========================================================================== #

BASE_URL = 'https://olhovivo.sptrans.com.br/'
EXPORT_COLUMNS = ['route_id', 'trip_code', 'direction_id', 'bus_prefix', 'is_accessible', 'timestamp', 'lat', 'lon']

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

def format(info_df: pd.DataFrame, export_columns: list) -> pd.DataFrame:
    info_df.columns = ['route_id', 'trip_code', 'direction_id', 'to', 'from', 'bus_count', 'buses']

    buses_df = info_df.explode('buses').reset_index(drop=True)

    bus_info_df = pd.json_normalize(buses_df['buses'])
    bus_info_df.columns = ['bus_prefix', 'b', 'is_accessible', 'timestamp', 'lat', 'lon', 'sv', 'is']

    bus_info_df = pd.concat([
        buses_df.drop(columns=['buses']),
        bus_info_df
    ], axis=1)

    bus_info_df['timestamp'] = pd.to_datetime(bus_info_df['timestamp'])
    bus_info_df['ingestion_time'] = datetime.utcnow()

    return bus_info_df[['ingestion_time'] + export_columns]

# --------------------------------------------------------------------------- #

def upload(info_df: pd.DataFrame) -> None:
    gbq.to_gbq(
        info_df,
        destination_table='sptransit.transit.bus_position',
        project_id='sptransit',
        if_exists='append'
    )

# --------------------------------------------------------------------------- #

def main():
    session = create_session()

    info_df = _request(session)
    info_df = format(info_df, EXPORT_COLUMNS)

    upload(info_df)

# --------------------------------------------------------------------------- #

if __name__ == '__main__':
    main()

# =========================================================================== #
