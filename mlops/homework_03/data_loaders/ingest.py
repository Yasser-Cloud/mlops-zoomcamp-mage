import requests
from io import BytesIO
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def ingest_files(**kwargs) -> pd.DataFrame:
    # URL of the parquet file
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet'
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to download data: {response.text}")
    
    # Read the parquet file from the response content into a pandas DataFrame
    df = pd.read_parquet(BytesIO(response.content))
    
    return df
