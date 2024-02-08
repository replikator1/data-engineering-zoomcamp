# Block for loading data 
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    year = 2020
    month = [10,11,12]
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'lpep_pickup_datetime': str,
                    'lpep_dropoff_datetime':str,
                    'store_and_fwd_flag': pd.Int64Dtype(),
                    'RatecodeID': pd.Int64Dtype(),
                    'PULocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'fare_amount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'ehail_fee': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'payment_type': pd.Int64Dtype(),
                    'trip_type': pd.Int64Dtype(),
                    'congestion_surcharge': float
                }
        
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    df = pd.DataFrame(taxi_dtypes, index=[])
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month:02}.csv.gz'
    df = pd.read_csv(url, sep=',', compression='gzip', parse_dates=parse_dates, low_memory=False)
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.shape[0] > 0, 'The output in not empty'

# Block for transforming data
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd 
import re

@transformer
def transform(data, *args, **kwargs):
    df = data
    #Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    # Rename column to SnakeCase
    for col in df.columns:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col)
        new_col = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        df.columns = df.columns.str.replace(col, new_col)
    #return data
    return df 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.vendor_id is not None, 'Required column doesnt exist'
    assert len(output[output['passenger_count'] == 0]) == 0, 'Passenger_count is greater than 0'
    assert len(output[output['trip_distance'] == 0]) == 0, 'Trip distance is greater than 0'


# Block for exporting to GCS as partitioned_parquet
import pyarrow as pa
import pyarrow.parquet as pq 
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/dtc-de-course-412911-811dba4af3a4.json"

bucket_name = 'de-zoomcamp-dominik-1'

project_id = "dtc-de-course-412911"

table_name = "green"

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)

    gcs= pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem=gcs
    )

# Block for exporting data for Postgres

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'mage'  # Specify the name of the schema to export data to
    table_name = 'green_taxi'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )
