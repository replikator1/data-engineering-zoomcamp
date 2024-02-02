if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    year = 2020
    months = [10,11,12]

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

    for month in months:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month:02}.csv.gz'
        df_month = pd.read_csv(url, sep=',', compression='gzip', parse_dates=parse_dates)
        df = pd.concat([df,df_month], ignore_index=True)
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.shape[0] > 0, 'The output in not empty'
