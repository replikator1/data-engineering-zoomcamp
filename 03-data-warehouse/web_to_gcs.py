import io
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "de-zoomcamp-dominik-1")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(2,12):
        taxi_dtypes_dic = {'green': {
                            'VendorID': pd.Int64Dtype(),
                            'store_and_fwd_flag': str,
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
                        }    ,
                       'yellow': {
                            'VendorID': pd.Int64Dtype(),
                            'passenger_count': pd.Int64Dtype(),
                            'trip_distance': float,
                            'RatecodeID': pd.Int64Dtype(),
                            'store_and_fwd_flag': str,
                            'PULocationID': pd.Int64Dtype(),
                            'DOLocationID': pd.Int64Dtype(),
                            'payment_type': pd.Int64Dtype(),
                            'fare_amount': float,
                            'extra': float,
                            'mta_tax': float,
                            'tip_amount': float,
                            'tolls_amount': float,
                            'improvement_surcharge': float,
                            'total_amount': float,
                            'congestion_surcharge': float
                        }    ,
                       'fhv':{
                            'dispatching_base_num': str,
                            'PULocationID': pd.Int64Dtype(),
                            'DOLocationID': pd.Int64Dtype(),
                            'SR_Flag': float,
                            'Affiliated_base_number':str
                        }    }
        parse_data_dic = {'green': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
                          'yellow': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
                          'fhv': ['pickup_datetime', 'dropOff_datetime']
}
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, dtype=taxi_dtypes_dic[service], parse_dates=parse_data_dic[service], compression='gzip')
        if service == 'green':
            df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
            df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
        if service == 'yellow':
            df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
            df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
        if service == 'fhv':
            df['pickup_datetime'] = df['pickup_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
            df['dropOff_datetime'] = df['dropOff_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")       
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2020', 'yellow')


