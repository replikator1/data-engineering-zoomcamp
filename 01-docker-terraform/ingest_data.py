#!/usr/bin/env python
# coding: utf-8

# Import libs
import argparse 
import pandas as pd
from time import time
from sqlalchemy import create_engine
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db =  params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = 'output.csv.gz'

    # download the csv 
    os.system(f'wget {url} -O {csv_name}')
    url = params.url

    # Create engine 
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read csv
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Ingest data
    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f'chunk loaded!, took {t_end - t_start}')






# Parsing args 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password name for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port name for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='name of tablefor postgres')
    parser.add_argument('--url', help='url of table to ingest for postgres')

    args = parser.parse_args()
    main(args)





















