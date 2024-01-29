#!/usr/bin/env python
# coding: utf-8
import os
import argparse
import sys
import pandas as pd
from time import time
from sqlalchemy import create_engine
from file_downloader import FileDownloader


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    '''
    download_link = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

    file_path = 'green_tripdata_2019-09.csv.gz'

    downloader = FileDownloader(download_link, file_path)
    downloader.download()

    download_link = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

    file_path = 'taxi+_zone_lookup.csv'

    downloader = FileDownloader(download_link, file_path)
    downloader.download()
    '''
    #os.system(f"wget {url} -O {csv_name} --no-check-certificate")
    
    #os.system(f"wget --secure-protocol=auto --https-only --https-only --secure-protocol=auto --tls=1.2 {url}")

    #sys.exit(1)
    csv_name = 'green_tripdata_2019-09.csv'
    # download the csv
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))


def create_lookup_table(params):
    """
    Reads a CSV file and inserts its data into a PostgreSQL table.

    :param csv_file: Path to the CSV file.
    :param db_connection_string: Database connection string.
    :param table_name: Name of the table where data will be inserted.
    """
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db

    table_name = params.table_name
    csv_file = 'taxi+_zone_lookup.csv'
    table_name = 'taxi_zone_lookup'

    # Read CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Create a connection engine to the PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Convert the first column to integer
    df.iloc[:, 0] = df.iloc[:, 0].astype(int)

    # Insert data into the table
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    print(f"Data from {csv_file} has been inserted into the table {table_name}.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    #parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    
    #main(args)

    create_lookup_table(args)