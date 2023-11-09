#ingesting dataset from python to postgre using class
import pandas as pd
from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean, Float
from sqlalchemy import create_engine

#reading the dataset
def read_data():
    df = pd.read_parquet("../dataset/yellow_tripdata_2023-01.parquet", engine='pyarrow')
    return df

def manipulate_data(df):

    #transforming datatype to appropriate value
    df.dropna(inplace = True)
    df['VendorID'] = df['VendorID'].astype('int8')
    df['passenger_count'] = df['passenger_count'].astype('int8')
    df['RatecodeID'] = df['RatecodeID'].astype('int8')
    df['PULocationID'] = df['PULocationID'].astype('int8')
    df['DOlocationID'] = df['DOLocationID'].astype('int8')
    df['payment_type'] = df['payment_type'].astype('int8')

    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].replace(["N", "Y"], [False, True])
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("boolean")

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    

    return df

#create connection class
def get_postgres():

    user = 'postgres'
    password = 'admin'
    host = 'localhost'
    database = 'mydb'
    port = 5432

    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_string) 
    return engine

def load_to_pg(engine):
    # df = pd.read_parquet("../dataset/yellow_tripdata_2023-01.parquet", engine='pyarrow')
#define db schema
    df_schema = {
        'VendorID' : BigInteger,
        'tpep_pickup_datetime' : DateTime,
        'tpep_dropoff_datetime' : DateTime,
        'passenger_count' : BigInteger,
        'trip_distance' : Float,
        'RatecodeID' : BigInteger,
        'store_and_fwd_flag' : Boolean,
        'PULocationID' : BigInteger,
        'DOLocationID' : BigInteger,
        'payment_type' : Float,
        'fare_amount' : Float,
        'extra': Float,
        'mta_tax': Float,
        'tip_amount' : Float,
        'tolls_amount':Float,
        'improvement_surcharge': Float,
        'total_amount':Float,
        'congestion_surcharge':Float,
        }
    
    df.to_sql(name='parquetdata', con=engine, if_exists="replace", index=False, schema="public", dtype=df_schema, method=None, chunksize=5000)
 
df = read_data()
clean_data = manipulate_data(df)
 
postgres_conn = get_postgres()
load_to_pg(postgres_conn)