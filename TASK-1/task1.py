import pandas as pd

#reading the dataset
df = pd.read_csv(("../dataset/yellow_tripdata_2020-07.csv"))

#renaming columns to snake_case format
df = df.rename(columns={'VendorID': 'vendor_id', 'RatecodeID': 'rate_code_id', 'PULocationID':'pu_location_id'
                                    ,'DOLocationID':'do_location_id'})
#showing changes
# print (df.columns)

#top 10 highest number of passanger_count
max_pass_count = df.nlargest(10, 'passenger_count')[['vendor_id', 'passenger_count', 'trip_distance', 
                                                     'payment_type', 'fare_amount', 'extra', 'mta_tax', 
                                                     'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']]
# print(max_pass_count)

#transforming datatype to appropriate value
df=df.dropna()
df['vendor_id'] = df['vendor_id'].astype(int)
df['passenger_count'] = df['passenger_count'].astype(int)
df['rate_code_id'] = df['rate_code_id'].astype(int)
df['pu_location_id'] = df['pu_location_id'].astype(int)
df['do_location_id'] = df['do_location_id'].astype(int)
df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
df["store_and_fwd_flag"] = df["store_and_fwd_flag"].replace(["N", "Y"], [False, True])
df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("boolean")
print(df.dtypes)