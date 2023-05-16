import os
import pathlib

import pandas as pd
import sqlalchemy as sa

# TODO: Better waiting for db (polling):
print("Waiting for db to be ready")
import time
time.sleep(5)

print("Loading data into dataframe...")
csv_path = pathlib.Path("yellow_tripdata_sample_2019-01.csv")
df = pd.read_csv(csv_path)

print("Loading data into database...")
connection_string = os.getenv("CONNECTION_STRING")
engine = sa.create_engine(connection_string)
with engine.connect() as connection:
    name = "nyc_taxi_data"
    if_exists = "replace"
    df.to_sql(
        name=name,
        con=connection,
        if_exists=if_exists,
    )
