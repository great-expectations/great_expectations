import os
import pathlib

import pandas as pd

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa

csv_path = pathlib.Path("yellow_tripdata_sample_2019-01.csv")
df = pd.read_csv(csv_path)
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
