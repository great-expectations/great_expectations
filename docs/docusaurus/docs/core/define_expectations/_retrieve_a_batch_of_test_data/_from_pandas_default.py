import great_expectations as gx

context = gx.get_context()

file_path = "./data/nyc_trip_data_sample_2018.csv"

sample_batch = context.data_sources.pandas_default.read_csv(file_path)

sample_batch.head()
