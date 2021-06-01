def load_data_into_database(
    table_name: str, csv_path: str, connection_string: str
) -> None:
    import pandas as pd
    import sqlalchemy as sa

    engine = sa.create_engine(connection_string)
    engine.execute(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Dropping table {table_name}")
    df = pd.read_csv(csv_path)
    print(f"Creating table {table_name} from {csv_path}")
    df.to_sql(name=table_name, con=engine, index=False)
