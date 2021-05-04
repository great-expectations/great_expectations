from integration.code.query_postgres_runtime_data_connector import CONNECTION_STRING


def load_data_into_database(table_name: str, csv_path: str) -> None:
    import sqlalchemy as sa
    import pandas as pd

    engine = sa.create_engine(CONNECTION_STRING)
    engine.execute(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Dropping table {table_name}")
    df = pd.read_csv(csv_path)
    print(f"Creating table {table_name} from {csv_path}")
    df.to_sql(name=table_name, con=engine, index=False)
