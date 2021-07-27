from sqlalchemy.exc import SQLAlchemyError

def load_data_into_database(
    table_name: str, csv_path: str, connection_string: str
) -> None:
    import pandas as pd
    import sqlalchemy as sa

    engine = sa.create_engine(connection_string)
    try:
        connection = engine.connect()
        print(f"Dropping table {table_name}")
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        df = pd.read_csv(csv_path)
        # helping performance
        df = df.head(10)
        print(f"Creating table {table_name} from {csv_path}")
        df.to_sql(name=table_name, con=engine, index=False)
    except SQLAlchemyError as e:
        # TODO Add logging messages here
        raise
    finally:
        connection.close()
        engine.dispose()
