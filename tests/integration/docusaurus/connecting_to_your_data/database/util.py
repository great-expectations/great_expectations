from tests.integration.util import get_logger_from_config_file

logger = get_logger_from_config_file()


def load_data_into_database(
    table_name: str, csv_path: str, connection_string: str
) -> None:

    import pandas as pd
    import sqlalchemy as sa

    logger.debug("Starting tests.integration.docusaurus load_data_into_database")

    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    try:
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        logger.debug(f"Dropping table {table_name} from database")
        df = pd.read_csv(csv_path)
        df = df.head(
            10
        )  # <WILL> This line is here to address performance issues we have been running into with cloud resources (ie redshift). Can be taken out
        logger.debug(f"Creating table {table_name} from {csv_path}")
        df.to_sql(name=table_name, con=engine, index=False)
        logger.debug(f"tests.integration.docusaurus data loaded into database")
    except Exception as e:
        logger.error(
            "Unexpected error in tests.integration.docusaurus load_data_into_database: "
            + str(e)
        )
    finally:
        connection.close()
        engine.dispose()
