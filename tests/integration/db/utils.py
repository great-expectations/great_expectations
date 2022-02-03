import logging

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy.exc import SQLAlchemyError

except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sa = None
    reflection = None
    Table = None
    Select = None


def check_athena_table_count(
    connection_string: str, db_name: str, expected_table_count: int
) -> bool:
    """
    Helper function used by awsathena integration test. Checks whether expected number of tables exist in database
    """
    if sa:
        engine = sa.create_engine(connection_string)
    else:
        logger.debug(
            "Attempting to perform test on AWSAthena database, but unable to load SqlAlchemy context; "
            "install optional sqlalchemy dependency for support."
        )
        return
    try:
        athena_connection = engine.connect()
        result = athena_connection.execute(
            sa.text(f"SHOW TABLES in {db_name}")
        ).fetchall()
        return len(result) == expected_table_count
    except SQLAlchemyError as e:
        logger.error(
            f"""Docs integration tests encountered an error while loading test-data into test-database."""
        )
        raise
    finally:
        athena_connection.close()
        engine.dispose()


def clean_athena_db(connection_string: str, db_name: str, table_to_keep: str) -> None:
    """
    Helper function used by awsathena integration test. Cleans up "temp" tables that were created.
    """
    if sa:
        engine = sa.create_engine(connection_string)
    else:
        logger.debug(
            "Attempting to perform test on AWSAthena database, but unable to load SqlAlchemy context; "
            "install optional sqlalchemy dependency for support."
        )
        return
    try:
        athena_connection = engine.connect()
        result = athena_connection.execute(
            sa.text(f"SHOW TABLES in {db_name}")
        ).fetchall()
        for table_tuple in result:
            table = table_tuple[0]
            if table != table_to_keep:
                athena_connection.execute(sa.text(f"DROP TABLE `{table}`;"))
    finally:
        athena_connection.close()
        engine.dispose()
