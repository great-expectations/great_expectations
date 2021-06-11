import time
from urllib.parse import quote_plus

import numpy as np
import pyodbc

rng = np.random.default_rng(1234)
import pandas as pd
import sqlalchemy as sa
from faker import Faker
from sqlalchemy.sql.schema import ForeignKey, UniqueConstraint

conn_str_template = r"DRIVER={driver};SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password};TDS_VERSION={tds_version}"
db_password = "ReallyStrongPwd1234%^&*"


def gen_engine(conn):
    """Generate SQLAlchemy engine."""
    sql_engine_str = "mssql+pyodbc:///?odbc_connect="
    params = quote_plus(conn)
    return sa.create_engine(sql_engine_str + params, echo=False, fast_executemany=True)


def force_drop_db(conn_str, db_name):
    """Drop database

    Args:
        conn_str str: Connection string suitable for pyodbc
        db_name str: Name of database
    """
    tsql = r"""
    USE master;
    ALTER DATABASE {db} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE {db};
    """.format(
        db=db_name
    )

    connection = pyodbc.connect(
        conn_str,
        autocommit=True,
    )
    cursor = connection.cursor()
    cursor.execute(tsql)
    cursor.commit()
    cursor.close()
    connection.close()


def create_db(db_name):
    conn_str = conn_str_template.format(
        driver="ODBC Driver 17 for SQL Server",
        server="localhost",
        port="1433",
        database="master",
        username="sa",
        password=db_password,
        tds_version="7.3",
    )

    try:
        force_drop_db(conn_str, db_name)
    except Exception as e:
        print(e)

    tsql = r"""
    CREATE DATABASE {db};
    """.format(
        db=db_name
    )

    connection = pyodbc.connect(
        conn_str,
        autocommit=True,
    )
    cursor = connection.cursor()
    cursor.execute(tsql)
    cursor.commit()
    cursor.close()
    connection.close()


def write(conn_str, schema, table, df):
    tic = time.perf_counter()

    # pandas.to_sql is unfortunately slow when trying to write a lot of data.
    # http://docs.sqlalchemy.org/en/latest/faq/performance.html
    sql_insert = r"""
    INSERT INTO {}.{} {} VALUES {}
    """.format(
        schema,
        table,
        "(" + ", ".join(df.columns.to_list()) + ")",
        "(" + ", ".join(["?"] * len(df.columns)) + ")",
    ).replace(
        "'", ""
    )

    rader = [[y for y in x] for x in df.values]

    print("Writing {} rows to {}.{}.".format(len(rader), schema, table))

    connection = pyodbc.connect(conn_str)
    crsr = connection.cursor()
    crsr.fast_executemany = True
    crsr.executemany(sql_insert, rader)
    crsr.execute("COMMIT")
    crsr.close()
    connection.close()

    toc = time.perf_counter()
    sekunder = toc - tic

    msg = "Inserted {} rows into {}.{} in {} seconds.".format(
        len(rader), schema, table, sekunder
    )
    print(msg)

    return msg


def create_schema(conn_str, schema):
    """Create schema.

    Parameters
    ----------
    schema : str

    Returns
    -------
    (bool, msg) : tuple
        Bool indicates success or failure while msg explains what happened.
    """
    tsql = "CREATE SCHEMA {}".format(schema)

    with pyodbc.connect(conn_str) as con:
        con.execute(tsql)
        msg = "Created schema {}.".format(schema)
        print(msg)


def create_base_tables(db_name):
    conn_str = conn_str_template.format(
        driver="ODBC Driver 17 for SQL Server",
        server="localhost",
        port="1433",
        database=db_name,
        username="sa",
        password=db_password,
        tds_version="7.3",
    )
    engine = gen_engine(conn_str)

    metadata = sa.MetaData(schema="dbo")

    sa.Table(
        "dim_customer",
        metadata,
        sa.Column("Customer_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("Customer_name", sa.String(255), nullable=False, unique=False),
    )

    sa.Table(
        "dim_loan_type",
        metadata,
        sa.Column("Loan_type_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("Loan_type_name", sa.String(255), nullable=False, unique=True),
    )

    sa.Table(
        "dim_date",
        metadata,
        sa.Column("Date_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("Day", sa.SmallInteger, nullable=False),
        sa.Column("Month", sa.SmallInteger, nullable=False),
        sa.Column("Quarter", sa.SmallInteger, nullable=False),
        sa.Column("Year", sa.SmallInteger, nullable=False),
        UniqueConstraint("Day", "Month", "Year", name="Day_Month_Year"),
    )

    sa.Table(
        "fact_loan",
        metadata,
        sa.Column(
            "Loan_type_ID",
            sa.Integer,
            ForeignKey("dim_loan_type.Loan_type_ID"),
            primary_key=True,
        ),
        sa.Column(
            "Date_ID", sa.Integer, ForeignKey("dim_date.Date_ID"), primary_key=True
        ),
        sa.Column(
            "Customer_ID",
            sa.Integer,
            ForeignKey("dim_customer.Customer_ID"),
            primary_key=True,
        ),
        sa.Column("Amount", sa.DECIMAL(16, 2), nullable=False, unique=False),
        sa.Column("Rate", sa.DECIMAL(16, 2), nullable=False, unique=False),
    )

    metadata.create_all(engine)

    engine.dispose()


def populate_base_tables(db_name):
    conn_str = conn_str_template.format(
        driver="ODBC Driver 17 for SQL Server",
        server="localhost",
        port="1433",
        database=db_name,
        username="sa",
        password=db_password,
        tds_version="7.3",
    )
    engine = gen_engine(conn_str)

    nsamples = 10000

    fake = Faker()
    df_names = pd.DataFrame(
        [fake.name() for _ in range(nsamples)], columns=["Customer_name"]
    )
    write(conn_str, "dbo", "dim_customer", df_names)

    loan_types = ["Home equity", "Personal"]
    df_loans = pd.DataFrame({"Loan_type_name": loan_types})
    write(conn_str, "dbo", "dim_loan_type", df_loans)

    datelist = pd.date_range(start="2018-01-01", end="2020-01-01")
    days = pd.Series([date.day for date in datelist])
    months = pd.Series([date.month for date in datelist])
    quarters = pd.Series([date.quarter for date in datelist])
    years = pd.Series([date.year for date in datelist])
    df_dates = pd.DataFrame(
        {"Day": days, "Month": months, "Quarter": quarters, "Year": years}
    )
    df_dates["Day"] = df_dates["Day"].astype(pd.Int32Dtype())
    df_dates["Month"] = df_dates["Month"].astype(pd.Int32Dtype())
    df_dates["Quarter"] = df_dates["Quarter"].astype(pd.Int32Dtype())
    df_dates["Year"] = df_dates["Year"].astype(pd.Int32Dtype())
    write(conn_str, "dbo", "dim_date", df_dates)

    df_customers = pd.read_sql("SELECT * FROM dim_customer", con=engine)
    customer_ids = df_customers["Customer_ID"]

    df_loan_types = pd.read_sql("SELECT * FROM dbo.dim_loan_type", con=engine)
    loan_ids = pd.Series(
        np.random.choice(
            df_loan_types["Loan_type_ID"], size=len(customer_ids), replace=True
        )
    )

    df_dates = pd.read_sql("SELECT * FROM dbo.dim_date", con=engine)
    date_ids = pd.Series(
        np.random.choice(df_dates["Date_ID"], size=len(customer_ids), replace=True)
    )
    amount = rng.normal(loc=100000, scale=10000, size=nsamples)
    rate = np.random.choice([0.01, 0.03, 0.05, 0.06], size=nsamples, replace=True)

    df_fact_loan = pd.DataFrame(
        {
            "Loan_type_ID": loan_ids,
            "Date_ID": date_ids,
            "Customer_ID": customer_ids,
            "Amount": amount,
            "Rate": rate,
        }
    )
    write(conn_str, "dbo", "fact_loan", df_fact_loan)

    engine.dispose()


def create_views(engine):
    tsql = r"""
    CREATE VIEW [custom].[Loan] AS
    SELECT     f.Loan_type_ID
            , f.Date_ID
            , f.Customer_ID
            , f.Amount
            , f.Rate
            , c.Customer_name
            , d.Day
            , d.Month
            , d.Quarter
            , d.Year
            , l.Loan_type_name
    FROM       dbo.fact_loan     f
    INNER JOIN dbo.dim_customer  c
            ON c.Customer_ID = f.Customer_ID

    INNER JOIN dbo.dim_date      d
            ON d.Date_ID = f.Date_ID

    INNER JOIN dbo.dim_loan_type l
            ON l.Loan_type_ID = f.Loan_type_ID;
    """
    with engine.connect() as con:
        con.execute(tsql)


if __name__ == "__main__":
    db_name = "ge_test_db"
    create_db(db_name)

    conn_str = conn_str_template.format(
        driver="ODBC Driver 17 for SQL Server",
        server="localhost",
        port="1433",
        database=db_name,
        username="sa",
        password=db_password,
        tds_version="7.3",
    )

    create_base_tables(db_name)

    populate_base_tables(db_name)

    engine = gen_engine(conn_str)

    create_schema(conn_str, "custom")

    create_views(engine)
