import sqlalchemy as sa

db2_conn = "db2+ibm_db://db2inst1:my_db_password@hostname:50000/test_ci"

# start the SqlAlchemy engine
engine = sa.create_engine(db2_conn, echo=True, echo_pool="debug")
cnxn = engine.connect()
metadata = sa.MetaData()

# Create a schema
schema_name = "bigsql"
user_name = "db2inst1"
try:
    print("\n\nCREATING A SCHEMA")
    sql = f"CREATE SCHEMA {schema_name} AUTHORIZATION {user_name};"
    response = cnxn.execute(sql)
    print("SUCCESS CREATING A SCHEMA")
except Exception as e:
    print("FAILED AT CREATING A SCHEMA")
    print(e)

# Configure a tablespace
try:
    # https://stackoverflow.com/questions/53610628/a-table-space-could-not-be-found-with-a-page-size-of-at-least-4096-that-autho
    print("\n\nCREATING A TABLESPACE TO ENABLE THE CREATION OF TABLES")
    sql = """CREATE USER TEMPORARY TABLESPACE "UTMP4K"
             PAGESIZE 4096 MANAGED BY AUTOMATIC STORAGE
             USING STOGROUP "IBMSTOGROUP"
             EXTENTSIZE 4
             PREFETCHSIZE AUTOMATIC
             BUFFERPOOL "IBMDEFAULTBP"
             OVERHEAD INHERIT
             TRANSFERRATE INHERIT
             FILE SYSTEM CACHING
             DROPPED TABLE RECOVERY OFF;
    """
    response = cnxn.execute(sql)
    print("SUCCESS CREATING A TABLESPACE TO ENABLE THE CREATION OF TABLES")
except Exception as e:
    print("FAILED AT CREATING A TABLESPACE TO ENABLE THE CREATION OF TABLES")
    print(e)


# Test creating a temporary table (auto-delete when connexion is closed)
temp_table_name = "bigsql.GE_TMP_TABLE_1"
try:
    print("\n\nCREATE A TEMPORARY TABLE")
    # working for PM user on BDL DEV
    # the format of the schema works with both lowercase or uppercase
    # FAILS with single quotes around <schema_name>.<table_name>
    # Strange fact #1: the query fails running for the first time, and succeeds on the second!!!
    # Strange fact #2: adding "TEMPORARY" solves this issue, and create the table at once!!!
    # bug reported here: https://github.com/ibmdb/python-ibmdbsa/issues/93
    sql = (
        f"CREATE TEMPORARY TABLE {temp_table_name} ( ColOne int, ColTwo varchar(255), "
        "ColThree varchar(255), ColFour varchar(255), ColFive varchar(255) );"
    )
    response = cnxn.execute(sql)
    print(response)
    print("SUCCESS CREATING A TEMPORARY TABLE")

    print("\n\nINSERT DATA IN A TEMPORARY TABLE")
    try:
        # FAILS with single/DOUBLE quotes around <schema_name>.<table_name>
        sql = (
            f"INSERT INTO {temp_table_name} ( ColOne, ColTwo, ColThree, ColFour, ColFive) "
            " VALUES ('1234', 'abcd', 'efgh', 'ijkl', 'mnop');"
        )
        response = cnxn.execute(sql)
        print(response)
        print("SUCCESS INSERTING DATA IN TEMPORARY TABLE")
    except Exception as e2:
        print("FAILED TO INSERT DATA IN TEMPORARY TABLE")
        print(e2)
except Exception as e:
    print("FAILED TO CREATE A TEMPORARY TABLE")
    print(e)


try:
    print("\n\nREADING DATA FROM TEMPORARY TABLE")
    sql = f"SELECT * FROM {temp_table_name} FETCH FIRST 1 ROWS ONLY"
    response = cnxn.execute(sql)
    print(response.fetchone())
    print("SUCCESS READING TABLE")
except Exception as e:
    print("FAILED TO READ THE TEMPORARY TABLE")
    print(e)


second_temp_table_name = "bigsql.GE_TMP_TABLE_2"
try:
    print("\n\nCREATE TEMPORARY TABLE FROM READING DATA FROM TABLE FIRST 1000 ROWS")
    sql = (
        f"CREATE TEMPORARY TABLE {second_temp_table_name} AS "
        f"(SELECT * FROM {temp_table_name} FETCH FIRST 1000 ROWS ONLY);"
    )
    response = cnxn.execute(sql)
    print(response)
    print("SUCCESS CREATE TEMPORARY TABLE FROM READING TABLE FIRST 1000 ROWS")
except Exception as e:
    print(e)
    print("FAILED TO CREATE TEMPORARY TABLE FROM READING THE TABLE FIRST 1000 ROWS")

try:
    print("\n\nREADING THE TEMPORARY TABLE")
    sql = f"SELECT * FROM {second_temp_table_name};"
    response = cnxn.execute(sql)
    print(response.fetchone())
    print("SUCCESS READING THE TEMPORARY TABLE")
except Exception as e:
    print(e)
    print("FAILED AT READING THE TEMPORARY TABLE")



permanent_table = "bigsql.GE_TABLE_ABC3"
try:
    # https://www.ibm.com/support/knowledgecenter/en/ssw_ibm_i_71/sqlp/rbafycrttblas.htm
    print("\n\nCREATE TABLE FROM READING DATA FROM TABLE FIRST 1000 ROWS")
    sql = (
        f"CREATE TABLE {permanent_table} AS "
        f"(SELECT * FROM {second_temp_table_name}) WITH DATA"
    )
    response = cnxn.execute(sql)
    print(response)
    print("SUCCESS CREATE TABLE")
except Exception as e:
    print(e)
    print("FAILED TO CREATE TABLE")

try:
    print("\n\nREADING TABLE")
    sql = f"SELECT * FROM {permanent_table};"
    response = cnxn.execute(sql)
    print(response.fetchone())
    print("SUCCESS READING TABLE")
except Exception as e:
    print(e)
    print("FAILED READING TABLE")


permanent_view = "bigsql.GE_VIEW_ABC3"
try:
    # https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_72/sqlp/rbafyviewnt.htm
    print("\n\nCREATE VIEW FROM READING DATA FROM TABLE")
    sql = (
        f"CREATE VIEW {permanent_view} AS "
        f"(SELECT * FROM {permanent_table})"
    )
    response = cnxn.execute(sql)
    print(response)
    print("SUCCESS CREATE VIEW")
except Exception as e:
    print(e)
    print("FAILED TO CREATE VIEW")

try:
    print("\n\nREADING VIEW")
    sql = f"SELECT * FROM {permanent_view};"
    response = cnxn.execute(sql)
    print(response.fetchone())
    print("SUCCESS READING VIEW")
except Exception as e:
    print(e)
    print("FAILED READING VIEW")

