import sqlalchemy as sa

db2_conn = "db2+ibm_db://db2inst1:my_db_password@localhost:50000/test_ci"

# start the SqlAlchemy engine
engine = sa.create_engine(db2_conn, echo=True, echo_pool="debug")
cnxn = engine.connect()
metadata = sa.MetaData()

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
