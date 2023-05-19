import sqlalchemy as sa

connection = "mssql://sa:BK72nEAoI72CSWmP@db:1433/integration?driver=ODBC+Driver+17+for+SQL+Server&charset=utf&autocommit=true"
e = sa.create_engine(connection)
results = e.execute(sa.text("SELECT TOP 10 * from dbo.taxi_data")).fetchall()
for r in results:
    print(r)

print("finish")
