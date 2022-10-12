from great_expectations.zep.core import SQLDatasource


def add_sql(name: str) -> SQLDatasource:
    print(f"Added SQL - {name}")
    return SQLDatasource()
