from __future__ import annotations

from typing import Literal

from great_expectations.compatibility.pydantic import AnyUrl
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.config_str import ConfigUri


class BigQueryDsn(AnyUrl):
    allowed_schemes = {
        "bigquery",
    }


class BigQueryDatasource(SQLDatasource):
    """Adds a bigquery datasource to the data context.

    Uses the bigquery dialect of SQLAlchemy. For more information, see
    https://github.com/googleapis/python-bigquery-sqlalchemy

    Args:
        name: The name of this postgres datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres
            database.
            For example: "bigquery://my_project/my_dataset"
        assets: An optional list of TableAsset or QueryAsset.
    """

    type: Literal["bigquery"] = "bigquery"
    connection_string: ConfigUri | BigQueryDsn
