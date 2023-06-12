from typing import Literal, Optional, Union

import pydantic
from pydantic import AnyUrl

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel


class ConnectArgs(FluentBaseModel):
    iam: bool = True
    credentials_provider: Optional[str] = None
    idp_host: AnyUrl
    app_id: str
    app_name: str
    region: str
    cluster_identifier: str
    ssl_insecure: bool = False


class RedshiftDsn(pydantic.PostgresDsn):
    allowed_schemes = {
        "redshift",
        "redshift+redshift_connector",
        "redshift+psycopg2",
    }


@public_api
class Redshift(SQLDatasource):
    """Adds a postgres datasource to the data context.

    Args:
        name: The name of this postgres datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "redshift+redshift_connector://redshift:@localhost/test_database"
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """

    type: Literal["redshift"] = "redshift"  # type: ignore[assignment]
    connection_string: Union[ConfigStr, RedshiftDsn]
    connect_args: Optional[ConnectArgs] = pydantic.Field(
        None, description="Connect args for redshift"
    )
