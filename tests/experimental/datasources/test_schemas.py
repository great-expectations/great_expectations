import json
import sys
from typing import Type

import pandas
import pydantic
import pytest

from great_expectations.experimental.datasources import _SCHEMAS_DIR
from great_expectations.experimental.datasources.sources import _SourceFactories


@pytest.mark.unit
@pytest.mark.parametrize(
    "zep_ds_or_asset_model",
    [
        _SourceFactories.type_lookup[x]
        for x in _SourceFactories.type_lookup.type_names()
    ],
)
def test_vcs_schemas_match(zep_ds_or_asset_model: Type[pydantic.BaseModel]):
    """
    Test that json schemas for each DataSource and DataAsset match the current schema
    under version control.

    This is important because some of these classes are generated dynamically and
    monitoring these schemas files can clue us into problems that could otherwise go
    unnoticed.

    If this test is failing run `invoke schema --sync` to update schemas and commit the
    changes.
    """
    print(f"python version: {sys.version.split()[0]}")
    print(f"pandas version: {pandas.__version__}\n")

    schema_path = _SCHEMAS_DIR.joinpath(f"{zep_ds_or_asset_model.__name__}.json")
    print(schema_path.relative_to(schema_path.cwd()))

    # TODO: remove this logic and make this fail once all json schemas are working
    if schema_path.name in (
        "CSVAsset.json",
        "ExcelAsset.json",
        "PandasDatasource.json",
        "SqliteDatasource.json",
        "SqliteTableAsset.json",
    ):
        pytest.xfail(f"{schema_path.name} does not exist")

    json_str = schema_path.read_text().rstrip()

    assert (
        json.loads(json_str) == zep_ds_or_asset_model.schema()
    ), "Schemas are out of sync. Run `invoke schema --sync`. Also check your pandas version."
