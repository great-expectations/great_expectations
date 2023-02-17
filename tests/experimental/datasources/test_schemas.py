import json
import sys
from typing import Any, Type

import pandas
import pydantic
import pytest

from great_expectations.experimental.datasources import (
    _PANDAS_SCHEMA_VERSION,  # this is the version we run in the standard test pipeline. Update as needed
    _SCHEMAS_DIR,
)
from great_expectations.experimental.datasources.sources import _SourceFactories

PANDAS_VERSION: str = pandas.__version__


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

    Note: if the installed version of pandas doesn't match the one used in the standard
    test pipeline, the test will be marked a `xfail` because the schemas will not match.
    """

    def _sort_required_lists(schema_as_dict: dict) -> None:
        """Sometimes "required" lists come unsorted, causing misleading assertion failures; this corrects the issue.

        Args:
            schema_as_dict: source dictionary (will be modified "in-situ")

        """
        key: str
        value: Any
        for key, value in schema_as_dict.items():
            if key == "required":
                schema_as_dict[key] = sorted(value)

            if isinstance(value, dict):
                _sort_required_lists(schema_as_dict=value)

    if _PANDAS_SCHEMA_VERSION != PANDAS_VERSION:
        pytest.xfail(reason=f"schema generated with pandas {_PANDAS_SCHEMA_VERSION}")

    print(f"python version: {sys.version.split()[0]}")
    print(f"pandas version: {PANDAS_VERSION}\n")

    schema_path = _SCHEMAS_DIR.joinpath(f"{zep_ds_or_asset_model.__name__}.json")

    # TODO: remove this logic and make this fail once all json schemas are working
    if schema_path.name in (
        "SqliteTableAsset.json",
        "SqliteQueryAsset.json",
        "SASAsset.json",
        "PandasSASAsset.json",
    ):
        pytest.xfail(f"{schema_path.name} does not exist")

    json_str = schema_path.read_text().rstrip()

    schema_as_dict = json.loads(json_str)
    _sort_required_lists(schema_as_dict=schema_as_dict)
    zep_ds_or_asset_model_as_dict = zep_ds_or_asset_model.schema()
    _sort_required_lists(schema_as_dict=zep_ds_or_asset_model_as_dict)

    assert (
        schema_as_dict == zep_ds_or_asset_model_as_dict
    ), "Schemas are out of sync. Run `invoke schema --sync`. Also check your pandas version."
