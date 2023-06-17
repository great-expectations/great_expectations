from __future__ import annotations

import json
import pathlib
import sys
from pprint import pformat as pf
from typing import Any, Generator, Type

import pandas
import pytest

from great_expectations.datasource.fluent import (
    _PANDAS_SCHEMA_VERSION,  # this is the version we run in the standard test pipeline. Update as needed
    _SCHEMAS_DIR,
    DataAsset,
    Datasource,
)
from great_expectations.datasource.fluent.sources import (
    _iter_all_registered_types,
)

PANDAS_VERSION: str = pandas.__version__


def _models_and_schema_dirs() -> (
    Generator[tuple[Type[Datasource | DataAsset], pathlib.Path, str], None, None]
):
    datasource: Type[Datasource] = Datasource
    ds_type_name: str = ""

    yield Datasource, _SCHEMAS_DIR, Datasource.__name__

    for name, model in _iter_all_registered_types():
        if issubclass(model, Datasource):
            datasource = model
            ds_type_name = name
            schema_dir = _SCHEMAS_DIR
        else:
            schema_dir = _SCHEMAS_DIR / datasource.__name__

        yield model, schema_dir, f"{ds_type_name}:{model.__name__}"


@pytest.mark.unit
@pytest.mark.parametrize(
    ["fluent_ds_or_asset_model", "schema_dir"],
    [pytest.param(t[0], t[1], id=t[2]) for t in _models_and_schema_dirs()],
)
def test_vcs_schemas_match(
    fluent_ds_or_asset_model: Type[Datasource | DataAsset], schema_dir: pathlib.Path
):
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

    def _sort_any_of(d: dict) -> str:
        if items := d.get("items"):
            _sort_any_of(items)
        if ref := d.get("$ref"):
            return ref
        if type_ := d.get("type"):
            return type_
        # return any string for sorting
        return "z"

    def _sort_lists(schema_as_dict: dict) -> None:
        """Sometimes "required" and "anyOf" lists come unsorted, causing misleading assertion failures; this corrects the issue.

        Args:
            schema_as_dict: source dictionary (will be modified "in-situ")

        """
        key: str
        value: Any

        for key, value in schema_as_dict.items():
            if key == "required":
                schema_as_dict[key] = sorted(value)
            elif key == "anyOf":
                if isinstance(value, list):
                    for val in value:
                        if isinstance(value, dict):
                            schema_as_dict[key] = sorted(val, key=_sort_any_of)
                else:
                    schema_as_dict[key] = sorted(value, key=_sort_any_of)

            if isinstance(value, dict):
                _sort_lists(schema_as_dict=value)

    if "Pandas" in str(schema_dir) and _PANDAS_SCHEMA_VERSION != PANDAS_VERSION:
        pytest.xfail(reason=f"schema generated with pandas {_PANDAS_SCHEMA_VERSION}")

    print(f"python version: {sys.version.split()[0]}")
    print(f"pandas version: {PANDAS_VERSION}\n")

    schema_path = schema_dir.joinpath(f"{fluent_ds_or_asset_model.__name__}.json")
    print(schema_path)

    json_str = schema_path.read_text().rstrip()

    schema_as_dict = json.loads(json_str)
    _sort_lists(schema_as_dict=schema_as_dict)
    fluent_ds_or_asset_model_as_dict = fluent_ds_or_asset_model.schema()
    _sort_lists(schema_as_dict=fluent_ds_or_asset_model_as_dict)

    if "Excel" in str(schema_path):
        pytest.xfail(reason="Sorting of nested anyOf key")

    assert (
        schema_as_dict == fluent_ds_or_asset_model_as_dict
    ), "Schemas are out of sync. Run `invoke schema --sync`. Also check your pandas version."


@pytest.mark.skipif(
    _PANDAS_SCHEMA_VERSION != PANDAS_VERSION,
    reason=f"schemas generated with pandas {_PANDAS_SCHEMA_VERSION}",
)
def test_no_orphaned_schemas():
    """Ensure that there are no schemas that have no corresponding type."""
    print(f"python version: {sys.version.split()[0]}")
    print(f"pandas version: {PANDAS_VERSION}\n")

    # NOTE: this is a very low fidelity check
    all_schemas: set[str] = {t[1].__name__ for t in _iter_all_registered_types()}
    all_schemas.add("BatchRequest")
    all_schemas.add(Datasource.__name__)

    orphans: list[pathlib.Path] = []

    for schema in _SCHEMAS_DIR.glob("**/*.json"):
        if schema.stem not in all_schemas:
            orphans.append(schema)

    assert (
        not orphans
    ), f"The following schemas appear to be orphaned and should be removed. Run `invoke schema --sync --clean`\n{pf(orphans)}"
