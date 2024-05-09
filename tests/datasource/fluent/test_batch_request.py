from __future__ import annotations

import json
import re
from typing import Any, Final

import pytest

from great_expectations.core.batch_definition import PartitionerT
from great_expectations.core.partitioners import (
    ColumnPartitioner,
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
    FileNamePartitionerDaily,
    FileNamePartitionerMonthly,
    FileNamePartitionerYearly,
)
from great_expectations.datasource.fluent import BatchRequest


@pytest.mark.unit
@pytest.mark.parametrize(
    "optional_batch_request_config,parsed_batch_slice",
    [
        pytest.param(
            {
                "batch_slice": "[4:20:2]",
            },
            slice(4, 20, 2),
            id="no options, batch_slice: str",
        ),
        pytest.param(
            {
                "batch_slice": (1, 27, 3),
            },
            slice(1, 27, 3),
            id="no options, batch_slice: tuple",
        ),
        pytest.param(
            {
                "options": {"season": "summer"},
            },
            slice(0, None, None),
            id="no batch_slice, options: str",
        ),
        pytest.param(
            {
                "options": {"month": 7},
            },
            slice(0, None, None),
            id="no batch_slice, options: int",
        ),
        pytest.param(
            {
                "options": {"greeting": "hello"},
                "batch_slice": -1,
            },
            slice(-1, None, None),
            id="options: str, batch_slice: neg int",
        ),
        pytest.param(
            {
                "options": {"month": None},
                "batch_slice": "[1:4]",
            },
            slice(1, 4, None),
            id="options: None, batch_slice: str",
        ),
    ],
)
def test_batch_request_config_serialization_round_trips(
    optional_batch_request_config: dict, parsed_batch_slice: slice
) -> None:
    datasource_name: Final[str] = "my_datasource"
    data_asset_name: Final[str] = "my_data_asset"
    batch_request_config: dict[str, Any] = {
        "datasource_name": datasource_name,
        "data_asset_name": data_asset_name,
        "partitioner": ColumnPartitionerYearly(column_name="my_column"),
    }
    batch_request_config.update(optional_batch_request_config)
    batch_request = BatchRequest[ColumnPartitioner](**batch_request_config)
    assert batch_request.datasource_name == datasource_name
    assert batch_request.data_asset_name == data_asset_name
    # options is optional and an empty dict by default
    assert batch_request.options == batch_request_config.get("options", {})
    # BatchRequest always has a slice associated with it,
    # even if it is slice(0, None, None) (all elements in sequence).
    assert batch_request.batch_slice == parsed_batch_slice

    #
    # dict
    #
    batch_request_dict = batch_request.dict()
    assert batch_request_dict["datasource_name"] == datasource_name
    assert batch_request_dict["data_asset_name"] == data_asset_name
    # options is optional and an empty dict by default
    assert batch_request_dict["options"] == batch_request_config.get("options", {})
    # Even though BatchRequest.batch_slice has a slice object,
    # it will serialize to None since batch_slice wasn't passed in.
    assert batch_request_dict["batch_slice"] == batch_request_config.get("batch_slice", None)

    #
    # json
    #
    batch_request_json = batch_request.json()

    # convert options dict to json string
    options_json = json.dumps(batch_request_config.get("options", {}))

    # convert batch_slice value to json string
    batch_slice_json = json.dumps(batch_request_config.get("batch_slice", None))

    assert batch_request_json == (
        "{"
        f'"datasource_name": "{datasource_name}", '
        f'"data_asset_name": "{data_asset_name}", '
        f'"options": {options_json}, '
        '"partitioner": {'
        '"column_name": "my_column", '
        '"sort_ascending": true, '
        '"method_name": "partition_on_year"'
        "}, "
        f'"batch_slice": {batch_slice_json}'
        "}"
    )


def _partitioner_test_cases():
    return [
        pytest.param(
            ColumnPartitionerDaily(
                column_name="foo",
                sort_ascending=False,
            ),
            id="Sql Daily",
        ),
        pytest.param(
            ColumnPartitionerMonthly(
                column_name="foo",
                sort_ascending=False,
            ),
            id="Sql Monthly",
        ),
        pytest.param(
            ColumnPartitionerYearly(
                column_name="foo",
                sort_ascending=False,
            ),
            id="Sql Yearly",
        ),
        pytest.param(
            FileNamePartitionerDaily(
                regex=re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}).csv"),
                sort_ascending=False,
            ),
            id="Regex Daily",
        ),
        pytest.param(
            FileNamePartitionerMonthly(
                regex=re.compile(r"data_(?P<year>\d{4})-(?P<month>\d{2}).csv"),
                sort_ascending=False,
            ),
            id="Regex Monthly",
        ),
        pytest.param(
            FileNamePartitionerYearly(
                regex=re.compile(r"data_(?P<year>\d{4}).csv"),
                sort_ascending=False,
            ),
            id="Regex Yearly",
        ),
        pytest.param(
            None,
            id="None",
        ),
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "partitioner",
    _partitioner_test_cases(),
)
def test_batch_request_config_partitioner_dict_round_trip_serialization(
    partitioner: PartitionerT,
) -> None:
    # Using Any here effectively turns off type checking. This test demonstrates that the
    # BatchRequest doesn't need to be parameterized for Pydantic to correctly deserialize from dict
    batch_request = BatchRequest[Any](
        datasource_name="test-datasource", data_asset_name="test-asset", partitioner=partitioner
    )

    batch_request_dict = batch_request.dict()
    assert BatchRequest(**batch_request_dict) == batch_request


@pytest.mark.unit
@pytest.mark.parametrize(
    "partitioner",
    _partitioner_test_cases(),
)
def test_batch_request_config_partitioner_json_round_trip_serialization(
    partitioner: PartitionerT,
) -> None:
    # Using Any here effectively turns off type checking. This test demonstrates that the
    # BatchRequest doesn't need to be parameterized for Pydantic to correctly deserialize from JSON
    batch_request = BatchRequest[Any](
        datasource_name="test-datasource", data_asset_name="test-asset", partitioner=partitioner
    )

    batch_request_json = batch_request.json()
    assert BatchRequest.parse_raw(batch_request_json) == batch_request
