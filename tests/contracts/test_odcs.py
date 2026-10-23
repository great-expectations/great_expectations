from __future__ import annotations

import tempfile
from io import BytesIO, StringIO
from typing import Any, cast

import pytest

from great_expectations.contracts import (
    ODCSContractError,
    UnsupportedODCSRuleError,
    from_odcs,
)
from great_expectations.core.yaml_handler import YAMLHandler

pytestmark = pytest.mark.unit


def _property(name: str, logical_type: str, **extra):
    prop = {"name": name, "logicalType": logical_type}
    prop.update(extra)
    return prop


def _schema(properties, quality=None, name="orders"):
    schema = {"name": name, "logicalType": "object", "properties": properties}
    if quality is not None:
        schema["quality"] = quality
    return schema


def _contract(schemas=None, **overrides):
    contract = {
        "version": "1.0.0",
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "11111111-1111-1111-1111-111111111111",
        "status": "active",
        "name": "orders_contract",
        "schema": schemas or [_schema([_property("order_id", "integer")])],
    }
    contract.update(overrides)
    return contract


def _single(suite, expectation_type: str):
    matches = [
        expectation
        for expectation in suite.expectations
        if expectation.expectation_type == expectation_type
    ]
    assert len(matches) == 1, f"expected one {expectation_type}, got {matches}"
    return matches[0]


def test_structural_mapping() -> None:
    contract = _contract(
        [
            _schema(
                [
                    _property(
                        "order_id", "integer", required=True, primaryKey=True, physicalType="int64"
                    ),
                    _property(
                        "code",
                        "string",
                        logicalTypeOptions={
                            "pattern": "^[A-Z]{2}$",
                            "minLength": 2,
                            "maxLength": 2,
                        },
                    ),
                    _property("email", "string", logicalTypeOptions={"format": "email"}),
                    _property(
                        "amount",
                        "number",
                        logicalTypeOptions={"minimum": 0, "maximum": 100, "exclusiveMaximum": 100},
                    ),
                ]
            )
        ]
    )

    suite = from_odcs(contract)

    assert suite.name == "orders"
    columns = _single(suite, "expect_table_columns_to_match_set")
    assert columns.column_set == ["order_id", "code", "email", "amount"]
    assert columns.exact_match is True

    null_checks = [
        expectation
        for expectation in suite.expectations
        if expectation.expectation_type == "expect_column_values_to_not_be_null"
    ]
    assert [(cast("Any", expectation).column,) for expectation in null_checks] == [("order_id",)]

    unique = _single(suite, "expect_column_values_to_be_unique")
    assert unique.column == "order_id"

    code_regexes = [
        expectation
        for expectation in suite.expectations
        if expectation.expectation_type == "expect_column_values_to_match_regex"
    ]
    assert {
        cast("Any", expectation).column: cast("Any", expectation).regex
        for expectation in code_regexes
    } == {
        "code": "^[A-Z]{2}$",
        "email": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
    }

    lengths = _single(suite, "expect_column_value_lengths_to_be_between")
    assert lengths.column == "code"
    assert lengths.min_value == 2
    assert lengths.max_value == 2

    amount = _single(suite, "expect_column_values_to_be_between")
    assert amount.column == "amount"
    assert amount.min_value == 0
    assert amount.max_value == 100
    assert amount.strict_min is False
    assert amount.strict_max is True

    type_check = _single(suite, "expect_column_values_to_be_of_type")
    assert type_check.column == "order_id"
    assert type_check.type_ == "int64"


def test_type_check_is_opt_in_without_physical_type_or_type_map() -> None:
    suite = from_odcs(_contract([_schema([_property("order_id", "integer")])]))
    assert not [
        expectation
        for expectation in suite.expectations
        if expectation.expectation_type == "expect_column_values_to_be_of_type"
    ]


def test_type_map_overrides_physical_type() -> None:
    suite = from_odcs(
        _contract([_schema([_property("order_id", "integer", physicalType="INTEGER")])]),
        type_map={"integer": "int64"},
    )
    type_check = _single(suite, "expect_column_values_to_be_of_type")
    assert type_check.type_ == "int64"


@pytest.mark.parametrize(
    "type_map",
    [{"integer": ""}, {"not-a-logical-type": "int64"}],
)
def test_invalid_type_map_value_is_rejected(type_map: dict[str, str]) -> None:
    with pytest.raises(ODCSContractError, match="type_map"):
        from_odcs(
            _contract([_schema([_property("order_id", "integer")])]),
            type_map=type_map,
        )


def test_physical_type_error_mentions_the_property_path() -> None:
    with pytest.raises(ODCSContractError, match=r"schema\[0\]\.properties\[0\]\.physicalType"):
        from_odcs(
            _contract([_schema([_property("order_id", "integer", physicalType="")])]),
        )


def test_suite_and_expectation_metadata() -> None:
    contract = _contract(
        [
            _schema(
                [
                    _property(
                        "order_id",
                        "integer",
                        quality=[{"id": "pk-not-null", "metric": "nullValues", "mustBe": 0}],
                    )
                ],
                quality=[{"id": "row-count", "metric": "rowCount", "mustBe": 10}],
            )
        ]
    )

    suite = from_odcs(contract)

    assert suite.meta["odcs"] == {
        "contract_id": "11111111-1111-1111-1111-111111111111",
        "contract_name": "orders_contract",
        "contract_version": "1.0.0",
        "api_version": "v3.1.0",
        "schema": "orders",
        "ignored": [],
    }
    row_count = _single(suite, "expect_table_row_count_to_be_between")
    assert row_count.meta["odcs"]["path"] == "schema[0].quality[0]"
    assert row_count.meta["odcs"]["rule_id"] == "row-count"
    assert row_count.meta["odcs"]["metric"] == "rowCount"

    not_null = _single(suite, "expect_column_values_to_not_be_null")
    assert not_null.meta["odcs"]["path"] == "schema[0].properties[0].quality[0]"
    assert not_null.meta["odcs"]["rule_id"] == "pk-not-null"


def test_duplicate_rules_keep_every_source_in_metadata() -> None:
    contract = _contract(
        [
            _schema(
                [_property("id", "integer")],
                quality=[
                    {"id": "first-count", "metric": "rowCount", "mustBe": 10},
                    {"id": "second-count", "metric": "rowCount", "mustBe": 10},
                ],
            )
        ]
    )

    suite = from_odcs(contract)

    row_count = _single(suite, "expect_table_row_count_to_be_between")
    assert row_count.meta["odcs"]["rule_id"] == "first-count"
    assert row_count.meta["odcs"]["sources"] == [
        {
            "contract_id": "11111111-1111-1111-1111-111111111111",
            "path": "schema[0].quality[0]",
            "rule_id": "first-count",
            "metric": "rowCount",
        },
        {
            "contract_id": "11111111-1111-1111-1111-111111111111",
            "path": "schema[0].quality[1]",
            "rule_id": "second-count",
            "metric": "rowCount",
        },
    ]


def test_primary_key_columns_are_ordered_by_position() -> None:
    contract = _contract(
        [
            _schema(
                [
                    _property("order_id", "integer", primaryKey=True, primaryKeyPosition=3),
                    _property("tenant_id", "integer", primaryKey=True, primaryKeyPosition=1),
                    _property("region", "string", primaryKey=True, primaryKeyPosition=2),
                ]
            )
        ]
    )

    suite = from_odcs(contract)

    compound = _single(suite, "expect_compound_columns_to_be_unique")
    assert compound.column_list == ["tenant_id", "region", "order_id"]
    not_null_columns = {
        cast("Any", expectation).column
        for expectation in suite.expectations
        if expectation.expectation_type == "expect_column_values_to_not_be_null"
    }
    assert not_null_columns == {"order_id", "tenant_id", "region"}


def test_duplicate_primary_key_positions_are_rejected() -> None:
    contract = _contract(
        [
            _schema(
                [
                    _property("a", "integer", primaryKey=True, primaryKeyPosition=1),
                    _property("b", "integer", primaryKey=True, primaryKeyPosition=1),
                ]
            )
        ]
    )

    with pytest.raises(ODCSContractError, match="primaryKeyPosition"):
        from_odcs(contract)


def test_single_primary_key_is_unique() -> None:
    suite = from_odcs(_contract([_schema([_property("order_id", "integer", primaryKey=True)])]))
    unique = _single(suite, "expect_column_values_to_be_unique")
    assert unique.column == "order_id"


def test_missing_and_minus_one_primary_key_positions_are_assigned_after_explicit_ones() -> None:
    contract = _contract(
        [
            _schema(
                [
                    _property("first", "integer", primaryKey=True),
                    _property("second", "integer", primaryKey=True, primaryKeyPosition=1),
                    _property("third", "integer", primaryKey=True, primaryKeyPosition=-1),
                ]
            )
        ]
    )

    suite = from_odcs(contract)

    compound = _single(suite, "expect_compound_columns_to_be_unique")
    assert compound.column_list == ["second", "first", "third"]


def test_required_and_primary_key_share_one_not_null_expectation() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "order_id",
                            "integer",
                            required=True,
                            primaryKey=True,
                            quality=[{"metric": "nullValues", "mustBe": 0}],
                        )
                    ]
                )
            ]
        )
    )
    not_null = _single(suite, "expect_column_values_to_not_be_null")
    assert not_null.column == "order_id"


@pytest.mark.parametrize(
    ("operator", "value", "expected"),
    [
        ("mustBe", 7, (7, 7, False, False)),
        ("mustBeBetween", [1, 10], (1, 10, False, False)),
        ("mustBeGreaterThan", 0, (0, None, True, False)),
        ("mustBeGreaterOrEqualTo", 0, (0, None, False, False)),
        ("mustBeLessThan", 10, (None, 10, False, True)),
        ("mustBeLessOrEqualTo", 10, (None, 10, False, False)),
    ],
)
def test_row_count_operators(operator: str, value, expected) -> None:
    expected_min, expected_max, strict_min, strict_max = expected
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [_property("id", "integer")], quality=[{"metric": "rowCount", operator: value}]
                )
            ]
        )
    )
    row_count = _single(suite, "expect_table_row_count_to_be_between")
    assert row_count.min_value == expected_min
    assert row_count.max_value == expected_max
    assert row_count.strict_min is strict_min
    assert row_count.strict_max is strict_max


def test_row_count_percent_is_unsupported() -> None:
    with pytest.raises(UnsupportedODCSRuleError, match="rowCount"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[{"metric": "rowCount", "mustBe": 1, "unit": "percent"}],
                    )
                ]
            )
        )


def test_null_values_exact_zero_maps_to_not_null() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "email",
                            "string",
                            quality=[{"metric": "nullValues", "mustBe": 0}],
                        )
                    ]
                )
            ]
        )
    )
    expectation = _single(suite, "expect_column_values_to_not_be_null")
    assert expectation.column == "email"
    assert expectation.mostly == 1


def test_null_values_percent_maps_to_mostly() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "email",
                            "string",
                            quality=[
                                {
                                    "metric": "nullValues",
                                    "mustBeLessThan": 2,
                                    "unit": "percent",
                                }
                            ],
                        )
                    ]
                )
            ]
        )
    )
    expectation = _single(suite, "expect_column_values_to_not_be_null")
    assert expectation.mostly > 0.98
    assert expectation.mostly == pytest.approx(0.98)


def test_null_values_exact_non_zero_percent_is_unsupported() -> None:
    with pytest.raises(UnsupportedODCSRuleError, match="percentage threshold"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [
                            _property(
                                "email",
                                "string",
                                quality=[
                                    {
                                        "metric": "nullValues",
                                        "mustBe": 5,
                                        "unit": "percent",
                                    }
                                ],
                            )
                        ]
                    )
                ]
            )
        )


def test_null_values_rows_threshold_is_unsupported() -> None:
    with pytest.raises(UnsupportedODCSRuleError, match="nullValues"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [
                            _property(
                                "email",
                                "string",
                                quality=[{"metric": "nullValues", "mustBeLessThan": 2}],
                            )
                        ]
                    )
                ]
            )
        )


def test_must_be_between_zero_is_an_exact_zero_threshold() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "email",
                            "string",
                            quality=[
                                {
                                    "metric": "nullValues",
                                    "mustBeBetween": [0, 0],
                                }
                            ],
                        ),
                        _property(
                            "code",
                            "string",
                            quality=[
                                {
                                    "metric": "invalidValues",
                                    "arguments": {"pattern": "^[A-Z]+$"},
                                    "mustBeBetween": [0, 0],
                                }
                            ],
                        ),
                        _property(
                            "nickname",
                            "string",
                            quality=[
                                {
                                    "metric": "missingValues",
                                    "arguments": {"missingValues": [None]},
                                    "mustBeBetween": [0, 0],
                                }
                            ],
                        ),
                        _property(
                            "backup_email",
                            "string",
                            quality=[
                                {
                                    "metric": "duplicateValues",
                                    "mustBeBetween": [0, 0],
                                }
                            ],
                        ),
                    ]
                )
            ]
        )
    )

    assert {expectation.expectation_type for expectation in suite.expectations} >= {
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_match_regex",
        "expect_column_values_to_be_unique",
    }


def test_missing_values_maps_null_and_sentinels() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "email",
                            "string",
                            quality=[
                                {
                                    "metric": "missingValues",
                                    "arguments": {"missingValues": [None, "", "N/A"]},
                                    "mustBe": 0,
                                }
                            ],
                        )
                    ]
                )
            ]
        )
    )
    assert _single(suite, "expect_column_values_to_not_be_null").column == "email"
    not_in_set = _single(suite, "expect_column_values_to_not_be_in_set")
    assert not_in_set.column == "email"
    assert not_in_set.value_set == ["", "N/A"]


def test_missing_values_non_zero_is_unsupported() -> None:
    with pytest.raises(UnsupportedODCSRuleError, match="missingValues"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [
                            _property(
                                "email",
                                "string",
                                quality=[
                                    {
                                        "metric": "missingValues",
                                        "arguments": {"missingValues": [None]},
                                        "mustBe": 1,
                                    }
                                ],
                            )
                        ]
                    )
                ]
            )
        )


def test_invalid_values_pattern() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "email",
                            "string",
                            quality=[
                                {
                                    "metric": "invalidValues",
                                    "arguments": {"pattern": "^[^@]+@[^@]+$"},
                                    "mustBe": 0,
                                }
                            ],
                        )
                    ]
                )
            ]
        )
    )
    regex = _single(suite, "expect_column_values_to_match_regex")
    assert regex.column == "email"
    assert regex.regex == "^[^@]+@[^@]+$"


def test_invalid_values_valid_values_with_percent() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "currency",
                            "string",
                            quality=[
                                {
                                    "metric": "invalidValues",
                                    "arguments": {"validValues": ["USD", "EUR"]},
                                    "mustBeLessThan": 5,
                                    "unit": "percent",
                                }
                            ],
                        )
                    ]
                )
            ]
        )
    )
    in_set = _single(suite, "expect_column_values_to_be_in_set")
    assert in_set.column == "currency"
    assert in_set.value_set == ["USD", "EUR"]
    assert in_set.mostly == pytest.approx(0.95)


def test_invalid_values_requires_one_target() -> None:
    with pytest.raises(ODCSContractError, match="exactly one of pattern or validValues"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [
                            _property(
                                "email",
                                "string",
                                quality=[
                                    {
                                        "metric": "invalidValues",
                                        "arguments": {},
                                        "mustBe": 0,
                                    }
                                ],
                            )
                        ]
                    )
                ]
            )
        )


def test_property_duplicate_values_maps_to_unique() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "email",
                            "string",
                            quality=[{"metric": "duplicateValues", "mustBe": 0}],
                        )
                    ]
                )
            ]
        )
    )
    assert _single(suite, "expect_column_values_to_be_unique").column == "email"


def test_schema_duplicate_values_maps_to_compound_unique() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [_property("tenant_id", "integer"), _property("order_id", "integer")],
                    quality=[
                        {
                            "metric": "duplicateValues",
                            "arguments": {"properties": ["tenant_id", "order_id"]},
                            "mustBe": 0,
                        }
                    ],
                )
            ]
        )
    )
    compound = _single(suite, "expect_compound_columns_to_be_unique")
    assert compound.column_list == ["tenant_id", "order_id"]


def test_schema_duplicate_values_unknown_column_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="unknown column"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("tenant_id", "integer")],
                        quality=[
                            {
                                "metric": "duplicateValues",
                                "arguments": {"properties": ["missing"]},
                                "mustBe": 0,
                            }
                        ],
                    )
                ]
            )
        )


def test_duplicate_values_percent_maps_to_mostly() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [_property("email", "string")],
                    quality=[
                        {
                            "metric": "duplicateValues",
                            "arguments": {"properties": ["email"]},
                            "mustBeLessThan": 1,
                            "unit": "percent",
                        }
                    ],
                )
            ]
        )
    )
    unique = _single(suite, "expect_column_values_to_be_unique")
    assert unique.column == "email"
    assert unique.mostly == pytest.approx(0.99)


def test_duplicate_values_row_threshold_is_unsupported() -> None:
    with pytest.raises(UnsupportedODCSRuleError, match="duplicateValues"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("email", "string")],
                        quality=[
                            {
                                "metric": "duplicateValues",
                                "arguments": {"properties": ["email"]},
                                "mustBeLessThan": 10,
                                "unit": "rows",
                            }
                        ],
                    )
                ]
            )
        )


def test_must_not_be_is_unsupported() -> None:
    with pytest.raises(UnsupportedODCSRuleError, match="only supports"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [
                            _property(
                                "email",
                                "string",
                                quality=[
                                    {
                                        "metric": "nullValues",
                                        "mustNotBe": 0,
                                    }
                                ],
                            )
                        ]
                    )
                ]
            )
        )


def test_unknown_operator_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="exactly one operator"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("email", "string")],
                        quality=[{"metric": "nullValues"}],
                    )
                ]
            )
        )


def test_old_operator_names_are_rejected_with_the_correct_name() -> None:
    with pytest.raises(ODCSContractError, match="mustBeGreaterOrEqualTo"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[{"metric": "rowCount", "mustBeGreaterThanOrEqualTo": 1}],
                    )
                ]
            )
        )


@pytest.mark.parametrize("field", ["version", "apiVersion", "kind", "id", "status"])
def test_required_root_fields_are_enforced(field: str) -> None:
    contract = _contract()
    del contract[field]
    with pytest.raises(ODCSContractError, match=field):
        from_odcs(contract)


def test_legacy_root_shape_is_rejected() -> None:
    contract = _contract()
    contract["dataset"] = {"name": "orders", "columns": []}
    with pytest.raises(ODCSContractError, match="schema"):
        from_odcs(contract)


def test_wrong_contract_kind_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="kind"):
        from_odcs(_contract(kind="NotADataContract"))


@pytest.mark.parametrize("api_version", ["v2.0.0", "v3.0.0", "v3.2.0"])
def test_unsupported_api_version_is_rejected(api_version: str) -> None:
    with pytest.raises(ODCSContractError, match="apiVersion"):
        from_odcs(_contract(apiVersion=api_version))


def test_schema_must_be_a_non_empty_list() -> None:
    with pytest.raises(ODCSContractError, match="schema"):
        from_odcs(_contract(schema=[]))


def test_schema_object_requires_properties() -> None:
    with pytest.raises(ODCSContractError, match="properties"):
        from_odcs(_contract([{"name": "orders", "logicalType": "object"}]))


def test_schema_object_name_must_be_unique() -> None:
    schema = _schema([_property("id", "integer")])
    with pytest.raises(ODCSContractError, match="Duplicate"):
        from_odcs(_contract([schema, schema]))


def test_invalid_logical_type_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="logicalType"):
        from_odcs(_contract([_schema([_property("id", "varchar")])]))


def test_logical_type_is_optional() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        {"name": "id", "logicalTypeOptions": {"format": "email"}},
                    ]
                )
            ]
        )
    )

    assert not [
        expectation
        for expectation in suite.expectations
        if expectation.expectation_type == "expect_column_values_to_be_of_type"
    ]
    assert {
        "path": "schema[0].properties[0]",
        "reason": "logicalTypeOptions are not mapped without logicalType",
    } in suite.meta["odcs"]["ignored"]


def test_logical_type_options_must_be_a_mapping() -> None:
    with pytest.raises(ODCSContractError, match="logicalTypeOptions"):
        from_odcs(_contract([_schema([_property("id", "integer", logicalTypeOptions=[])])]))


def test_quality_must_be_a_list() -> None:
    with pytest.raises(ODCSContractError, match="quality"):
        from_odcs(_contract([_schema([_property("id", "integer")], quality={})]))


def test_quality_rule_must_be_a_mapping() -> None:
    with pytest.raises(ODCSContractError, match="mapping"):
        from_odcs(_contract([_schema([_property("id", "integer")], quality=["not-a-rule"])]))


def test_multiple_schemas_require_selection() -> None:
    contract = _contract(
        [
            _schema([_property("id", "integer")], name="orders"),
            _schema([_property("customer_id", "integer")], name="customers"),
        ]
    )
    with pytest.raises(ODCSContractError, match="orders, customers"):
        from_odcs(contract)

    suite = from_odcs(contract, schema="customers")
    assert suite.name == "customers"
    assert _single(suite, "expect_table_columns_to_match_set").column_set == ["customer_id"]


def test_unknown_schema_selection_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="not found"):
        from_odcs(_contract(), schema="missing")


def test_suite_name_override() -> None:
    suite = from_odcs(_contract(), suite_name="my_suite")
    assert suite.name == "my_suite"


def test_yaml_string_path_and_stream_sources(tmp_path) -> None:
    contract = _contract()
    yaml_text = YAMLHandler().dump(contract)
    assert yaml_text is not None

    assert from_odcs(yaml_text).name == "orders"

    path = tmp_path / "contract.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    assert from_odcs(path).name == "orders"
    assert from_odcs(str(path)).name == "orders"

    assert from_odcs(StringIO(yaml_text)).name == "orders"
    assert from_odcs(BytesIO(yaml_text.encode("utf-8"))).name == "orders"

    with tempfile.TemporaryFile("w+", encoding="utf-8") as stream:
        stream.write(yaml_text)
        stream.seek(0)
        assert from_odcs(cast("Any", stream)).name == "orders"

    hash_path = tmp_path / "contract#1.yaml"
    hash_path.write_text(yaml_text, encoding="utf-8")
    assert from_odcs(str(hash_path)).name == "orders"


def test_unsupported_source_type_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="source must be"):
        from_odcs(cast("Any", 42))


def test_invalid_yaml_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="Invalid YAML"):
        from_odcs("foo: [unclosed")


def test_non_mapping_yaml_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="mapping"):
        from_odcs("- one\n- two\n")


def test_sql_rules_are_rejected() -> None:
    with pytest.raises(UnsupportedODCSRuleError, match="SQL"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[
                            {
                                "type": "sql",
                                "query": "SELECT count(*) FROM orders",
                                "mustBe": 1,
                            }
                        ],
                    )
                ]
            )
        )


def test_text_rules_are_recorded_as_ignored() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [_property("id", "integer")],
                    quality=[{"type": "text", "description": "validated by a human"}],
                )
            ]
        )
    )
    assert suite.meta["odcs"]["ignored"] == [
        {
            "path": "schema[0].quality[0]",
            "reason": "text rules are descriptive and not executable",
        }
    ]


def test_unknown_metric_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="unknown library metric"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[{"metric": "freshness", "mustBe": 0}],
                    )
                ]
            )
        )


def test_unknown_rule_type_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="unknown quality rule type"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[{"type": "python", "mustBe": 0}],
                    )
                ]
            )
        )


def test_custom_gx_rule_maps_to_registered_expectation() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [_property("id", "integer")],
                    quality=[
                        {
                            "type": "custom",
                            "engine": "greatExpectations",
                            "implementation": {
                                "type": "expect_table_row_count_to_be_between",
                                "kwargs": {"minValue": 1, "maxValue": 10},
                            },
                        }
                    ],
                )
            ]
        )
    )
    row_count = _single(suite, "expect_table_row_count_to_be_between")
    assert row_count.min_value == 1
    assert row_count.max_value == 10


def test_custom_gx_rule_accepts_yaml_implementation() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [_property("id", "integer")],
                    quality=[
                        {
                            "type": "custom",
                            "engine": "gx",
                            "implementation": (
                                "type: expect_column_values_to_not_be_null\nkwargs:\n  column: id\n"
                            ),
                        }
                    ],
                )
            ]
        )
    )
    assert _single(suite, "expect_column_values_to_not_be_null").column == "id"


def test_property_custom_rule_injects_column_and_accepts_type_name_shorthand() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "email",
                            "string",
                            quality=[
                                {
                                    "type": "custom",
                                    "engine": "gx",
                                    "implementation": "expect_column_values_to_not_be_null",
                                }
                            ],
                        )
                    ]
                )
            ]
        )
    )

    not_null = _single(suite, "expect_column_values_to_not_be_null")
    assert not_null.column == "email"


def test_custom_gx_rule_rejects_unknown_expectation() -> None:
    with pytest.raises(ODCSContractError, match="not registered"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[
                            {
                                "type": "custom",
                                "engine": "gx",
                                "implementation": {"type": "expect_nope", "kwargs": {}},
                            }
                        ],
                    )
                ]
            )
        )


def test_custom_gx_rule_rejects_invalid_kwargs() -> None:
    with pytest.raises(ODCSContractError, match="could not build"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[
                            {
                                "type": "custom",
                                "engine": "gx",
                                "implementation": {
                                    "type": "expect_column_values_to_not_be_null",
                                    "kwargs": {"column": "id", "not_a_field": True},
                                },
                            }
                        ],
                    )
                ]
            )
        )


def test_custom_rule_without_engine_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="engine"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [_property("id", "integer")],
                        quality=[
                            {
                                "type": "custom",
                                "implementation": {"type": "expect_table_row_count_to_be_between"},
                            }
                        ],
                    )
                ]
            )
        )


def test_foreign_custom_rule_is_recorded_as_ignored() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [_property("id", "integer")],
                    quality=[
                        {
                            "type": "custom",
                            "engine": "soda",
                            "implementation": "checks for orders:\n  - row_count > 0\n",
                        }
                    ],
                )
            ]
        )
    )
    assert suite.meta["odcs"]["ignored"] == [
        {
            "path": "schema[0].quality[0]",
            "reason": "custom rule targets engine 'soda'",
        }
    ]


def test_unmapped_property_options_and_relationships_are_recorded() -> None:
    schema = _schema(
        [
            _property(
                "payload",
                "object",
                properties=[
                    _property(
                        "a",
                        "string",
                        quality=[{"metric": "nullValues", "mustBe": 0}],
                    )
                ],
                logicalTypeOptions={"maxProperties": 3},
            ),
            _property(
                "tags",
                "array",
                items={
                    "logicalType": "string",
                    "quality": [{"metric": "nullValues", "mustBe": 0}],
                },
                logicalTypeOptions={"minItems": 1},
            ),
            _property(
                "amount",
                "number",
                logicalTypeOptions={"minimum": 0, "multipleOf": 0.01},
                relationships=[{"from": "orders", "to": "customers"}],
            ),
            _property(
                "created_at",
                "timestamp",
                logicalTypeOptions={"timezone": "UTC"},
            ),
        ]
    )
    schema["relationships"] = [{"from": "orders", "to": "customers"}]

    suite = from_odcs(_contract([schema]))

    ignored = suite.meta["odcs"]["ignored"]
    reasons = [item["reason"] for item in ignored]
    assert "nested object properties are not mapped" in reasons
    assert "array items are not mapped" in reasons
    assert "nested quality rules are not mapped" in reasons
    assert "object constraints are not mapped" in reasons
    assert "array constraints are not mapped" in reasons
    assert "multipleOf constraints are not mapped" in reasons
    assert "relationships are not mapped" in reasons
    assert "property relationships are not mapped" in reasons
    assert "timezone constraints are not mapped" in reasons
    assert {
        "path": "schema[0].properties[0].properties[0].quality[0]",
        "reason": "nested quality rules are not mapped",
    } in ignored
    assert {
        "path": "schema[0].properties[1].items.quality[0]",
        "reason": "nested quality rules are not mapped",
    } in ignored


def test_zero_bounds_are_not_dropped() -> None:
    suite = from_odcs(
        _contract(
            [
                _schema(
                    [
                        _property(
                            "amount",
                            "number",
                            logicalTypeOptions={"minimum": 0, "maximum": 0},
                        )
                    ]
                )
            ]
        )
    )
    expectation = _single(suite, "expect_column_values_to_be_between")
    assert expectation.min_value == 0
    assert expectation.max_value == 0


def test_unsupported_missing_values_shape_is_rejected() -> None:
    with pytest.raises(ODCSContractError, match="missingValues"):
        from_odcs(
            _contract(
                [
                    _schema(
                        [
                            _property(
                                "email",
                                "string",
                                quality=[
                                    {
                                        "metric": "missingValues",
                                        "arguments": {"missingValues": []},
                                        "mustBe": 0,
                                    }
                                ],
                            )
                        ]
                    )
                ]
            )
        )
