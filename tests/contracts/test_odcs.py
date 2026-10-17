from __future__ import annotations

import io
from pathlib import Path

import pytest

import great_expectations as gx
from great_expectations.contracts.odcs import from_odcs
from great_expectations.expectations.core.expect_column_max_to_be_between import (
    ExpectColumnMaxToBeBetween,
)
from great_expectations.expectations.core.expect_column_values_to_be_of_type import (
    ExpectColumnValuesToBeOfType,
)
from great_expectations.expectations.core.expect_column_values_to_be_unique import (
    ExpectColumnValuesToBeUnique,
)
from great_expectations.expectations.core.expect_column_values_to_match_regex import (
    ExpectColumnValuesToMatchRegex,
)
from great_expectations.expectations.core.expect_column_values_to_not_be_null import (
    ExpectColumnValuesToNotBeNull,
)
from great_expectations.expectations.core.expect_compound_columns_to_be_unique import (
    ExpectCompoundColumnsToBeUnique,
)
from great_expectations.expectations.core.expect_table_columns_to_match_set import (
    ExpectTableColumnsToMatchSet,
)
from great_expectations.expectations.core.expect_table_row_count_to_be_between import (
    ExpectTableRowCountToBeBetween,
)

SAMPLE_ORDERS_ODCS_YAML = """
apiVersion: v3.1.0
kind: DataContract
dataset:
  name: orders
  columns:
    - name: order_id
      type: integer
      required: true
    - name: customer_email
      type: string
      format: email
    - name: order_date
      type: timestamp
    - name: total_amount
      type: float
quality:
  - type: row_count
    mustBeBetween: [100, 1000]
  - type: duplicate
    columns: [order_id, order_date]
  - type: freshness
    column: order_date
    min_value: '2024-01-01'
    max_value: '2025-01-01'
"""


@pytest.mark.unit
def test_from_odcs_all_mappings() -> None:
    suite = from_odcs(SAMPLE_ORDERS_ODCS_YAML)

    # 1. dataset.name -> Asset / Suite name
    assert suite.name == "orders"

    exp_types = [e.expectation_type for e in suite.expectations]

    # 2. column name + type -> ExpectTableColumnsToMatchSet, ExpectColumnValuesToBeOfType
    assert "expect_table_columns_to_match_set" in exp_types
    col_match_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectTableColumnsToMatchSet)
    )
    assert set(col_match_exp.column_set) == {
        "order_id",
        "customer_email",
        "order_date",
        "total_amount",
    }

    type_exps = [e for e in suite.expectations if isinstance(e, ExpectColumnValuesToBeOfType)]
    assert len(type_exps) == 4
    type_map = {e.column: e.type_ for e in type_exps}
    assert type_map["order_id"] == "integer"
    assert type_map["customer_email"] == "string"
    assert type_map["order_date"] == "timestamp"
    assert type_map["total_amount"] == "float"

    # 3. quality.row_count -> ExpectTableRowCountToBeBetween
    assert "expect_table_row_count_to_be_between" in exp_types
    row_count_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectTableRowCountToBeBetween)
    )
    assert row_count_exp.min_value == 100
    assert row_count_exp.max_value == 1000

    # 4. quality.missing -> ExpectColumnValuesToNotBeNull
    assert "expect_column_values_to_not_be_null" in exp_types
    not_null_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectColumnValuesToNotBeNull)
    )
    assert not_null_exp.column == "order_id"

    # 5. quality.invalid -> ExpectColumnValuesToMatchRegex
    assert "expect_column_values_to_match_regex" in exp_types
    regex_exp = next(e for e in suite.expectations if isinstance(e, ExpectColumnValuesToMatchRegex))
    assert regex_exp.column == "customer_email"
    assert "@" in regex_exp.regex

    # 6. quality.duplicate -> ExpectCompoundColumnsToBeUnique
    assert "expect_compound_columns_to_be_unique" in exp_types
    compound_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectCompoundColumnsToBeUnique)
    )
    assert list(compound_exp.column_list) == ["order_id", "order_date"]

    # 7. quality.freshness -> ExpectColumnMaxToBeBetween (with a literal bound)
    assert "expect_column_max_to_be_between" in exp_types
    freshness_exp = next(e for e in suite.expectations if isinstance(e, ExpectColumnMaxToBeBetween))
    assert freshness_exp.column == "order_date"
    assert freshness_exp.min_value is not None
    assert freshness_exp.max_value is not None


@pytest.mark.unit
def test_from_odcs_exposed_on_gx_namespace() -> None:
    suite = gx.contracts.from_odcs(SAMPLE_ORDERS_ODCS_YAML)
    assert suite.name == "orders"


@pytest.mark.unit
def test_from_odcs_with_custom_suite_name() -> None:
    suite = from_odcs(SAMPLE_ORDERS_ODCS_YAML, suite_name="custom_orders_suite")
    assert suite.name == "custom_orders_suite"


@pytest.mark.unit
def test_from_odcs_from_file_path(tmp_path: Path) -> None:
    contract_file = tmp_path / "orders.odcs.yaml"
    contract_file.write_text(SAMPLE_ORDERS_ODCS_YAML, encoding="utf-8")

    suite_from_path = from_odcs(contract_file)
    assert suite_from_path.name == "orders"

    suite_from_str_path = from_odcs(str(contract_file))
    assert suite_from_str_path.name == "orders"


@pytest.mark.unit
def test_from_odcs_from_text_stream() -> None:
    stream = io.StringIO(SAMPLE_ORDERS_ODCS_YAML)
    suite = from_odcs(stream)
    assert suite.name == "orders"


@pytest.mark.unit
def test_from_odcs_from_dict() -> None:
    data = {
        "dataset": {"name": "dict_dataset"},
        "columns": [
            {"name": "col_a", "type": "varchar"},
            {"name": "col_b", "type": "int"},
        ],
        "quality": [
            {"type": "row_count", "min_value": 5, "max_value": 50},
        ],
    }
    suite = from_odcs(data)
    assert suite.name == "dict_dataset"
    assert any(isinstance(e, ExpectTableRowCountToBeBetween) for e in suite.expectations)


@pytest.mark.unit
def test_from_odcs_schema_with_properties_and_single_duplicate() -> None:
    yaml_content = """
apiVersion: v3.0.0
name: customers_contract
schema:
  - name: customers
    properties:
      - name: customer_id
        logicalType: integer
        unique: true
      - name: email
        logicalType: string
        quality:
          - rule: invalid
            pattern: '^[a-z]+@[a-z]+\\.[a-z]+$'
quality:
  - rule: rowCount
    mustBe: '>= 10 and <= 200'
  - rule: missing
    columns: [customer_id]
"""
    suite = from_odcs(yaml_content)
    assert suite.name == "customers_contract"

    # Single column uniqueness should produce ExpectColumnValuesToBeUnique
    unique_exp = next(e for e in suite.expectations if isinstance(e, ExpectColumnValuesToBeUnique))
    assert unique_exp.column == "customer_id"

    # Regex rule with pattern
    regex_exp = next(e for e in suite.expectations if isinstance(e, ExpectColumnValuesToMatchRegex))
    assert regex_exp.column == "email"
    assert regex_exp.regex == "^[a-z]+@[a-z]+\\.[a-z]+$"

    # Missing check
    not_null_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectColumnValuesToNotBeNull)
    )
    assert not_null_exp.column == "customer_id"

    # Row count bounds parsed from string
    row_count_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectTableRowCountToBeBetween)
    )
    assert row_count_exp.min_value == 10
    assert row_count_exp.max_value == 200


@pytest.mark.unit
def test_from_odcs_with_quality_as_dict() -> None:
    yaml_content = """
dataset:
  name: dict_quality_orders
  columns:
    order_id:
      type: integer
    order_date:
      type: date
quality:
  row_count:
    min: 1
    max: 100
  missing:
    columns: [order_id]
  duplicate:
    columns: [order_id, order_date]
  freshness:
    column: order_date
    bound: '2024-06-01'
"""
    suite = from_odcs(yaml_content)
    assert suite.name == "dict_quality_orders"

    row_count_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectTableRowCountToBeBetween)
    )
    assert row_count_exp.min_value == 1
    assert row_count_exp.max_value == 100

    not_null_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectColumnValuesToNotBeNull)
    )
    assert not_null_exp.column == "order_id"

    compound_exp = next(
        e for e in suite.expectations if isinstance(e, ExpectCompoundColumnsToBeUnique)
    )
    assert list(compound_exp.column_list) == ["order_id", "order_date"]

    freshness_exp = next(e for e in suite.expectations if isinstance(e, ExpectColumnMaxToBeBetween))
    assert freshness_exp.column == "order_date"
    assert freshness_exp.min_value is not None


@pytest.mark.unit
def test_from_odcs_with_data_context() -> None:
    context = gx.get_context(mode="ephemeral")
    suite = from_odcs(SAMPLE_ORDERS_ODCS_YAML, context=context)
    assert suite.name == "orders"
    assert suite.name in [s.name for s in context.suites.all()]


@pytest.mark.unit
def test_from_odcs_invalid_input() -> None:
    with pytest.raises(TypeError, match="Unsupported source type"):
        from_odcs(12345)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Parsed contract YAML must be a mapping/dict"):
        from_odcs("- list item\n- another item")


@pytest.mark.unit
def test_from_odcs_checkpoint_execution_and_gate_pass() -> None:
    import pandas as pd  # type: ignore[import-untyped]

    context = gx.get_context(mode="ephemeral")

    yaml_contract = """
apiVersion: v3.1.0
dataset:
  name: test_orders
  columns:
    - name: id
      type: int64
      required: true
    - name: email
      type: str
      format: email
    - name: amount
      type: float64
quality:
  - type: row_count
    mustBeBetween: [1, 10]
  - type: duplicate
    columns: [id, email]
"""
    suite = from_odcs(yaml_contract, context=context)

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
            "amount": [10.5, 20.0, 99.9],
        }
    )

    data_source = context.data_sources.add_pandas("e2e_pandas_datasource")
    data_asset = data_source.add_dataframe_asset("e2e_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe("e2e_batch_def")
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="e2e_val_def", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="e2e_checkpoint", validation_definitions=[val_def])
    )

    res = checkpoint.run(batch_parameters={"dataframe": df})
    assert res.success is True

    failed_batches = gx.contracts.enforce_contract_gate(res, raise_error=True)
    assert failed_batches == []


@pytest.mark.unit
def test_from_odcs_checkpoint_execution_and_gate_failure() -> None:
    import pandas as pd  # type: ignore[import-untyped]

    context = gx.get_context(mode="ephemeral")

    yaml_contract = """
apiVersion: v3.1.0
dataset:
  name: test_orders_failing
  columns:
    - name: id
      type: int64
      required: true
    - name: email
      type: str
      format: email
quality:
  - type: row_count
    mustBeBetween: [1, 2]
"""
    suite = from_odcs(yaml_contract, context=context)

    # 3 rows exceeds max of 2, plus invalid email
    df = pd.DataFrame(
        {
            "id": [1, None, 3],
            "email": ["bad_email", "bob@example.com", "charlie@example.com"],
        }
    )

    data_source = context.data_sources.add_pandas("failing_pandas_datasource")
    data_asset = data_source.add_dataframe_asset("failing_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe("failing_batch_def")
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="failing_val_def", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="failing_checkpoint", validation_definitions=[val_def])
    )

    res = checkpoint.run(batch_parameters={"dataframe": df})
    assert res.success is False

    quarantined: list[str] = []

    def quarantine_cb(batch_ids: list[str]) -> None:
        quarantined.extend(batch_ids)

    with pytest.raises(gx.contracts.DataContractValidationError) as exc_info:
        gx.contracts.enforce_contract_gate(
            res,
            raise_error=True,
            quarantine_callback=quarantine_cb,
        )

    assert len(exc_info.value.failed_batch_ids) > 0
    assert quarantined == exc_info.value.failed_batch_ids
