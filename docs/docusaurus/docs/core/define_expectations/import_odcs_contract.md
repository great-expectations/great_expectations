---
title: Import an ODCS data contract
description: Build an Expectation Suite from an Open Data Contract Standard v3.1 contract.
---

An [Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/) (ODCS) contract describes a dataset, its columns, and its quality rules. Great Expectations can translate an ODCS v3.1 contract into an Expectation Suite so the same contract that documents your data also validates it.

The importer only uses existing Expectations. It does not add a datasource, a validation engine, or any new metrics.

## Prerequisites

- Great Expectations installed.
- An ODCS v3.1 YAML contract, or an equivalent Python dictionary.

## Import a contract

Given `orders.contract.yaml`:

```yaml
version: 1.0.0
apiVersion: v3.1.0
kind: DataContract
id: 8c3f8e3e-0d1a-4b5e-9f8a-2a7c1d4e5f60
status: active
name: orders_contract
schema:
  - name: orders
    logicalType: object
    properties:
      - name: order_id
        logicalType: integer
        required: true
        primaryKey: true
      - name: email
        logicalType: string
        required: true
        quality:
          - id: email_must_be_valid
            metric: invalidValues
            arguments:
              pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
            mustBe: 0
      - name: amount
        logicalType: number
        logicalTypeOptions:
          minimum: 0
    quality:
      - id: order_rows
        metric: rowCount
        mustBeBetween: [1, 1000000]
```

Import it and add the resulting suite to your Data Context:

```python
import great_expectations as gx

suite = gx.contracts.from_odcs("orders.contract.yaml")

context = gx.get_context()
context.suites.add(suite)
```

The suite is named after the ODCS schema object. Pass `suite_name=` to override it. `from_odcs` returns an Expectation Suite and does not need a Data Context; only adding the suite to a context does.

## Select a schema object

An ODCS contract can describe more than one schema object. If it does, choose one by name:

```python
suite = gx.contracts.from_odcs("warehouse.contract.yaml", schema="orders")
```

If the contract has several schema objects and `schema` is omitted, the importer raises an error that lists the available names rather than silently combining unrelated tables.

## Type checks

GX type names are backend-specific, so the importer never guesses one from an ODCS `logicalType`. If a property has `physicalType`, the importer adds a type check using that value automatically. To override `physicalType`, or to supply a type name when a property has none, pass a mapping from ODCS `logicalType` to the type name your backend expects:

```python
suite = gx.contracts.from_odcs(
    "orders.contract.yaml",
    type_map={"integer": "int64", "number": "float64", "string": "object"},
)
```

Without `physicalType` or a matching `type_map` entry, the importer skips the type check for that property. A `physicalType` must be the exact name the backend expects, such as `int64` for pandas or `INTEGER` for a SQL backend.

## What is imported

| ODCS | Expectation |
| --- | --- |
| Schema properties | `ExpectTableColumnsToMatchSet` |
| `required`, `primaryKey`, `nullValues` with an exact-zero or percentage threshold | `ExpectColumnValuesToNotBeNull` |
| `unique`, single-column `primaryKey`, property `duplicateValues` | `ExpectColumnValuesToBeUnique` |
| Composite `primaryKey` | `ExpectCompoundColumnsToBeUnique`, ordered by `primaryKeyPosition` |
| `logicalTypeOptions.pattern`, `invalidValues` with a pattern | `ExpectColumnValuesToMatchRegex` |
| `invalidValues` with `validValues` | `ExpectColumnValuesToBeInSet` |
| `missingValues` containing `null` | `ExpectColumnValuesToNotBeNull` |
| `missingValues` with other sentinels | `ExpectColumnValuesToNotBeInSet` |
| `logicalTypeOptions.minimum` and `maximum` | `ExpectColumnValuesToBeBetween` |
| `logicalTypeOptions.minLength` and `maxLength` | `ExpectColumnValueLengthsToBeBetween` |
| Schema `rowCount` | `ExpectTableRowCountToBeBetween` |
| Schema `duplicateValues` with one `arguments.properties` column | `ExpectColumnValuesToBeUnique` |
| Schema `duplicateValues` with several `arguments.properties` columns | `ExpectCompoundColumnsToBeUnique` |

Every library quality rule must contain exactly one ODCS operator. The importer recognizes `mustBe`, `mustNotBe`, `mustBeGreaterThan`, `mustBeGreaterOrEqualTo`, `mustBeLessThan`, `mustBeLessOrEqualTo`, `mustBeBetween`, and `mustNotBeBetween`. Library metrics support this subset:

- `nullValues`, `invalidValues`, and `duplicateValues`: an exact-zero threshold, or a percentage upper bound. Percentages become `mostly` values. The exact-zero forms include `mustBe: 0`, `mustBeLessOrEqualTo: 0`, and `mustBeBetween: [0, 0]`.
- `missingValues`: an exact-zero threshold only.
- `rowCount`: all range operators, including strict bounds.

A nonzero row-count threshold for `missingValues`, `invalidValues`, or `duplicateValues` cannot be represented with an existing GX Expectation and raises `UnsupportedODCSRuleError`. So does `mustNotBe` or `mustNotBeBetween` on a library metric.

The importer never silently discards a rule:

- `text` rules are documentation only and are listed in `suite.meta["odcs"]["ignored"]`.
- `sql` rules cannot be translated to an existing Expectation and raise `UnsupportedODCSRuleError`.
- Rules that use an unsupported operator, unit, or threshold raise `UnsupportedODCSRuleError` with the path of the offending rule.
- Custom rules for another engine are listed as ignored.

Object and array properties, property-level relationships, timezone constraints, and object/array cardinality constraints are valid ODCS but do not have a flat-table Expectation mapping. The importer validates their structure and records them in `suite.meta["odcs"]["ignored"]`. The `ignored` key is always present and is an empty list when nothing was skipped.

ODCS v3.1 does not define a freshness metric. Contracts that need freshness can express it as a custom Great Expectations rule; ODCS SLA properties are not translated by this importer.

## Custom Great Expectations rules

If your contract already contains Great Expectations configuration, use an ODCS `custom` rule with `engine: greatExpectations`:

```yaml
quality:
  - id: custom_row_count
    type: custom
    engine: greatExpectations
    implementation:
      type: expect_table_row_count_to_be_between
      kwargs:
        min_value: 100
        max_value: 1000
```

The implementation is resolved through the GX Expectation registry, so unknown Expectation names and invalid arguments raise an error at import time. Use snake_case argument names, matching the GX Expectation signature. `implementation` can be a mapping, a YAML mapping string, or an Expectation type name on its own. For a property-level custom rule, the importer adds the property name as `column` when the Expectation accepts a `column` argument and the rule does not supply one.

## Block on validation failure

A contract is only useful if a failed validation stops the pipeline. Use `enforce_contract_gate` after the Checkpoint run:

```python
from great_expectations.contracts import enforce_contract_gate

result = checkpoint.run()
enforce_contract_gate(result)
```

The gate returns when every validation succeeds. Otherwise it raises `DataContractValidationError`, which identifies the failed batch IDs. Run cleanup or quarantining before the exception is raised with `on_failure`:

```python
def quarantine(batch_ids): ...  # move the failed batches aside


enforce_contract_gate(result, on_failure=quarantine)
```

Because the gate raises after validation completes, it does not prevent Data Docs or other Checkpoint actions from running.
