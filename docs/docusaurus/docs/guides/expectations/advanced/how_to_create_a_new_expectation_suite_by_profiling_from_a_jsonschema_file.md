---
title: How to create a new Expectation Suite by profiling from a jsonschema file
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

import InProgress from '@site/docs/components/warnings/_in_progress.md'

<InProgress />

The ``JsonSchemaProfiler`` helps you quickly create <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> from jsonschema files.

<Prerequisites>

- [Set up a working deployment of Great Expectations](/docs/guides/setup/setup_overview)
- Have a valid jsonschema file that has top level object of type `object`

</Prerequisites>

:::warning
This implementation does not traverse any levels of nesting.
:::

## Steps

### 1. Set a filename and a suite name

```python
jsonschema_file = "YOUR_JSON_SCHEMA_FILE.json"
suite_name = "YOUR_SUITE_NAME"
```

### 2. Load a DataContext

```python
context = gx.get_context()
```

### 3. Load the jsonschema file

```python
with open(jsonschema_file, "r") as f:
  schema = json.load(f)
```

### 4. Instantiate the profiler

```python
profiler = JsonSchemaProfiler()
```

### 5. Create the suite

```python
suite = profiler.profile(schema, suite_name)
```

### 6. Save the suite

```python
context.add_expectation_suite(expectation_suite=suite)
```

## 7. (Optional) Generate Data Docs and review the results

<TechnicalTag tag="data_docs" text="Data Docs" /> provides a concise and useful way to review the Expectation Suite that has been created.

```bash
context.build_data_docs()
```

You can also review and update the <TechnicalTag tag="expectation" text="Expectations" /> created by the <TechnicalTag tag="profiler" text="Profiler" /> to get to the Expectation Suite you want using ``great_expectations suite edit``.

## Additional notes

:::important

Note that JsonSchemaProfiler generates Expectation Suites using column map Expectations, which assumes a tabular data structure, because Great Expectations does not currently support nested data structures.

:::

The full example script is here:

```python
import json
import great_expectations as gx
from great_expectations.profile.json_schema_profiler import JsonSchemaProfiler

jsonschema_file = "YOUR_JSON_SCHEMA_FILE.json"
suite_name = "YOUR_SUITE_NAME"

context = gx.get_context()

with open(jsonschema_file, "r") as f:
    raw_json = f.read()
    schema = json.loads(raw_json)

print("Generating suite...")
profiler = JsonSchemaProfiler()
suite = profiler.profile(schema, suite_name)
context.add_expectation_suite(expectation_suite=suite)
```

