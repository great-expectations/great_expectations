---
title: Manage Validation Definitions
---

A Validation Definition is an immutable reference that links a Batch of data to an Expectation Suite. It can be run by itself to validate the referenced data against the associated Expectations for testing or data exploration.  Multiple Validation Definitions can also be provided to a Checkpoint which, when run, executes Actions based on the Validation Results for each provided Validation Definition.

## Create a Validation Definition

```python
from great_expectations.core import ValidationDefinition
```

```python
validation_definition = context.validation_definitions.add(ValidationDefintion(batch_definition, suite)) 
```

## Get a Validation Definition by name

```python
validation_definition = context.validation_definitions.get(name_of_my_validation_definition)
```

## Get Validation Definitions by attributes

```python
validation_definition = next(
  vd for vd in context.validation_definitions
  if vd.data_source.name == "foo"
  and vd.asset.name == "bar"
  and vd.batch_definition.name == "baz"
  and vd.expectation_suite.name == "qux"
)
```

## Update a Validation Definition

Validation definitions are intended to be immutable references that link a set of data and corresponding Expectation Suite.  As such, they do not include an update method.

## Delete a Validation Definition

```python
validation = context.validations.delete(name=validation.name)
```

## Run a Validation Definition

```python
bd = asset.add_batch_definition(gxb.All(name="whole_asset"))
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(name="my_validation", bd, suite)
)
validation_results = validation_definition.run() # results are saved
print(validation_results)
```

GX Cloud users can view the Validation Results in the GX Cloud UI by following the url provided with:

```python
print(validation_results.result_url)
```