---
title: DataContext.test_yaml_config
---
[Back to class documentation](/docs/api_docs/classes/great_expectations-data_context-data_context-data_context-DataContext)

## Function test_yaml_config

### Fully qualified path

`great_expectations.data_context.data_context.data_context.DataContext.test_yaml_config`

[See it on GitHub](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/data_context/data_context/data_context.py)

### Synopsis

Convenience method for testing yaml configs
### Parameters

Parameter|Typing|Default|Description
---------|------|-------|-----------
self||||
yaml_config| str||A string containing the yaml config to be tested|A string containing the yaml config to be tested
name| Union[str, NoneType] | None|(Optional) A string containing the name of the component to instantiate|(Optional) A string containing the name of the component to instantiate
class_name| Union[str, NoneType] | None||
runtime_environment| Union[dict, NoneType] | None||
pretty_print| bool | True|Determines whether to print human-readable output|Determines whether to print human-readable output
return_mode| Union[Literal['instantiated_class'], Literal['report_object']] | 'instantiated_class'|Determines what type of object test_yaml_config will return. Valid modes are "instantiated_class" and "report_object"|Determines what type of object test_yaml_config will return. Valid modes are "instantiated_class" and "report_object"
shorten_tracebacks| bool | False|If true, catch any errors during instantiation and print only the last element of the traceback stack. This can be helpful for rapid iteration on configs in a notebook, because it can remove the need to scroll up and down a lot.|If true, catch any errors during instantiation and print only the last element of the traceback stack. This can be helpful for rapid iteration on configs in a notebook, because it can remove the need to scroll up and down a lot.

### Docstring

test_yaml_config is a convenience method for configuring the moving
parts of a Great Expectations deployment. It allows you to quickly
test out configs for system components, especially Datasources,
Checkpoints, and Stores.

For many deployments of Great Expectations, these components (plus
Expectations) are the only ones you'll need.

test_yaml_config is mainly intended for use within notebooks and tests.

**Args:**

- **yaml_config:**  A string containing the yaml config to be tested
- **name:**  (Optional) A string containing the name of the component to instantiate
- **pretty_print:**  Determines whether to print human-readable output
- **return_mode:**  Determines what type of object test_yaml_config will return. Valid modes are "instantiated_class" and "report_object"

- **shorten_tracebacks:** If true, catch any errors during instantiation and print only the last element of the traceback stack. This can be helpful for rapid iteration on configs in a notebook, because it can remove the need to scroll up and down a lot.

**Returns:**

-  The instantiated component (e.g. a Datasource) OR a json object containing metadata from the component's self_check method. The returned object is determined by return_mode.

### Related Documentation
- [/docs/terms/data_context](/docs/terms/data_context)