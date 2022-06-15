---
title: DataContext.test_yaml_config
---
[Back to class documentation](/docs/api_docs/classes/great_expectations-data_context-data_context-data_context-DataContext)

### Fully qualified path

`great_expectations.data_context.data_context.data_context.DataContext.test_yaml_config`

### Synopsis

Convenience method for testing yaml configs

test_yaml_config is a convenience method for configuring the moving
parts of a Great Expectations deployment. It allows you to quickly
test out configs for system components, especially Datasources,
Checkpoints, and Stores.

For many deployments of Great Expectations, these components (plus
Expectations) are the only ones you'll need.

test_yaml_config is mainly intended for use within notebooks and tests.

### Parameters

Parameter|Typing|Default|Description
---------|------|-------|-----------
self||||
yaml_config| str||A string containing the yaml config to be tested|A string containing the yaml config to be tested
name| Union[str, NoneType] | None|\(Optional\) A string containing the name of the component to instantiate|\(Optional\) A string containing the name of the component to instantiate
class_name| Union[str, NoneType] | None||
runtime_environment| Union[dict, NoneType] | None||
pretty_print| bool | True|Determines whether to print human\-readable output|Determines whether to print human\-readable output
return_mode| Union[Literal['instantiated_class'], Literal['report_object']] | 'instantiated_class'|Determines what type of object test\_yaml\_config will return\. Valid modes are "instantiated\_class" and "report\_object"|Determines what type of object test\_yaml\_config will return\. Valid modes are "instantiated\_class" and "report\_object"
shorten_tracebacks| bool | False|If true, catch any errors during instantiation and print only the last element of the traceback stack\. This can be helpful for rapid iteration on configs in a notebook, because it can remove the need to scroll up and down a lot\.|If true, catch any errors during instantiation and print only the last element of the traceback stack\. This can be helpful for rapid iteration on configs in a notebook, because it can remove the need to scroll up and down a lot\.

### Returns

The instantiated component (e.g. a Datasource) OR a json object containing metadata from the component's self_check method. The returned object is determined by return_mode.

## Relevant documentation (links)

- [Data Context](/docs/terms/data_context)
- [How to configure a new Checkpoint using test_yaml_config](/docs/guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config)