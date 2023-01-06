---
title: DataContext.create
---
[Back to class documentation](../classes/great_expectations-data_context-data_context-data_context-DataContext.md)

### Fully qualified path

`great_expectations.data_context.data_context.data_context.DataContext.create`

### Synopsis

Build a new great_expectations directory and DataContext object in the provided project_root_dir.

`create` will create a new "great_expectations" directory in the provided folder, provided one does not
already exist. Then, it will initialize a new DataContext in that folder and write the resulting config.

### Parameters

Parameter|Typing|Default|Description
---------|------|-------|-----------
project_root_dir| Union[str, NoneType] | None|path to the root directory in which to create a new great\_expectations directory|path to the root directory in which to create a new great\_expectations directory
usage_statistics_enabled| bool | True|boolean directive specifying whether or not to gather usage statistics|boolean directive specifying whether or not to gather usage statistics
runtime_environment| Union[dict, NoneType] | None|a dictionary of config variables that override both those set in config\_variables\.yml and the environment|a dictionary of config variables that override both those set in config\_variables\.yml and the environment

### Returns

DataContext

## Relevant documentation (links)

- [Data Context](../../terms/data_context.md)