---
title: Store
id: store
hoverText: A connector to store and retrieve information about metadata in Great Expectations.
---

A Store is a connector to store and retrieve information about metadata in Great Expectations.

Great Expectations supports a variety of Stores for different purposes, but the most common Stores are Expectation Stores, Validations Stores, Checkpoint Stores, and Evaluation Parameter Stores (or Metric Stores). 

## Finding your Stores

You can find a list of all of your Stores in your `great_expectations.yml` file.  Alternatively, from the root folder of your Data Context you can retrieve a list of Stores with a CLI command:

```markdown title="Console command"
great_expectations store list
```



NOTES: TEMPORARY
--------------
Location where your Data Context stores information about your Expectations, Validation Results, and Metrics.
A connector to store and retrieve information about metadata in Great Expectations., such as Expectations, Validation Results, and Metrics.

By default, newly profiled Expectations are stored in JSON format in the `expectations/` subdirectory of your great_expectations/ folder.

By default, Validation results are stored in JSON format in the uncommitted/validations/ subdirectory of your great_expectations/ folder. Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.