---
title: How to create and edit Expectations with the User Configurable Profiler
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide will help you create a new Expectation Suite by profiling your data with the User Configurable Profiler.

<Prerequisites>

- Configured a [Data Context](../../tutorials/getting_started/initialize_a_data_context.md).
- Configured a [Datasource](../../tutorials/getting_started/connect_to_data.md)

</Prerequisites>

:::Note
You can access this same functionality from the Great Expectations CLI by running
```console
great_expectations --v3-api suite new --profile
```

If you go that route, you can follow along in the resulting Jupyter Notebook instead of using this guide.
:::

## Steps

### 1. Load or create your Data Context

Load an on-disk Data Context via:
```python
    import great_expectations as ge
    context: DataContext = DataContext(
        context_root_dir='path/to/my/context/root/directory/great_expectations'
    )
```

Alternatively, [you can instantiate a Data Context without a .yml file](../setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file)


### 2. Set your expectation_suite_name and create your Batch Request 

The Batch Request specifies which batch of data you would like to profile in order to create your suite. We will pass it into a Validator in the next step.

```python
expectation_suite_name = "insert_the_name_of_your_suite_here"

batch_request = {
    "datasource_name": "my_datasource",
    "data_connector_name": "default_inferred_data_connector_name",
    "data_asset_name": "yellow_tripdata_sample_2020-05.csv",
}
```

### 3. Instantiate your Validator

We use a Validator access and interact with your data. We will be passing the Validator to our Profiler in a later step.

```python
validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)
```

After you get your Validator, you can call `validator.head()` to confirm that it contains the data that you expect.

### 4. Instantiate a UserConfigurableProfiler

Next, we instantiate a UserConfigurableProfiler, passing in the Validator with our data

```python
profiler = UserConfigurableProfiler(profile_dataset=validator)
```

### 5. Use the profiler to build a suite

Finally, we call `profiler.build_suite()` to produce an Expectation Suite

```python
suite = profiler.build_suite()
```