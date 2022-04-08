---
title: How to quickly explore Expectations in a notebook
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx'

Building <TechnicalTag tag="expectation" text="Expectations" /> as you conduct exploratory data analysis is a great way to ensure that your insights about data processes and pipelines remain part of your team's knowledge.

This guide will help you quickly get a taste of Great Expectations, without even setting up a <TechnicalTag tag="data_context" text="Data Context" />. All you need is a notebook and some data.

:::note This how-to guide assumes you have already:
- Installed Great Expectations (e.g. `pip install great_expectations`)
- Have access to a notebook (e.g. `jupyter notebook`, `jupyter lab`, etc.)
- Obtained a sample of data to use for exploration
:::

:::info
Unlike most how-to guides, these instructions do *not* assume that you have already configured a Data Context by running `great_expectations init`. Once you're comfortable with these basic concepts, you will almost certainly want to unlock the full power of Great Expectations by configuring a Data Context. Please check out the instructions in the [Getting started tutorial](../../tutorials/getting_started/intro.md) when you're ready to start.
:::

## Steps

All of these steps take place within your notebook:

### 1. Import Great Expectations.

```python
import great_expectations as ge
```

### 2. Load some data.

The simplest way to do this is with `read_csv`.

```python
my_df = ge.read_csv("my_data_directory/titanic.csv")
```

This method behaves exactly the same as `pandas.read_csv`, so you can add parameters to parse your file:

```python
my_df = ge.read_csv(
    "my_data_directory/my_messy_data.csv",
    sep="\t",
    skiprows=3
)
```

Similarly wrapped versions of other pandas methods (`read_excel`, `read_table`, `read_parquet`, `read_pickle`, `read_json`, etc.) are also available. Please see the `great_expectations.utils` module for details.

If you wish to load data from somewhere else (e.g. from a SQL database or blob store), please fetch a copy of the data locally. Alternatively, you can [configure a Data Context with Datasources](../../tutorials/getting_started/connect_to_data.md), which will allow you to take advantage of more of Great Expectations' advanced features.

As alternatives, if you have already instantiated :

- a `pandas.Dataframe`, you can use `from_pandas`:

```python
my_df = ge.from_pandas(
    my_pandas_dataframe
)
```

This method will convert your boring old pandas `DataFrame` into a new and exciting great_expectations `PandasDataset`. The two classes are absolutely identical, except that `PandasDataset` has access to Great Expectations' methods.

- a `Spark DataFrame`, you can use `SparkDFDataset`:

```python
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

my_df = SparkDFDataset(my_spark_dataframe)
```

This method will create an object with access to Great Expectations' methods, such as `ProfilingResultsPageRenderer`.


### 3. Explore your data and add Expectations

Each of the methods in step 1 will produce `my_df`, a `PandasDataset`. `PandasDataset` is a subclass of `pandas.DataFrame`, which means that you can use all of pandas' normal methods on it.

```python
my_df.head()
my_df.Sex.value_counts()
my_df[my_df.Sex=="male"].head()
# etc., etc. 
```

In addition, `my_df` has access to a wide array of Expectations. You can see the full list [in the Expectation Gallery](https://greatexpectations.io/expectations). By convention, every <TechnicalTag tag="expectation" text="Expectation" /> method name starts with the name `expect_...`, so you can quickly access the full list with tab-based autocomplete:

![image](../images/expectation_autocomplete.gif)

When you invoke an Expectation, it will immediately be <TechnicalTag tag="validation" text="Validated" /> against your data. The returned object will contain the result and a list of unexpected values. This instant feedback helps you zero in on unexpected data very quickly, taking a lot of the guesswork out of data exploration.

![image](../images/expectation_notebook_interactive_loop.gif)

Hint: it's common to encounter data issues where most cases match, but you can't guarantee 100% adherence. In these cases, consider using a `mostly` parameter. This parameter is an option for all Expectations that are applied on a row-by-row basis, and allows you to control the level of wiggle room you want built into your data Validation.


![image](../images/interactive_mostly.gif)


Note how `success` switches from `false` to `true` once `mostly=.99` is added.


### 4. Review your Expectations.

As you run Expectations in your notebook, `my_df` will build up a running list of Expectations. By default, Great Expectations will recognize and replace duplicate Expectations, so that only the most recent version is stored. (See "Determining duplicate results" below for details.)

You can get the config file for your Expectations by running:

```python
my_df.get_expectation_suite()
```

which will return an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> object.

By default, `get_expectation_suite()` only returns Expectations with `success=True` on their most recent Validation. You can override this behavior with:

```python
my_df.get_expectation_suite(discard_failed_expectations=False)
```

### 5. Save your Expectation Suite.

Expectation Suites can be serialized as JSON objects, so you can save your Expectation Suite like this:

```python
import json

with open( "my_expectation_file.json", "w") as my_file:
    my_file.write(
        json.dumps(my_df.get_expectation_suite().to_json_dict())
    )
```

As you develop more Expectation Suites, you'll probably want some kind of system for naming and organizing them, not to mention matching them up with data, validating them, and keeping track of <TechnicalTag tag="validation_result" text="Validation Results" />.

When you get to this stage, we recommend following the [getting started tutorial](../../tutorials/getting_started/tutorial_overview.md) to set up a Data Context. You can get through the basics in less than half an hour, and setting up a Data Context will unlock many additional power tools within Great Expectations.
        
## Additional notes

### Adding notes and metadata

You can also add notes and structured metadata to Expectations:

```python
>> my_df.expect_column_values_to_match_regex(
    "Name",
    "^[A-Za-z\, \(\)\']+$",
    meta = {
        "notes": {
            "content": [ "A simple experimental regex for name matching." ],
            "format": "markdown",
            "source": "max@company.com"
        }
    )
```

### Determining duplicate results

As a general rule, 

- If a given Expectation has no `column` parameters, it will replace another Expectation(s) of the same type.

    Example:
    
    ```python
    expect_table_row_count_to_equal(100)
    ``` 
    will overwrite
    
    ```python
    expect_table_row_count_to_equal(200)
    ```
- If a given Expectation has one or more `column` parameters, it will replace another Expectation(s) of the same type with the same column parameter(s).

    Example:

    ```python
    expect_column_values_to_be_between(
        column="percent_agree",
        min_value=0,
        max_value=100,
    )
    ```

    will overwrite
    
    ```python
    expect_column_values_to_be_between(
        column="percent_agree",
        min_value=10,
        max_value=90,
    )
    ```

    or

    ```python
    expect_column_values_to_be_between(
        column="percent_agree",
        min_value=0,
        max_value=100,
        mostly=.80,
    )
    ```

    but not

    ```python
    expect_column_values_to_be_between(
        column="percent_agreement",
        min_value=0,
        max_value=100,
        mostly=.80,
    )
    ```
    
    and not

    ```python
    expect_column_mean_to_be_between(
        column="percent",
        min_value=65,
        max_value=75,
    )
    ```

## Additional resources

- [Expectation Gallery](https://greatexpectations.io/expectations)
