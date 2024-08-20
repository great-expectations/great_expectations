---
title: About GX Core
---
import GxData from '../_core_components/_data.jsx'
import PythonVersion from '../_core_components/_python_version.md'
import GxCloudAdvert from '/static/docs/_static_components/_gx_cloud_advert.md'

Great Expectations (GX) is the leading tool for validating and documenting your data. {GxData.product_name} is the open source Python library that supports this tool.  With
{GxData.product_name} you can further customize, automate, and expand on GX's processes to suite your specialized use cases.

Software developers have long known that automated testing is essential for managing complex codebases. GX brings the same discipline, confidence, and acceleration to data science and data engineering teams.

## Why use GX?

With GX, you can assert what you expect from the data you load and transform, and catch data issues quickly – Expectations are basically unit tests for your data. Not only that, but GX also creates data documentation and data quality reports from those Expectations. Data science and data engineering teams use GX to:

- Test data they ingest from other teams or vendors and ensure its validity.
- Validate data they transform as a step in their data pipeline in order to ensure the correctness of transformations.
- Prevent data quality issues from slipping into data products.
- Streamline knowledge capture from subject-matter experts and make implicit knowledge explicit.
- Develop rich, shared documentation of their data.

To learn more about how data teams are using GX, see [GX case studies](https://greatexpectations.io/case-studies/).

## Key Features

GX provides a unique framework for describing your data and validating it to ensure that it meets the standards you've defined.  In the process, GX will generate human-readable reports about the state of your data.  Additionally, GX's support for multiple backends ensures you can run your validations on data stored in different formats with minimal effort.

### Expectations

Expectations are assertions about your data. In GX, those assertions are expressed in a declarative language in the form of simple, human-readable Python methods. For example, in order to assert that you want the column “passenger_count” to be between 1 and 6, you can say:

```python title="Python code"
expect_column_values_to_be_between(
    column="passenger_count",
    min_value=1,
    max_value=6
)
```

GX then uses this statement to validate whether the column passenger_count in a given table is indeed between 1 and 6, and returns a success or failure result. The library currently provides several dozen highly expressive built-in Expectations, and allows you to write custom Expectations.

### Data validation

Once you’ve created your Expectations, GX can load any batch or several batches of data to validate with your suite of Expectations. GX tells you whether each Expectation in an Expectation Suite passes or fails, and returns any unexpected values that failed a test, which can significantly speed up debugging data issues!

### Data Docs

GX renders Expectations in a clean, human-readable format called Data Docs. These HTML docs contain both your Expectation Suites and your data Validation Results each time validation is run – think of it as a continuously updated data quality report. The following image shows a sample Data Doc:

![Screenshot of Data Docs](/docs/oss/guides/images/datadocs.png)

### Support for various Data Sources and Store backends

GX currently supports native execution of Expectations against various Data Sources, such as Pandas dataframes, Spark dataframes, and SQL databases via SQLAlchemy. This means you’re not tied to having your data in a database in order to validate it: You can also run GX against CSV files or any piece of data you can load into a dataframe.

GX is highly configurable. It allows you to store all relevant metadata, such as the Expectations and Validation Results in file systems, database backends, as well as cloud storage such as S3 and Google Cloud Storage, by configuring metadata Stores.

## What does GX NOT do?

GX is NOT a pipeline execution framework.

GX integrates seamlessly with DAG execution tools such as [Airflow](https://airflow.apache.org/), [dbt](https://www.getdbt.com/), [Prefect](https://www.prefect.io/), [Dagster](https://github.com/dagster-io/dagster), and [Kedro](https://github.com/quantumblacklabs/kedro). GX does not execute your pipelines for you, but instead, validation can simply be run as a step in your pipeline.

GX is NOT a data versioning tool.

GX does not store data itself. Instead, it deals in metadata about data: Expectations, Validation Results, etc. If you want to bring your data itself under version control, check out tools like: [DVC](https://dvc.org/), [Quilt](https://github.com/quiltdata/quilt), and [lakeFS](https://github.com/treeverse/lakeFS/).

GX is NOT an independent executable.

{GxData.product_name} is a Python library.  To use GX, you will need an installation of <PythonVersion/>.  Ideally, you will also configure a Python virtual environment to in which you can install and run GX.  Guidance on setting up your Python environment and installing the GX Python library is provided under [Set up a GX environment](/core/installation_and_setup/installation_and_setup.md) in the GX docs.

## GX Cloud

<GxCloudAdvert/>
