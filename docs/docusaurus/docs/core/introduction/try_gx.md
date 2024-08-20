---
title: Try GX Core
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import ReleaseVersionBox from '../../components/versions/_gx_version_code_box.mdx'
import GxCloudAdvert from '/static/docs/_static_components/_gx_cloud_advert.md'

Start here to learn how to connect to data, create Expectations, validate data, and review Validation Results. This is an ideal place to start if you're new to GX Core and want to experiment with features and see what it offers.

To complement your code exploration, check out the [GX Core overview](/core/introduction/gx_overview.md) for a primer on the GX components and workflow pattern used in the examples.

## Prerequisites

- <PrereqPythonInstalled/>

## Setup

GX Core is a Python library you can install with the Python `pip` tool.

For more comprehensive guidance on setting up a Python environment, installing GX Core, and installing additional dependencies for specific data formats and storage environments, see [Set up a GX environment](/core/installation_and_setup/install_gx.md).

1. Run the following terminal command to install the GX Core library:

   ```bash title="Terminal input"
   pip install great_expectations
   ```

2. Verify GX Core installed successfully:

   ```bash title="Terminal input"
   great_expectations --version
   ```

   The following output appears when GX Core is successfully installed:

   <ReleaseVersionBox/>


## Sample data
The examples provided on this page use a sample of [NYC taxi trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Using this data, you can make certain assumptions. For example:
* The passenger count should be greater than zero because at least one passenger needs to be present for a ride. And, taxis can accommodate a maximum of six passengers.
* Trip fares should be greater than zero.

## Try an exploratory workflow to validate data in a DataFrame
This example workflow walks you through connecting to data in a Pandas DataFrame and validating the data using a single Expectation.

:::tip Pandas install
This example requires that [Pandas](https://pandas.pydata.org/) is installed in the same Python environment where you are running GX Core.
:::

<Tabs>

<TabItem value="exploratory_walkthrough" label="Walkthrough">

Run the following steps in a Python interpreter, IDE, notebook, or script.

1. Import the `great_expectations` library and `expectations` module.

   The `great_expectations` module is the root of the GX library and contains shortcuts and convenience methods for starting a GX project in a Python session.

   The `expectations` module contains all the Expectation classes that are provided by the GX library.

   The `pandas` library is used to ingest sample data for this example.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py import gx library"
   ```

2. Download and read the sample data into a Pandas DataFrame.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py import sample data"
   ```

3. Create a Data Context.

   A Data Context object serves as the entrypoint for interacting with GX components.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py create data context"
   ```

4. Connect to data and create a Batch.

   Define a Data Source, Data Asset, Batch Definition, and Batch. The Pandas DataFrame is provided to the Batch Definition at runtime to create the Batch.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py connect to data and get batch"
   ```

5. Create an Expectation.

   Expectations are a fundamental component of GX.  They allow you to explicitly define the state to which your data should conform.

   Run the following code to define an Expectation that the contents of the column `passenger_count` consist of values ranging from `1` to `6`:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py create expectation"
   ```

6. Run the following code to validate the sample data against your Expectation and view the results:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py validate batch"
   ```

   The sample data conforms to the defined Expectation and the following Validation Results are returned:

   ```python title="Python output" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py passing output"
   ```

</TabItem>

<TabItem value="exploratory_full_example" label="Full example code">
```python title="Full example code" name="docs/docusaurus/docs/core/introduction/try_gx_exploratory.py full exploratory script"
```

</TabItem>

</Tabs>

## Try an end-to-end workflow to validate data in a SQL table
This example end-to-end workflow walks you through connecting to data in a Postgres table, creating an Expectation Suite, and setting up a Checkpoint to validate the data.

<Tabs>

<TabItem value="end2end_walkthrough" label="Walkthrough">

Run the following steps in a Python interpreter, IDE, notebook, or script.

1. Import the `great_expectations` library and `expectations` module.

   The `great_expectations` module is the root of the GX library and contains shortcuts and convenience methods for starting a GX project in a Python session.

   The `expectations` module contains all the Expectation classes that are provided by the GX library.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py import gx library"
   ```

2. Create a Data Context.

   A Data Context object serves as the entrypoint for interacting with GX components.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create data context"
   ```

3. Connect to data and create a Batch.

   Define a Data Source, Data Asset, Batch Definition, and Batch. The connection string is used by the Data Source to connect to the cloud Postgres database hosting the sample data.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py connect to data and get batch"
   ```

4. Create an Expectation Suite.

   Expectations are a fundamental component of GX.  They allow you to explicitly define the state to which your data should conform. Expectation Suites are collections of Expectations.

   Run the following code to define an Expectation containing two Expectations. The first Expectation expects that the column `passenger_count` consists of values ranging from `1` to `6`, and the second expects that the column `fare_amount` contains non-negative values.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create expectation suite"
   ```

5. Create an Validation Definition.

   The Validation Definition explicitly ties together the Batch of data to be validated to the Expectation Suite used to validate the data.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create validation definition"
   ```

6. Create and run a Checkpoint to validate the data based on the supplied Validation Definition. `.describe()` is a convenience method to view a summary of the Checkpoint results.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py create and run checkpoint"
   ```

The returned results reflect the passing of one Expectation and the failure of one Expectation.

When an Expectation fails, the Validation Results of the failed Expectation include metrics to help you assess the severity of the issue:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py checkpoint result"
   ```

To reduce the size of the results and make it easier to review, only a portion of the failed values and record indexes are included in the Checkpoint results. The failed counts and percentages correspond to the failed records in the validated data.

</TabItem>

<TabItem value="end2end_full_example" label="Full example code">
```python title="Full example code" name="docs/docusaurus/docs/core/introduction/try_gx_end_to_end.py full end2end script"
```
</TabItem>
</Tabs>


## Next steps

- Go to the [Expectations Gallery](https://greatexpectations.io/expectations) and experiment with other Expectations.

- If you're ready to start using GX Core with your own data, the [Set up a GX environment](/core/installation_and_setup/install_gx.md) documentation provides a more comprehensive guide to setting up GX to work with specific data formats and environments.

- <GxCloudAdvert/>