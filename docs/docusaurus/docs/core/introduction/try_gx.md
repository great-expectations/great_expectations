---
title: Try Great Expectations
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import GxData from '../_core_components/_data.jsx';
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import ReleaseVersionBox from '../../components/versions/_gx_version_code_box.mdx'
import GxCloudAdvert from '/static/docs/_static_components/_gx_cloud_advert.md'

If you're new to {GxData.product_name}, start here to learn how to connect to sample data, build an Expectation, validate sample data, and review Validation Results. This is an ideal place to start if you're new to GX and want to experiment with features and see what it offers.

## Prerequisites

- <PrereqPythonInstalled/>

## Setup

{GxData.product_name} is a Python library you can install with the Python `pip` tool.

For more comprehensive guidance on setting up a Python environment, installing {GxData.product_name}, and installing additional dependencies for specific data formats and storage environments, see [Set up a GX environment](/core/installation_and_setup/install_gx.md).

1. Run the following terminal command to install the {GxData.product_name} library:

   ```bash title="Terminal input"
   pip install great_expectations
   ```

2. Verify {GxData.product_name} installed successfully:

   ```bash title="Terminal input"
   great_expectations --version
   ```

   The following output appears when {GxData.product_name} is successfully installed:

   <ReleaseVersionBox/>


## Test features and functionality

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the `great_expectations` library and `expectations` module.

   The `great_expectations` module is the root of the GX library and contains shortcuts and convenience methods for starting a GX project in a Python session.

   The `expectations` module contains all the Expectation classes that are provided by the GX library.

   Run the following code in a Python interpreter, IDE, or script:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py imports"
   ```

3. Run the following code to create a temporary Data Context and connect to sample data:

   In Python, a Data Context provides the API for interacting with many common GX objects.

   In the following code, a Data Context is initialized and then used to read the contents of a `.csv` file into a Batch of sample data:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py set up"
   ```

3. Create an Expectation.

   Expectations are a fundamental component of GX.  They allow you to explicitly define the state to which your data should conform.

   The sample data you're using is taxi trip record data. With this data, you can make certain assumptions.  For example, the passenger count shouldn't be zero because at least one passenger needs to be present.  Additionally, a taxi can accomodate a maximum of six passengers.

   Run the following code to define an Expectation that the contents of the column `passenger_count` consist of values ranging from `1` to `6`: 

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py create an expectation"
   ```

4. Run the following code to validate the sample data against your Expectation and view the results:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view results"
   ```

   The sample data conforms to the defined Expectation and the following Validation Results are returned:

   ```python title="Python output" name="docs/docusaurus/docs/core/introduction/try_gx.py output1"
   ```

5. Optional. Create an Expectation that will fail when validated against the provided data.

   A failed Expectation lets you know there is something wrong with the data, such as missing or incorrect values, or there is a misunderstanding about the data.

   Run the following code to create an Expectation that fails because it assumes that a taxi can seat a maximum of three passengers:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view failed results"
   ```

   When an Expectation fails, the Validation Results of the failed Expectation include metrics to help you assess the severity of the issue:

   ```python title="Python output" name="docs/docusaurus/docs/core/introduction/try_gx.py failed output"
   ```

   To reduce the size of the report and make it easier to review, only a portion of the failed values and record indexes are included in the Validation Results.  The failed counts and percentages correspond to the failed records in the validated data.

6. Optional. Go to the [Expectations Gallery](https://greatexpectations.io/expectations) and experiment with other Expectations.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Full example script" name="docs/docusaurus/docs/core/introduction/try_gx.py full example script"
```

</TabItem>

</Tabs>

## Next steps

<GxCloudAdvert/>

To learn more about {GxData.product_name}, see [Community resources](/core/introduction/community_resources.md).

If you're ready to start using {GxData.product_name} with your own data, the [Set up a GX environment](/core/installation_and_setup/install_gx.md) documentation provides a more comprehensive guide to setting up GX to work with specific data formats and environments.

