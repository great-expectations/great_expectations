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

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Run the following code to install the {GxData.product_name} library:

  

   ```bash title="Terminal input"
   pip install great_expectations
   ```

   

2. Verify that GX installed successfully.

3. Run the following code to verify the {GxData.product_name} version:

   ```bash title="Terminal input"
   great_expectations --version
   ```

   The following output appears when {GxData.product_name} is successfully installed:

   <ReleaseVersionBox/>

</TabItem>

</Tabs>

## Test features and functionality

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Run the following code in a Python interpreter, IDE, or script to import the required modules from the {GxData.product_name} library:

   

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py imports"
   ```
   
   The `great_expectations` module is the root of the GX library and contains shortcuts and convenience methods for starting a GX project in a Python session.

   The `expectations` module contains all the Expectation classes that are provided by the GX library.

2. Run the following code to create a temporary Data Context and connect to sample data:

   A Data Context can be thought of as a GX project.  In Python, it provides the API for interacting with many common GX objects.

   In the following code, a Data Context is initialized and then used to read the contents of a `.csv` file into a Batch of sample data:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py set up"
   ```

   

3. Run the following code to create an Expectation:

   Expectations are fundamental components of GX.  They allow you to explicitly define the state that your data should conform to.

   The provided sample data is a table of paid taxicab rides. Knowing that lets you assume some things about what the content of that data should look like.  For example, it wouldn't make sense for the passenger count to be zero, since at least one passenger needs to be present to purchase the ride.  Additionally, the taxicabs in question should not have room to seat more than six passengers.

   The following code defines an Expectation that the contents of the column `passenger_count` in the provided data consist of a value ranging from `1` to `6`: 

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py create an expectation"
   ```

4. Run the following code to validate the sample data against your Expectation and view the results:
 
  

   

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view results"
   ```

  The sample data does not conform to the defined Expectation and the following Validation Results are returned:

   ```python title="Python output" name="docs/docusaurus/docs/core/introduction/try_gx.py output1"
   ```

5. Run the following code to create an Expectation that fails because it assumes that a taxi can seat a maximum of three passengers:

  

   A failed Expectation lets you know there is something wrong with the data, such as missing or incorrect values, or there is a misunderstanding about the data.

   

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view failed results"
   ```

  In the following example, the results of the failed Expectation include metrics to help you assess the severity of the issue:

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

You can learn more about GX by browsing through [Community resources](/core/introduction/community_resources.md) such as the GX Discourse forum, public Slack channel, and GitHub repo.

### Try GX Cloud

<GxCloudAdvert/>

### Dive in with GX and Python

If you're ready to start using {GxData.product_name} with your own data, the [Set up a GX environment](/core/installation_and_setup/install_gx.md) documentation provides a more comprehensive guide to setting up GX to work with specific data formats and environments.

