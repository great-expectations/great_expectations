---
title: Try Great Expectations
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import GxData from '../_core_components/_data.jsx';
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import ReleaseVersionBox from '../../components/versions/_gx_version_code_box.mdx'
import GxCloudAdvert from '/static/docs/_static_components/_gx_cloud_advert.md'

Follow this guide to install {GxData.product_name}, connect to sample data, build your first Expectation, validate the sample data, and review the Validation Results.  This is an ideal place to start if you're new to GX and would like to experiment with some basic features of GX to see what it has to offer.

## Prerequisites

- <PrereqPythonInstalled/>

## Setup

{GxData.product_name} is a Python library and can be installed using Python's included `pip` tool.

More comprehensive guidance on setting up a Python environment, installing GX, and installing additional dependencies for working with specific data formats and storage environments is available in the [Set up a GX environment](/core/installation_and_setup/install_gx.md) section of the docs. 

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Install the GX 1.0 library.

   The following command will install GX in your Python environment:

   ```bash title="Terminal input"
   pip install great_expectations
   ```

2. Verify that GX installed successfully.

   Once the {GxData.product_name} library has been installed, you can verify that you are using the correct version with:

   ```bash title="Terminal input"
   great_expectations --version
   ```

   The output you receive if GX was successfully installed will be:

   <ReleaseVersionBox/>

</TabItem>

</Tabs>

## Try GX

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the required modules from the {GxData.product_name} library.

   From a Python interpreter, IDE, or script run the following import statements:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py imports"
   ```
   
   The `great_expectations` module is the root of the GX library and contains shortcuts and convenience methods for starting a GX project in a Python session.

   The `expectations` module contains all the Expectation classes that are provided by the GX library.

2. Create a temporary Data Context and connect to the provided sample data.

   A Data Context can be thought of as a GX project.  In Python, it provides the API for interacting with many common GX objects.

   In the following code, a Data Context is initialized and then used to read the contents of a `.csv` file into a Batch of sample data:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py set up"
   ```

   The sample data that has been retrieved as a Batch will be used to test the Expectations that are created later in this guide.

3. Create an Expectation.

   Expectations are fundamental components of GX.  They allow you to explicitly define the state that your data should conform to.

   Since the provided sample data corresponds to paid taxicab trips, some assumptions can be made about what the content of that data should look like.  For example, it wouldn't make sense for the passenger count to be zero, since at least one passenger needs to be present to purchase the ride.  Additionally, the taxicabs in question should not seat more than six individuals.

   The following code defines an Expectation that the contents of the column `passenger_count` in the provided data consist of a value ranging from `1` to `6`: 

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py create an expectation"
   ```

4. Validate the sample data against your Expectation and view the results.
 
   Once an Expectation has been defined it can be applied to a Batch of data to see how well that data conforms to the Expectation.

   In the following code, the previously defined Expectation is validated against the Batch of sample data and the Validation Results are printed for review:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view results"
   ```

   In this example, the sample data does conform to the Expectation that was defined.  The printed Validation Results will look like:

   ```python title="Python output" name="docs/docusaurus/docs/core/introduction/try_gx.py output1"
   ```

5. Create an Expectation that will fail.

   Not all Expectations will pass successfully when validated.  This is the key to using GX for data quality: If the data does not match the state described in an Expectation, the Expectation will fail.  A failed Expectation lets you know that you have an issue to look into: it could be something wrong with the data itself, or it could be a misunderstanding about the information presented in the data, or any of a variety of issues that impact data quality.

   The following code creates an Expectation that will fail:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view failed results"
   ```
   
   You can see that the results of a failed Expectation are very different, and include metrics to help you assess the severity of the issue you've uncovered:

   ```python title="Python output" name="docs/docusaurus/docs/core/introduction/try_gx.py failed output"
   ```

   By default, only a portion of the failed values and record indexes will be included in the Validation Results to prevent the report from being too bloated to easily review.  However, the failed counts and percentages will correspond to all the failed records in the validated data.

7. Optional.  Experiment with other Expectations.

   The {GxData.product_name} library comes with numerous Expectations you can apply to your data.  You can browse the available Expectations through the [Expectations Gallery](https://greatexpectations.io/expectations).  Each entry in the Expectations Gallery links to a page with comprehensive information about the Expectation in question.  You can reference these pages to find details on how the Expectation functions and what parameters need to be provided when implementing it. 

   The Expectations Gallery also includes search and filter functionality to facilitate finding appropriate Expectations for your use case.  For instance, this guide uses pandas as the backend for reading data.  As such, you want to try additional Expectations, you should [filter the Expectation Gallery to find Expectations that support pandas](https://greatexpectations.io/expectations/?viewType=Summary&filterType=Backend+support&showFilters=true&subFilterValues=pandas).

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

