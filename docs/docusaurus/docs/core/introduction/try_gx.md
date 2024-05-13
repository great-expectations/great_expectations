---
title: Try Great Expectations
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import GxData from '../_core_components/_data.jsx';
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';



Follow this guide to install {GxData.product_name}, connect to sample data, build your first Expectation, validate the sample data, and review the Validation Results.  This is an ideal place to start if you're new to GX and would like to experiment with some basic features of GX to see what it has to offer.

## Prerequisites

- <PrereqPythonInstalled/>

## Try GX

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the required modules from the {GxData.product_name} library.

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py imports"
   ```

2. Create a temporary Data Context and connect to the provided sample data.

   A Data Context can be thought of as a GX project.  In the following code, a Data Context is initialized and then used to read the contents of a `.csv` file into a Batch of sample data:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py set up"
   ```

   The sample data that has been retrieved as a Batch will be used to test the Expectations that are created later in this guide.

3. Create an Expectation.

   Expectations are a fundamental feature of GX.  They allow you to explicitly define the state that your data should conform to.

   Since the provided sample data corresponds to purchased taxicab trips, some assumptions can be made about the content of that data.  For example, it wouldn't make sense for the passenger count to ever be `0`, since at least one passanger needs to be present to purchase the ride.  Additionally, the taxicabs in question should not be capable of seating more than `6` individuals.

   The following code defines an Expectation that the contents of the column `passenger_count` in the provided data consist of a value ranging from `1` to `6`: 

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py create an expectation"
   ```

4. Validate the sample data against your Expectation and view the results.
 
   Once an Expectation has been defined it can be applied to a Batch of data to see how well that data conforms to the Expectation.

   In the following code, the previously defined Expectation is validated against the Batch of sample data and the Validation Results are then printed for evaluation:

   ```python title="Python input" name="docs/docusaurus/docs/core/introduction/try_gx.py validate and view results"
   ```

   In this example, the sample data does conform to the Expectation that was defined.  The validation results returned will look like:

   ```python title="Python output" name="docs/docusaurus/docs/core/introduction/try_gx.py output1"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Full example script" name="docs/docusaurus/docs/core/introduction/try_gx.py full example script"
```

</TabItem>

</Tabs>