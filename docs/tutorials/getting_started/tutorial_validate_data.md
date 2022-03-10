---
title: 'Tutorial, Step 4: Validate data'
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '/docs/term_tags/_tag.mdx';

<UniversalMap setup='inactive' connect='inactive' create='inactive' validate='active'/> 

:::note Prerequisites

- Completed [Step 3: Create Expectations](./tutorial_create_expectations.md) of this tutorial.

:::

### Set up a Checkpoint

Let’s set up our first <TechnicalTag relative="../../" tag="checkpoint" text="Checkpoint" />!  

A Checkpoint runs an <TechnicalTag relative="../../" tag="expectation_suite" text="Expectation Suite" /> against a <TechnicalTag relative="../../" tag="batch" text="Batch" /> (or <TechnicalTag relative="../../" tag="batch_request" text="Batch Request" />).  Running a Checkpoint produces <TechnicalTag relative="../../" tag="validation_result" text="Validation Results" />.  Checkpoints can also be configured to perform additional <TechnicalTag relative="../../" tag="action" text="Actions" />.  

For the purposes of this tutorial, the Checkpoint we create will run the Expectation Suite we previously configured against the data we provide.  We will use it to verify that there are no unexpected changes in the February NYC taxi data compared to what our <TechnicalTag relative="../../" tag="profiler" text="Profiler" /> observed in the January NYC taxi data.

**Go back to your terminal** and shut down the Jupyter Notebook, if you haven’t yet. Then run the following command:

```console
great_expectations checkpoint new getting_started_checkpoint
```

This will **open a Jupyter Notebook** that will allow you to complete the configuration of your Checkpoint.

The Jupyter Notebook contains some boilerplate code that allows you to configure a new Checkpoint. The second code cell is pre-populated with an arbitrarily chosen Batch Request and Expectation Suite to get you started. Edit the `data_asset_name` to reference the data we want to validate (the February data), as follows:

```python file=../../../tests/integration/docusaurus/tutorials/getting-started/getting_started.py#L161-L174
```

You can then execute all cells in the notebook in order to store the Checkpoint to your Data Context.

#### What just happened?

- `getting_started_checkpoint` is the name of your new Checkpoint.

- The Checkpoint uses `getting_started_expectation_suite_taxi.demo` as its primary Expectation Suite.

- You configured the Checkpoint to validate the `yellow_tripdata_sample_2019-02.csv` (i.e. our February data) file.

### How to run validation and inspect your Validation Results

In order to build <TechnicalTag relative="../../" tag="data_docs" text="Data Docs" /> and get your results in a nice, human-readable format, you can simply uncomment and run the last cell in the notebook. This will open Data Docs, where you can click on the latest <TechnicalTag relative="../../" tag="validation" text="Validation" /> run to see the Validation Results page for this Checkpoint run.

![data_docs_failed_validation1](../../../docs/images/data_docs_taxi_failed_validation01.png)

You’ll see that the test suite failed when you ran it against the February data.

#### What just happened? Why did it fail?? Help!?

We ran the Checkpoint and it successfully failed! **Wait - what?** Yes, that’s correct, this indicates that the February data has data quality issues, which means we want the Validation to fail.

Click on the highlighted row to access the Validation Results page, which will tell us specifically what is wrong with the February data.

![data_docs_failed_validation2](../../../docs/images/data_docs_taxi_failed_validation02.png)

On the Validation Results page, you will see that the Validation of the staging data *failed* because the set of *Observed Values* in the `passenger_count` column contained the value `0`! This violates our Expectation, which makes the validation fail.

**And this is it!**

We have successfully created an Expectation Suite based on historical data, and used it to detect an issue with our new data. **Congratulations! You have now completed the “Getting started with Great Expectations” tutorial.**
