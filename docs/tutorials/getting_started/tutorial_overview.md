---
title: Getting started with Great Expectations
---
import TechnicalTag from '/docs/term_tags/_tag.mdx';
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

Welcome to the Great Expectations getting started tutorial! This tutorial will help you set up your first local deployment of Great Expectations.  This deployment will contain a small <TechnicalTag relative="../../" tag="expectation_suite" text="Expectation Suite" /> that we will use to <TechnicalTag relative="../../" tag="validation" text="Validate" /> some sample data. We'll also introduce important concepts, with links to detailed material you can dig into later.

:::tip
The steps described in this tutorial assume you are installing Great Expectations version 0.13.8 or above.

For a tutorial for older versions of Great Expectations, please see older versions of this documentation, which can be found [here](https://docs.greatexpectations.io/en/latest/guides/tutorials.html).
:::

### This tutorial will walk you through the following steps

<table class="borderless markdown">
<tr>
    <td>
        <img
          src={require('../../images/universal_map/Gear-active.png').default}
          alt="Setup"
        />
    </td>
    <td>
        <h4>Setup</h4>
        <p>

First, we will make sure you have Great Expectations installed and show you how to initialize a <TechnicalTag relative="../../" tag="data_context" text="Data Context" />.

</p>
    </td>
</tr>
<tr>
    <td>
        <img
          src={require('../../images/universal_map/Outlet-active.png').default}
          alt="Connect to Data"
        />
    </td>
    <td>
        <h4>Connect to Data</h4>
        <p>

Then you will learn how to configure a <TechnicalTag relative="../../" tag="datasource" text="Datasource" /> to connect to your data.

</p>
    </td>
</tr>
<tr>
    <td>
        <img
          src={require('../../images/universal_map/Flask-active.png').default}
          alt="Create Expectations"
        />
    </td>
    <td>
        <h4>Create Expectations</h4>
        <p>

You will then create your first Expectation Suite using the built-in automated <TechnicalTag relative="../../" tag="profiler" text="Profiler" />. You'll also take your first look at <TechnicalTag relative="../../" tag="data_docs" text="Data Docs" />, where you will be able to see the <TechnicalTag relative="../../" tag="expectation" text="Expectations" /> that were created.

</p>
    </td>
</tr>
<tr>
    <td>
        <img
          src={require('../../images/universal_map/Checkmark-active.png').default}
          alt="Validate Data"
        />
    </td>
    <td>
        <h4>Validate Data</h4>
        <p>

Finally, we will show you how to use this Expectation Suite to Validate a new batch of data, and take a deeper look at the Data Docs which will show your <TechnicalTag relative="../../" tag="validation_result" text="Validation Results" />.

</p>
    </td>
</tr>
</table>

But before we dive into the first step, let's bring you up to speed on the problem we are going to address in this tutorial, and the data that we'll be using to illustrate it.

### The data problem we're solving in this tutorial

In this tutorial we will be looking at two sets of data representing the same information over different periods of time.  We will use the values of the first set of data to populate the rules that we expect this data to follow in the future.  We will then use these Expectations to determine if there is a problem with the second set of data.

The data we're going to use for this tutorial is the [NYC taxi data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). This is an open data set which is updated every month. Each record in the data corresponds to one taxi ride and contains information such as the pick-up and drop-off location, the payment amount, and the number of passengers, among others.

In this tutorial, we provide two CSV files, each with a 10,000 row sample of the Yellow Taxi Trip Records set:

- **yellow_tripdata_sample_2019-01.csv**: a sample of the January 2019 taxi data

- **yellow_tripdata_sample_2019-02.csv**: a sample of the February 2019 taxi data

For purposes of this tutorial, we are treating the January 2019 taxi data as our "current" data, and the February 2019 taxi data as "future" data that we have not yet looked at.  We will use Great Expectations to build a profile of the January data and then use that profile to check for any unexpected data quality issues in the February data.  In a real-life scenario, this would ensure that any problems with the February data would be caught (so it could be dealt with) before the February data is used in a production application!

It should be noted that in the tutorial we only have one month's worth of "current" data.  However, you can use Multi-Batch Profilers to build profiles of multiple past or current sets of data.  Doing so will generally result in a more accurate data profile but for this small example a single set of "current" data will suffice.

### Getting started with the Getting Started Tutorial

Now that you have the background for the data we're using and what we want to do with it, we're ready to start the tutorial in earnest.

Remember the icons for the four steps we'll be going through?

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

Great! You should know: The icon associated with each of these steps will also be displayed on any related documentation.  So if you do follow links into more detailed discussions of anything we introduce you to, you will be able to find your way back to the step you were on with ease.

And now it looks like you're ready to move on to [Step 1: Setup.](./tutorial_setup.md)
