---
title: How to choose between working with a single or multiple Batches of data
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you decide when you should use a single Batch and when it is beneficial to use multiple Batches, instead.  Building on that knowledge, this guide will also explain the principles behind configuring a Datasource's Data Asset to either only allow for a single Batch to be returned when requested, or to potentially include multiple Batches.  Finally, this guide will discuss how to configure Batch Requests to return a specific Batch or subset of Batches when you are requesting data from a Data Asset that has been configured to include multiple Batches, so that you can get the most versatility out of your Datasource configurations.

By the end of this guide, you will know when it will be most beneficial to be working with a single Batch of data, when it will be most beneficial to be working with multiple Batches of data, and what you need to do in order to ensure your Datasource and Batch Request configurations permit returning multiple Batches or ensure the return of only one.

## Prerequisites

<Prerequisites>

- [An understanding of Datasource basics](../../terms/datasource.md)
- [An understanding of how to request data with a Batch Request](/docs/0.15.50/guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource)

</Prerequisites>

## Steps

The steps of this guide will allow you to quickly determine if you will want to use a single Batch or multiple Batches when creating Expectations and performing Validations.  This knowledge will then inform how you configure Data Assets in your Datasources.

### 1. Determine if a single Batch or multiple Batches of data will be most beneficial in your future use cases

#### When to work with a single Batch of data:

In some cases, a single Batch of data is all you need (or all that is supported).  In particular:
- When you are Validating data, you will need to specify a single Batch of data.  If you provide multiple Batches in a Batch Request, Validations will default to operating on the last Batch in the list.
- If you want to quickly create some Expectations as part of an early Expectation Suite or exploratory data analysis, a single Batch is likely to be all that you need.

:::note 

When Validating data you may include multiple *Batch Requests* in a single Checkpoint.  But only one Batch out of each Batch Request will be used in the Validation process.

:::

#### When to work with multiple Batches of data:

If you are creating Expectations, you will benefit from ensuring your Data Assets and Batch Requests can return multiple Batches when:
- You will be using a Data Assistant to generate an Expectation Suite that is populated with Expectations for you.
- You will be using auto-initializing Expectations to automatically generate the parameters for Expectations that you individually define and add to an Expectation Suite.

This is particularly true if you are working with Expectations that involve statistical analysis on a per-Batch basis.

For example, let's say you are working with the NYC taxi data and intended to define an Expectation to validate that the median trip distance of rides taken within a given month falls in a specific range. You would first need to ensure you could divide your data up into months for analysis, which you could do by configuring your Datasource to define an individual Data Asset for each month.  Then, you *could* analyze each month individually to determine what the appropriate minimum and maximum values for your Expectation would be.  But that would be a lot of work that Great Expectations can automate for you.

Instead, you can configure your Datasource to define a single Data Asset that groups each month's worth of data into a discrete Batch.  Then, you can use a single Batch Request to retrieve that list of Batches. By feeding that Batch Request into the auto-initializing Expectation `expect_column_median_to_be_between` and setting the parameters `(column="trip_distance", auto=True)` you can have Great Expectations analyze each Batch and create an Expectation that has appropriate minimum and maximum values for you, saving you a lot of repetitive work.

Alternatively, if you know you will need additional Expectations, you can feed that Batch Request into a Data Assistant, which will generate an entire Expectation Suite with Expectations that have had their parameters determined from the analysis of the provided Batches... saving you from even more repetitive work. Huzzah for automation!

### 2. Determine if your Datasource should include Data Asset configurations that permit multiple Batches

If you have determined that your future use case will benefit from being able to analyze multiple Batches of data at once, then you will necessarily want to ensure that your Datasource is capable of returning multiple Batches.

Otherwise, it is sufficient for your Datasource to define Data Assets that are only capable of returning a single Data Asset.

#### Getting more versatility out of a Data Asset that is configured to return multiple Batches:

However, there is a caveat.  When you create a Batch Request, you may configure it to return a specific Batch from a Data Asset even if that Data Asset returns multiple Batches by default.  This feature means that if you have use cases that would benefit from working with multiple Batches of data and use cases that benefit from working with a single Batch of data you don't necessarily have to define two Data Assets in your Datasource configuration.

Let's look at the NYC taxi data example from before.  In that example, we determined that working with multiple Batches would benefit us because we wanted to do a per-Batch statistical analysis when creating an Expectation.  However, at the end of the day we are going to want to Validate future data against that Expectation, and the Validation process (as you read above) only supports operating on a single Batch at a time.

Does this mean you need to define two Data Assets, one that is configured to return one Batch of data for each month (for creating your Expectation) and one that is configured to only return the most recent month's data (for Validation purposes)?

No!

As long as the data you want to Validate corresponds to a single Batch in your Data Asset, you can use a Batch Request's ability to limit the returned Batches to specify a single Batch for Validation or any other use.

#### When you *must* configure your Data Asset to only permit single Batch of data to be returned:

There are only two cases where you *must* configure your Data Asset to correspond to at most a single Batch of data.

The first of these is when you are working with a Runtime Data Connector. The Runtime Data Connector puts a wrapper around a single Batch of data, and therefore does not support Data Asset configurations that permit the return of more than one Batch of data.

The second is when you are using a Sql Datasource and you want to work with a single Batch of data that would otherwise be split between Batches in a Data Asset that was configured to permit the return of more than one Batch.  

The reason that this case only applies to Sql Datasources is due to the difference in how Batches are handled in File System Datasources and Sql Datasources.

#### How do Sql Datasources and File System Datasources differ in how they handle Batches?

Data Asset configurations that permit the return of more than one Batch of data function differently between File System Datasources and Sql Datasources.  This is due to differences in how files are handled by Pandas and PySpark versus how tables are handled by SqlAlchemy.

In a File System Datasource:
- A Data Asset may consist of one or more files.
- A Batch of data corresponds to a single file.
- You cannot split a single file into multiple Batches.
- You cannot combine multiple files into a single Batch.

In a Sql Datasource:
- A Data Asset always consists of the data contained in a single Table.
- A Batch of data corresponds to a discrete portion of the data in the table associated with a given Data Asset (including, potentially, *all* of the data in the table).
- You cannot combine multiple tables as multiple Batches in a single Data Asset.
- You cannot combine multiple tables into a single Batch.

This means that in a File System Datasource, a Data Asset that permits the return of more than one Batch does so by combining multiple files and treating each file as a separate Batch in the Data Asset.  You can therefore always specify the return of a single file's data in a Batch Request by specifying the return of the Batch that corresponds to that file.

However, in a Sql Datasource, a Data Asset that permits the return of more than one Batch does so by splitting the contents of a single table based on a provided criteria and having each Batch correspond to a discrete portion of the split up data.  Therefore, if you have use cases for both splitting a table into Batches and working with the entire table as a single Batch the best practice is to define two Data Assets for the table: one that includes the configuration to split the table into Batches, and one that does not.

### 3. Next Steps

Congratulations!  At this point you should have a solid understanding of when to work with a single Batch of Data and when to work with multiple Batches of Data.  You should also know when you can use a single Data Asset that permits the return of multiple Batches to cover both workflows, and when you need to define and use a Data Asset that only corresponds to a single Batch.  Using this knowledge, your next step is to actually define the Datasources and Data Assets that you will be requesting Batches from in the future.

## Additional Information

For more detailed information on how to use Batch Requests to return a specific Batch of data from a configured Datasource, please see:
- [How to get one or more Batches of data from a configured Datasource](/docs/0.15.50/guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource)