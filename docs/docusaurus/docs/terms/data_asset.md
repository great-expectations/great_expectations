---
title: "Data Asset"
---

import TechnicalTag from '../term_tags/_tag.mdx';

A Data Asset is a collection of records within a <TechnicalTag relative="../" tag="datasource" text="Data Source" /> which is usually named based on the underlying data system and sliced to correspond to a desired specification.

Data Assets are used to specify how Great Expectations will organize data into <TechnicalTag relative="../" tag="batch" text="Batches" />.

Data Assets are usually tied to existing data that already has a name (e.g. “the UserEvents table”). In many cases, Data Assets slice the data one step further (e.g. “new records for each day within the UserEvents table.”) To further illustrate with some examples: in a SQL database, rows from a table grouped by the week they were delivered may be a data asset; in an S3 bucket or filesystem, files matching a particular regex pattern may be a data asset. 

The specifics of a filesystem Data Asset are defined by the parameters provided when it is created. With a SQL Data Asset, you can also add splitters after you have initially created the Data Asset.

You can define multiple Data Assets built from the same underlying source data system to support different workflows such as interactive exploration and creation of <TechnicalTag relative="../" tag="expectation" text="Expectations" />, the use of <TechnicalTag relative="../" tag="profiler" text="Profilers" /> to analyze data, and ongoing <TechnicalTag relative="../" tag="validation" text="Validation" /> through <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" />.

Great Expectations is designed to help you think and communicate clearly about your data. To do that, we need to rely on some specific ideas about *what* we're protecting with our Expectations. You usually do not need to think about these nuances to use Great Expectations, and many users never think about what *exactly* makes a Data Asset or Batch. But we think it can be extremely useful to understand the design decisions that guide Great Expectations.

Great Expectations protects the quality of **Data Assets**. A **Data Asset** is a logical collection of records. Great Expectations
consumes and creates **metadata about Data Assets**.

- For example, a Data Asset might be *a user table in a database*, *monthly financial data*, *a collection of event log
  data*, or anything else that your organization uses.

How do you know when a collection of records is *one* Data Asset instead of two Data Assets or when two collections of
records are really part of the same Data Asset? In Great Expectations, we think the answer lies in the user. Great
Expectations opens insights and enhances communication while protecting against pipeline risks and data risks, but that
revolves around a *purpose* in using some data (even if that purpose starts out as "I want to understand what I have
here"!).

We recommend that you call a collection of records a Data Asset when you would like to track metadata (and especially, *
Expectations*) about it. **A collection of records is a Data Asset when it's worth giving it a name.**

Since the purpose is so important for understanding when a collection of records is a Data Asset, it immediately follows
that *Data Assets are not necessarily disjoint*. The same data can be in multiple Data Assets. You may have different
Expectations of the same raw data for different purposes or produce documentation tailored to specific analyses and
users.

- Similarly, it may be useful to describe subsets of a Data Asset as new Data Assets. For example, if we have a Data Asset
  called the "user table" in our data warehouse, we might also have a different Data Asset called the "Canadian User
  Table" that includes data only for some users.

Not all records in a Data Asset need to be available at the same time or place. A Data Asset could be built from *
streaming data* that is never stored, *incremental deliveries*, *analytic queries*, *incremental updates*, *replacement
deliveries*, or from a *one-time* snapshot.

That implies that a Data Asset is a **logical** concept. Not all of the records may be **accessible** at the same time.
That highlights a very important and subtle point: no matter where the data comes from originally, Great Expectations
validates **batches** of data. A **batch** is a discrete subset of a Data Asset that can be identified by a some
collection of parameters, like the date of delivery, value of a field, time of validation, or access control
permissions.

## Relationship to other objects

A Data Asset is a collection of records that you care about, which a Data Source is configured to interact with.  Batches are subsets of Data Asset records.  When a <TechnicalTag relative="../" tag="batch_request" text="Batch Request" /> is provided to a Data Source, the Data Source will reference its Data Asset in order to translate the Batch Request into a query for a specific Batch of data.

For the most part, Data Assets are utilized by Great Expectations behind the scenes.  Other than configuring them, you will rarely have to interact with them directly (if at all).

## Use cases

When connecting to your data you will define at least one Data Asset for each Data Source you create.  From these Data Assets you will be able to create Batch Requests, which will specify the data that you provide to your Expectations.

When using the interactive workflow for creating Expectations, it is often useful to utilize a simple Data Asset configuration for purposes of exploring the data.  This configuration can then be replaced when it is time to Validate data going forward.

When using a Profiler, Great Expectations can take advantage of as much information about your data as you can provide.  It may even be useful to configure multiple Data Assets to support complex Profiler configurations.

When you are Validating new Batches of data you'll be using a Data Asset and Batch Request to indicate the Batch or Batches of data to Validate.

## Versatility

Data Assets can be configured to cover a variety of use cases.  For example:

- For append-only datasets such as logs, you configure a Data Asset to split data into batches corresponding to the intervals that you want the profiler to evaluate. For example, you could configure a log-style Data Asset to divide the data into Batches corresponding to single months.
- When you are working with dynamic datasets such as database tables, you will configure a Data Asset that defines a Batch as the state of the table at the time of Validation. Because you usually cannot access previous states of the such a table, only a single Batch from that Data Asset will be available at any given time. In that case, you have a few options:
    - You can profile the data using a single Batch of data.  While you may not have as much statistical power in the estimated Expectations as if you had historical data available, this allows you to get started immediately, and you can build up a collection of Metrics over time that Great Expectations can use to refine the Expectations.
    - You can configure another Data Asset from the same source Data that randomly assigns data to different batches to bootstrap the refinement of Expectations by providing a random sampling of records.
    - You can configure another Data Asset from the same source data that splits the data into batches based on some keys in the data, such as load times or update times.
- For working with *streaming* data, we recommend that you pull a sample of data into a permanent store and first profile your data using that data sample.  You will be able to configure a Data Asset for the data sample just as you would for an append-only dataset.  When it is time to Validate the data, you will use a second Data Asset that is configured to directly connect to your streaming data platform.

## Flexibility

Data Assets are logical constructs based around how you want to define and subdivide your data.  Generally speaking, a collection of data that you want to collect metadata about can be considered a Data Asset.  As a rule of thumb: a set of data is a Data Asset when it is *worth giving a name.*  Because of this, it is entirely possible to define multiple Data Assets to subdivide the same set of data in different ways, or even that exclude some of the source data.

For instance, imagine you are doing analysis on sales data for cars.  You could create a Data Asset named "recent_sales" that only provides a batch for the most recent group of sold cars.  Or a Data Asset named "monthly_reports" that groups your source data system's records into Batches by month.  That same data could also be included in a Data Asset called "yearly_reports" that groups the data by year, and another Data Asset called "make_and_model" that groups records by those criteria, or a Data Asset called "local_sales" that groups records by dealership.  However it is that you want to analyze your data, you can configure a Data Asset to do so.

## Access

You will not typically need to directly access a Data Asset.  Great Expectations validates Batches, and your Data Assets will be used behind the scenes in conjunction with Datasources and Batch Requests to provide those Batches.  However, you yourself will not need to work with the Data Asset beyond configuring it in a Data Source.

## Create

You will not need to manually create a Data Asset.  Instead, they will be created from the configuration you provide for a Data Source when that Data Source needs them.

## Configure

Data Assets are configured by providing parameters when they are created.  SQL-based Data Assets can be further configured after creation by calling their methods for adding splitters. To configure Data Assets for various environments and source data systems, see [Connect to source data](../guides/connecting_to_your_data/connect_to_data_lp.md).
