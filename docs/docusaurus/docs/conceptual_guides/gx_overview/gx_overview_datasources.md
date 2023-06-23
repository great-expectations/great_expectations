---
title: Datasources
id: gx-overview-datasources
displayed_sidebar: docs
pagination_prev: conceptual_guides/gx_overview/gx-overview-data-context
pagination_next: conceptual_guides/gx_overview/gx-overview-expectations
---

Datasources connect GX to your Source Data System.  Regardless of the original format of your Source Data, a Datasource lets you access it through a consistent API in GX.

## Data Assets

Data Assets are collections of records that you have specified within a Datasource.  GX provides methods for you to define Data Assets that do not strictly correspond to the organization of your source data.  For instance, in a SQL Datasource you can use a Query Asset to define a Data Asset as the records returned by an arbitrary SQL query.  Or, in a Filesystem Datasource, you could use regex to combine the contents of multiple CSV files into a single Data Asset.


You may also define multiple Data Assets built from the same underlying Datasource to support different workflows. For example, if you define a Data Asset for all "user accounts" records in a Datasource connected to your data warehouse, you might also have a different Data Asset in the same Datasource that only contains records for some accounts, such as "paid user accounts".

## Batches

Data Assets can be further partitioned into subsets of data that GX calls Batches.  For instance, you could define a Data Asset for a SQL Datasource as the selection of all records from last year in a given table.  You could then partition that Data Asset into Batches of data that correspond to the records for individual months of the year.

## Batch Requests

A Batch Request is used to specify one or more Batches within a Data Asset.  This provides flexibility in how you work with the data described by a single Data Asset.

For instance, GX can automate the process of running statistical analyzes for multiple Batches of data.  This is possible because you can provide a Batch Request that corresponds to multiple Batches in a Data Asset.

Alternatively, you can specify a single Batch from that same Data Asset so that you do not need to re-run the analyzes on all of your data when you are only interested in a single subset.

## Data and Environment Credentials

Some Datasources will need credentials to access data, such as a PostgreSQL database, or an environment, such as AWS S3.  GX uses string substitution to provide you a secure way to reference your credentials without including them as plain text in your Datasource configurations.

You can use either environment variables or a key in the local configuration file `config_variables.yml` to safely store any passwords needed to connect to your data.

To further ensure security, when a Datasource configuration is saved GX will automatically replace any plain text passwords with variables.  When GX does this, the password is stripped out of your connection string, added to your local `config_variables.yml` file, and replaced in the Datasource's saved configuration with the appropriate reference.