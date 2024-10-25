---
sidebar_label: 'Volume'
title: 'Manage data volume with GX'
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Data volume, a critical aspect of data quality, refers to the quantity of records or data points within a dataset. Managing data volume effectively is crucial for maintaining data integrity, ensuring system performance, and deriving accurate insights. Unexpected changes in data volume can signal issues in data collection, processing, or storage, potentially leading to skewed analyses or system failures. Volume management is intrinsically linked to other aspects of data quality, such as data completeness and consistency, forming a crucial part of a comprehensive data quality strategy.

Great Expectations (GX) offers a powerful set of tools for monitoring and validating data volume through its volume-focused Expectations. By integrating these Expectations into your data pipelines, you can establish robust checks that ensure your datasets maintain the expected volume, catch anomalies early, and prevent downstream issues in your data workflows.

This guide will walk you through leveraging GX to effectively manage and validate data volume, helping you maintain high-quality, reliable datasets.

## Prerequisite knowledge

This article assumes basic familiarity with GX components and workflows. If you're new to GX, start with the [GX Overview](/core/introduction/gx_overview.md) to familiarize yourself with key concepts and setup procedures.

## Data preview

The examples in this article use a sample financial transaction dataset that is provided from a public Postgres database table. The sample data is also available in [CSV format](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/volume_financial_transfers.csv).

| transfer_type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_ts       |
|----------|------------------------|--------------------|-----------------|---------------------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    |
| domestic | 954005011218           | Nelson O’Connell   | 3285.21         | 2024-05-01 05:08    |

This dataset represents daily financial transactions. In a real-world scenario, you'd expect a certain volume of transactions to occur each day.

## Key volume Expectations

GX provides several Expectations specifically designed for managing data volume. These can be added to an Expectation Suite via the GX Cloud UI or using the GX Core Python library.

![Add a volume Expectation in GX Cloud](./volume_resources/gx_cloud_volume_expectations_add.gif)

### Expect Table Row Count To Be Between

Ensures that the number of rows in a dataset falls within a specified range.

**Use Case**: Validate that transaction volumes are within expected bounds, alerting to unusual spikes or drops in activity.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py ExpectTableRowCountToBeBetween"
```

<small>View `ExpectTableRowCountToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_table_row_count_to_be_between).</small>


###  Expect Table Row Count To Equal

Verifies that the dataset contains exactly the specified number of records.

**Use Case**: Ensure that a specific number of records are processed, useful for batch operations or reconciliation tasks.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py ExpectTableRowCountToEqual"
```

<small>View `ExpectTableRowCountToEqual` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_table_row_count_to_equal).</small>


### Expect Table Row Count To Equal Other Table

Compares the row count of the current table to another table within the same database.

**Use Case**: Verify data consistency across different stages of a pipeline or between source and target systems.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_expectations.py ExpectTableRowCountToEqualOtherTable"
```

<small>View `ExpectTableRowCountToEqualOtherTable` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_table_row_count_to_equal_other_table).</small>

<br/>
<br/>

:::tip[GX tips for volume Expectations]
- Regularly adjust your `ExpectTableRowCountToBeBetween` thresholds based on historical data and growth patterns to maintain relevance.
- Use `ExpectTableRowCountToEqual` in conjunction with time-based partitioning for precise daily volume checks.
- Implement `ExpectTableRowCountToEqualOtherTable` to ensure data integrity across your data pipeline stages.
:::

## Example: Validate daily transaction volume

**Context**: In SQL tables, data is often timestamped on row creation. Tables can hold historical data created over long ranges of time, however, organizations generally want to validate volume for a specific time period: over a year, over a month, over a day. When data arrives on a regular cadence, it is also useful to be able to monitor volume over the most recent window of time.

**Goal**: Using the `ExpectTableRowCountToBeBetween` Expectation and either GX Core or GX Cloud, validate daily data volume by batching a single Data Asset (a Postgres table) on a time-based column, `transfer_ts`.

<Tabs
   defaultValue="gx_cloud"
   values={[
      {value: 'gx_core', label: 'GX Core'},
      {value: 'gx_cloud', label: 'GX Cloud'}
   ]}
>

<TabItem value="gx_core" label="GX Core">
Run the following GX Core workflow.

```python title="" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/volume_resources/volume_workflow.py full example code"
```

**Result**:
| date | expectation passed | observed rows |
| :--- | :--- | :--- |
| 2024-05-01 | True | 4 |
| 2024-05-02 | True | 5 |
| 2024-05-03 | True | 5 |
| 2024-05-04 | True | 5 |
| 2024-05-05 | True | 5 |
| 2024-05-06 | False | 6 |
| 2024-05-07 | True | 5 |

</TabItem>

<TabItem value="gx_cloud" label="GX Cloud">

Use the GX Cloud UI to walk through the following steps.

1. Create a Postgres Data Asset for the `volume_financial_transfers` table, using the connection string:
   ```
   postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality
   ```

2. Profile the Data Asset.
3. Add an **Expect table row count to be between** Expectation to the freshly created Data Asset.
4. Populate the Expectation:
   * Define a **Daily** batch interval for the Expectation, using `transfer_ts` as the **Batch column**.
   * Provide a **Min Value** of `1` and a **Max Value** of `5`.
5. Save the Expectation.
6. Click the **Validate** button and define which batch to validate.
   * **Latest Batch** validates data for the most recent batch found in the Data Asset.
   * **Custom Batch** validates data for the batch provided.
7. Click **Validate**.
8. Review Validation Results.

</TabItem>
</Tabs>

**GX solution**: GX enables volume validation for yearly, monthly, or daily ranges of data. Data validation can be defined and run using either GX Core or GX Cloud.

## Scenarios

The scenarios in this section outline common real-world use cases for data volume validation, and how GX can be applied to identify and monitor volume issues.

### Data reconciliation across systems

**Context**: In many organizations, data is often stored and processed across multiple systems, such as source systems, data warehouses, and reporting databases. Ensuring data consistency across these systems is crucial for accurate reporting and decision-making. For example, in a banking environment, data might be stored in core banking platforms, data warehouses, and reporting databases, and ensuring consistency across these systems is essential for regulatory compliance and accurate financial reporting.

**GX solution**: Implement checks using `ExpectTableRowCountToEqualOtherTable` to ensure data volume consistency between source and target systems in a data reconciliation process.

### Monitoring data volume in real-time streaming pipelines

**Context**: Many organizations process large volumes of data in real-time for various purposes, such as fraud detection, system monitoring, or real-time analytics. Monitoring data volume in real-time streaming pipelines is essential to ensure that the volume remains within expected bounds and to detect any anomalies promptly. For instance, banks often process large volumes of data in real-time for fraud detection or market monitoring, and detecting volume anomalies quickly is crucial for mitigating risks.

**GX solution**: Implement checks using `ExpectTableRowCountToBeBetween` to monitor data volume in real-time streaming pipelines and alert when anomalies are detected.

### Batch processing verification

**Context**: In batch processing systems, it is important to verify that each batch contains the expected number of records to ensure complete processing. This is applicable across various industries, such as retail, where sales transactions might be processed in batches, or in healthcare, where patient records might be updated through batch processes. Ensuring that each batch contains the expected number of records is crucial for maintaining data integrity and avoiding data loss.

**GX solution**: Validate data using `ExpectTableRowCountToEqual` to ensure that each processed batch contains exactly the expected number of records.


## Avoid common volume validation pitfalls

- **Static Thresholds**: Avoid using fixed thresholds that don't account for natural growth or seasonality. Regularly review and adjust your `ExpectTableRowCountToBeBetween` parameters. For example, an e-commerce platform might need different volume thresholds for regular days versus holiday seasons.

- **Ignoring Data Skew**: Data skew refers to the uneven distribution of data across partitions or nodes in a distributed system. Failing to account for data skew when validating volume can lead to misleading results. Monitor volume at the partition level and implement checks to detect and handle data skew.

- **Ignoring Trends**: Don't overlook gradual changes in data volume over time. Implement trend analysis alongside point-in-time checks. GX can be used in conjunction with time-series analysis tools to detect and alert on unexpected volume trends.

- **Overlooking Granularity**: Ensure volume checks are applied at the appropriate level of granularity (e.g., daily, hourly) to catch issues promptly. For instance, a social media analytics pipeline might require hourly volume checks to detect and respond to viral content quickly.

- **Neglecting Context**: Remember that volume changes might be legitimate due to business events or system changes. Incorporate contextual information when possible. GX can be integrated with external systems to factor in known events or changes when validating volume expectations.

## The path forward

Proactive management and validation of data volume is a key part of ensuring the quality and reliability of your data. Implementing the strategies explored in this article will help you to enhance your data's integrity and trustworthiness.

Volume management is a critical component of data quality, however, it is one facet of a comprehensive data quality strategy. As you continue to iterate on your data quality strategy, leverage the full spectrum of GX capabilities to create a robust, scalable, and trustworthy data ecosystem. Explore our broader [data quality series](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md) to gain insights into how other critical aspects of data quality can be seamlessly integrated into your workflows.
