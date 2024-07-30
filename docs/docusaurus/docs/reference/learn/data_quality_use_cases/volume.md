---
sidebar_label: 'Volume'
title: 'Manage data volume with GX'
---

Data volume, a critical aspect of data quality, refers to the quantity of records or data points within a dataset. Managing data volume effectively is crucial for maintaining data integrity, ensuring system performance, and deriving accurate insights. Unexpected changes in data volume can signal issues in data collection, processing, or storage, potentially leading to skewed analyses or system failures. Volume management is intrinsically linked to other aspects of data quality, such as data completeness and consistency, forming a crucial part of a comprehensive data quality strategy.

Great Expectations (GX) offers a powerful set of tools for monitoring and validating data volume through its volume-focused Expectations. By integrating these Expectations into your data pipelines, you can establish robust checks that ensure your datasets maintain the expected volume, catch anomalies early, and prevent downstream issues in your data workflows.

This guide will walk you through leveraging GX to effectively manage and validate data volume, helping you maintain high-quality, reliable datasets for your analytics and decision-making processes.

## Understanding the impact of data volume on quality

Data volume plays a significant role in the overall quality and reliability of your datasets. Some key considerations include:

1. **Consistency**: Unexpected changes in data volume can indicate issues in data collection, processing, or integration, potentially leading to inconsistent or incomplete datasets.
2. **Performance**: Excessively large datasets can strain system resources, leading to slower query times, increased latency, and potential system failures.
3. **Accuracy**: Anomalies in data volume, such as sudden spikes or drops, can skew analytical results and lead to inaccurate insights if not properly addressed.
4. **Compliance**: Certain regulations, such as data retention policies, may require strict control over data volume to ensure compliance and avoid legal repercussions.

By proactively managing and validating data volume, you can mitigate these risks and maintain a high-quality, reliable data ecosystem.

## Prerequisite knowledge

This article assumes basic familiarity with GX components and workflows. If you're new to GX, start with the [GX Overview](https://docs.greatexpectations.io/docs/guides/overview) to familiarize yourself with key concepts and setup procedures.

## Data preview

The examples in this guide use a sample transaction dataset, available as a [CSV file on GitHub](https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/learn_data_quality_use_cases/volume_financial_transfers.csv).


| type     | sender_account_number  | recipient_fullname | transfer_amount | transfer_date       |
|----------|------------------------|--------------------|-----------------|---------------------|
| domestic | 244084670977           | Jaxson Duke        | 9143.40         | 2024-05-01 01:12    |
| domestic | 954005011218           | Nelson O’Connell   | 3285.21         | 2024-05-01 05:08    |

This dataset represents daily financial transactions. In a real-world scenario, you'd expect a certain number of transactions each day.

## Key volume Expectations

GX provides several Expectations specifically designed for managing data volume. These can be added to an Expectation Suite via the GX Cloud UI or using the GX Core Python library.

### Expect Table Row Count To Be Between

Ensures that the number of rows in a dataset falls within a specified range.

**Use Case**: Validate that daily transaction volumes are within expected bounds, alerting to unusual spikes or drops in activity.

```py
gxe.ExpectTableRowCountToBeBetween(
    min_value=1000
    max_value=1500
)
```

<sup>View `ExpectTableRowCountToBeBetween` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_table_row_count_to_be_between).</sup>


###  Expect Table Row Count To Equal

Verifies that the dataset contains exactly the specified number of records.

**Use Case**: Ensure that a specific number of records are processed, useful for batch operations or reconciliation tasks.

```py
gxe.ExpectTableRowCountToEqual(
    value=300
)
```

<sup>View `ExpectTableRowCountToEqual` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_table_row_count_to_equal).</sup>


### Expect Table Row Count To Equal Other Table

Compares the row count of the current table to another table within the same database.

**Use Case**: Verify data consistency across different stages of a pipeline or between source and target systems.

```py
gxe.ExpectTableRowCountToEqualOtherTable(
    other_table_name="transactions_summary"
)
```

<sup>View `ExpectTableRowCountToEqualOtherTable` in the [Expectation Gallery](https://greatexpectations.io/expectations/expect_table_row_count_to_equal_other_table).</sup>


:::tip[GX tips for volume Expectations]
- Regularly adjust your `ExpectTableRowCountToBeBetween` thresholds based on historical data and growth patterns to maintain relevance.
- Use `ExpectTableRowCountToEqual` in conjunction with time-based partitioning for precise daily volume checks.
- Implement `ExpectTableRowCountToEqualOtherTable` to ensure data integrity across your data pipeline stages.
:::

## Examples and scenarios

### Data reconciliation across systems

**Context**: In many organizations, data is often stored and processed across multiple systems, such as source systems, data warehouses, and reporting databases. Ensuring data consistency across these systems is crucial for accurate reporting and decision-making. For example, in a banking environment, data might be stored in core banking platforms, data warehouses, and reporting databases, and ensuring consistency across these systems is essential for regulatory compliance and accurate financial reporting.

**GX solution**: Implement checks to ensure data volume consistency between source and target systems in a data reconciliation process.

```python
gxe.ExpectTableRowCountToEqualOtherTable(
    other_table_name="target_system_transactions"
)
```

### Monitoring data volume in real-time streaming pipelines

**Context**: Many organizations process large volumes of data in real-time for various purposes, such as fraud detection, system monitoring, or real-time analytics. Monitoring data volume in real-time streaming pipelines is essential to ensure that the volume remains within expected bounds and to detect any anomalies promptly. For instance, banks often process large volumes of data in real-time for fraud detection or market monitoring, and detecting volume anomalies quickly is crucial for mitigating risks.

**GX solution**: Implement checks to monitor data volume in real-time streaming pipelines and alert when anomalies are detected.

```python
gxe.ExpectTableRowCountToBeBetween(
    min_value=1000,
    max_value=1500
)
```

### Batch processing verification

**Context**: In batch processing systems, it's important to verify that each batch contains the expected number of records to ensure complete processing. This is applicable across various industries, such as retail, where sales transactions might be processed in batches, or in healthcare, where patient records might be updated through batch processes. Ensuring that each batch contains the expected number of records is crucial for maintaining data integrity and avoiding data loss.

**GX solution**: Validate that each processed batch contains exactly the expected number of records.

```python
gxe.ExpectTableRowCountToEqual(
    value=300
)
```

## Avoid common volume validation pitfalls

- **Static Thresholds**: Avoid using fixed thresholds that don't account for natural growth or seasonality. Regularly review and adjust your `ExpectTableRowCountToBeBetween` parameters. For example, an e-commerce platform might need different volume thresholds for regular days versus holiday seasons.

- **Ignoring Data Skew**: Data skew refers to the uneven distribution of data across partitions or nodes in a distributed system. Failing to account for data skew when validating volume can lead to misleading results. Monitor volume at the partition level and implement checks to detect and handle data skew.

- **Ignoring Trends**: Don't overlook gradual changes in data volume over time. Implement trend analysis alongside point-in-time checks. GX can be used in conjunction with time-series analysis tools to detect and alert on unexpected volume trends.

- **Overlooking Granularity**: Ensure volume checks are applied at the appropriate level of granularity (e.g., daily, hourly) to catch issues promptly. For instance, a social media analytics pipeline might require hourly volume checks to detect and respond to viral content quickly.

- **Neglecting Context**: Remember that volume changes might be legitimate due to business events or system changes. Incorporate contextual information when possible. GX can be integrated with external systems to factor in known events or changes when validating volume expectations.

## Next steps: Advancing volume management

While volume management is a critical component of data quality, it's just one facet of a comprehensive data engineering strategy. To build on the foundations laid in this guide, consider the following steps:

1. Integrate volume-focused Expectations into your CI/CD pipelines, enabling automated checks at key points in your data flow. Follow our step-by-step guide on [integrating Great Expectations with Github Actions](https://greatexpectations.io/blog/github-actions) to streamline your deployment process.

2. Explore the [Expectation Gallery](https://greatexpectations.io/expectations/) to identify Expectations that complement your volume management strategy and address other dimensions of data quality. Experiment with combining different Expectations to create a robust validation framework tailored to your specific data quality challenges.

3. Develop a multifaceted approach that combines volume checks with other [crucial data quality aspects](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md), such as data integrity, schema evolution, and distribution analysis. For instance, consider coupling volume checks with schema validation:

```python
gxe.ExpectTableRowCountToBeBetween(
    min_value=1000
    max_value=1500
)

gxe.ExpectTableColumnsToMatchOrderedList(
    column_list=[
        "sender_account_number",
        "recipient_account_number",
        "transfer_amount",
        "transfer_date",
    ]
)
```

This combination allows you to monitor for unexpected data growth while simultaneously ensuring structural consistency, providing a more robust validation framework.

4. Implement a systematic review process for your Expectations suite, ensuring it evolves alongside your data architecture and business requirements. Schedule regular meetings with stakeholders to discuss data quality objectives, review Expectation performance, and identify areas for improvement.

5. Foster collaboration between data engineers, data analysts, and other stakeholders in developing and refining your data quality strategy. Encourage open communication and knowledge sharing to ensure that your data quality initiatives align with the needs of the broader organization. Consider sharing validation results from Great Expectations, which is designed to be useful as stand-alone data documentation, with other stakeholders outside of your team.

:::TODO GX link for #5?:::

As you continue to iterate on your data quality strategy, leverage the full spectrum of Great Expectations capabilities to create a robust, scalable, and trustworthy data ecosystem. Explore our broader [data quality series](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md) to gain insights into how other critical aspects of data quality can be seamlessly integrated into your engineering workflows.

By following these actionable steps and embracing a collaborative, multifaceted approach to data quality, you'll be well-equipped to tackle the ever-evolving challenges of modern data engineering. Together, let's build a future where high-quality, reliable data drives innovation and success across industries.
