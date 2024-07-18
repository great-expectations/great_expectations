---
sidebar_label: 'Missingness'
title: 'Missingness'
---

# Missingness

Missingness means that some data the user expected is not available in the dataset. It is often described as “NULL” because of the SQL standard for describing missing data, but how the missingness is represented varies in different systems.

## Type 1: Intermittent Missing Values
### Example Problem Scenarios
An enrichment system went down. For example, a common step in data processing pipelines is to add geolocation data to event records, based on the IP address recorded in the log. If the geo-resolver is down or slow, the geographic data may simply be omitted from the record to avoid adding a delay to the overall streaming system.

### Current GX Approach
In Great Expectations, specifically declare that we expect this value to be populated, at least 99% of the time. That allows some records to fail without it being a critical problem. By specifically including a threshold less than 100%, we are intentionally signaling that downstream systems must be able to handle missing records, BUT that if many records in a given batch are missing, we may want to re-run the entire batch after fixing our enrichment system.

```py
validator.expect_column_values_to_not_be_null('geolocation_country', mostly=0.99)
validator.expect_column_values_to_not_be_null('geolocation_city', mostly=0.99)
```

## Type 2: Missing Critical Data
### Example Problem Scenarios
An upstream data team did not populate a field. For example, another team had a temporary outage in their system that caused one or more batches of data to be missing an important field, such as the computed closing price in a finance dataset.
A default value was removed from an upstream system. For example, a system that used to automatically populate the updated_by field now provides missing values.
A join table was missing a case. For example, a new item_id was introduced into an orders table but the item_id -> item_name table was not updated. When an upstream system joins on item_id to add name, some item_name values will be missing.
### Current GX Approach
In Great Expectations, specifically declare fields that must be present.

```py
validator.expect_column_values_to_not_be_null('closing_price')
```

## Type 3: New Cases Appear
### Example Problem Scenarios:
An event data system was changed, causing null values to appear in a field that should only have null values based on the value of a different field. For example, the last_visited field might initially be populated only when the action is logout, but with the addition of a new type of action, last_visited should also be populated.
### Current Approach
In Great Expectations, define a conditional expectation.

```py
validator.expect_column_values_to_not_be_null(
'last_visited',
row_condition='action=="logout"',
condition_parser='pandas'
)
```


## Type 4a: Anomalies in the System being Monitored
Note that this use case is significantly different from the others, because it reflects an expectation about the system being monitored. That is, in this case, GX is being asked to perform a more classical alerting/observability function, not just a data quality function.
### Example Problem Scenarios
An increase in error rates means that a field that is not usually populated is now populated. For example, the errors field in a graphql query response log begins having values.
### Current GX Approach
In Great Expectations, specifically declare fields that must be null.

```py
validator.expect_column_values_to_be_null('errors')
```

## Type 4b: Seasonal Anomalies in the System being Monitored
### Example Problem Scenarios
A system has a probabilistic error, and the error probability is linked to another factor. For example, a claims log system runs a report that is enriched with user data, and after a surge of new signups, the number of claims that are missing user data increases.

### Current GX Approach
To address this use case with GX today requires using evaluation parameters.

```py
validator.expect_column_values_to_not_be_null(
'user_info',
mostly={'$PARAMETER': 'current_season_nullity'
)
context.run_checkpoint(
'claims',
evaluation_parameters={
	"current_season_nullity": get_current_season_nullity()
},
)
```

## Competitors' Approaches
Stanley, Jeremy. “When Data Disappears.” https://www.anomalo.com/post/when-data-disappears
“the most common data quality issue is that data has simply disappeared..”
“To ensure that data is available and complete, we run the following sequence of tests:

1. Are there any rows from yesterday?
2. Is the row count above a predicted lower bound?
3. Is there any missing data at the very end of the day?
4. Are there any key segments with far fewer than the expected number of rows?

“...We also note that this was the first time this table had failed to load data on time in the last 34 runs, providing users with a sense of just how unusual or extreme this behavior is…”
“...When it is relevant, we show a breakdown of exactly which segments are appearing less frequently than expected:...”

