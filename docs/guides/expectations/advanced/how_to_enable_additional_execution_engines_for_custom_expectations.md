---
title: How to Enable Additional Execution Engines for Custom Expectations
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::warning
This guide only applies to Great Expectations versions 0.13 and above, which make use of the new modular Expectation architecture. If you have implemented a custom Expectation but have not yet migrated it using the new modular patterns, you can still use this guide to implement custom renderers for your Expectation.
:::

This guide will help you implement additional execution engines for your custom Expectations, allowing them to work natively with SQLAlchemy and Spark. 

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- Configured a [Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- Implemented a [custom Expectation](../../../guides/expectations/creating_custom_expectations/how_to_create_custom_expectations.md).
    
</Prerequisites>

Steps
-----

1. **First, decide which execution engines and dialects you need to implement.**

2. **Implement the SQLAlchemy logic for your metric.**

Great Expectations provides a variety of ways to implement an Expectation in SQLAlchemy. Some of the most common ones include: 
1.  Computing the value of your metric directly from SQLAlchemy objects
2.  Defining a partial function that takes a SQLAlchemy column as input
3.  Using an existing metric that is already defined for SQLAlchemy. 

<Tabs
  groupId="-type"
  defaultValue='columnmap'
  values={[
  {label: 'Metric Value', value:'metricvalue'},
  {label: 'Partial Function', value:'partialfunction'},
  {label: 'Existing Metric', value:'existingmetric'},
  ]}>

<TabItem value="metricvalue">
</TabItem> 
  

<TabItem value="partialfunction">
</TabItem> 
  
<TabItem value="existingmetric">\
When using the value of an existing metric, the method signature is the same as when defining a metric value. 
```python
    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
```    
    
The `metrics` argument that the method is called with will be populated with your metric's dependencies, resolved by calling the `_get_evaluation_dependencies` class method. Suppose we wanted to implement a version of the `ColumnValuesEqualThree` expectation using the `column.value_counts` metric. We would then modify `_get_evaluation_dependencies`: 
    
```python 
    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):

        dependencies = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        table_domain_kwargs = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            dependencies["column.value_counts"] = MetricConfiguration(
                metric_name="column.value_counts",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )
        return dependencies    
```
Then within the _sqlchemy function, we would add: 

```python
    column_value_counts = metrics.get("column.value_counts")
    return(all(column_value_counts.index==3))
```
</TabItem>
