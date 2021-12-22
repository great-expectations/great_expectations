---
title: How to add SQLAlchemy support for custom Metrics 
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::warning
This guide only applies to Great Expectations versions 0.13 and above, which make use of the new modular Expectation architecture. If you have implemented a custom Expectation but have not yet migrated it using the new modular patterns, you can still use this guide to implement custom renderers for your Expectation.
:::

This guide will help you implement native SQLAlchemy support for your custom Metric. 

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- Configured a [Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- Implemented a [custom Expectation](../../../guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations.md).
    
</Prerequisites>

Steps
-----

1. **Decide which execution engines and dialects you want to implement and enable tests for them**

While SQLAlchemy is able to provide a common interface to a variety of SQL dialects, some functions may not work in a particular dialect, or in some cases they may return different values. To avoid surprises, it's helpful to determine beforehand what backends and dialects you plan to support, and test them along the way. 

Within the `examples` defined inside your Expectation class, the `test_backends` key specifies which backends and SQLAlchemy dialects to run tests for. (If not specified, Great Expectations will attempt to determine the implemented backends automatically, but wll only run SQLAlchemy tests against sqlite.) Add entries corresponding to the functionality you want to add: 
    
````python
examples = [
        {
            "data": {
            ....
            },
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
                {
                    "backend": "sqlalchemy",
                    "dialects": ["mysql", "postgresql"],
                },
                {
                    "backend": "spark",
                    "dialects": None,
                },
            ],
        },
    ]
````

2. **Implement the SQLAlchemy logic for your Expectation.**

Great Expectations provides a variety of ways to implement an Expectation in SQLAlchemy. Some of the most common ones include: 
1.  Defining a partial function that takes a SQLAlchemy column as input
2.  Directly executing queries using SQLAlchemy objects to determine the value of your Expectation's metric directly 
3.  Using an existing metric that is already defined for SQLAlchemy. 

<Tabs
  groupId="metric-type"
  defaultValue='partialfunction'
  values={[
  {label: 'Partial Function', value:'partialfunction'},
  {label: 'Query Execution', value:'queryexecution'},
  {label: 'Existing Metric', value:'existingmetric'},
  ]}>

<TabItem value="partialfunction">

Great Expectations allows for much of the SQLAlchemy logic for executing queries be abstracted away by specifying metric behavior as a partial function. To do this, use one of the decorators @column_aggregate_partial (for column aggregate expectation) , @column_condition_partial (for column map expectations),  @column_pair_condition_partial (for column pair map metrics), or @multicolumn_condition_partial for multicolumn map metrics. The decorated method takes in an SQLAlchemy `Column` object and will either return a `sqlalchemy.sql.functions.Function` or a `ColumnOperator` that Great Expectations will use to generate the appropriate SQL queries. 
    
For example, the `ColumnValuesEqualThree` metric can be defined as: 

````python
@column_condition_partial(engine=SqlAlchemyExecutionEngine)
def _sqlalchemy(cls, column, value_set, **kwargs):
    return column.in_([3])
````
    
</TabItem>
<TabItem value="queryexecution">

The most direct way of implementing a metric is by computing its value from provided SQLAlchemy objects. 
    
````python
@metric_value(engine=SqlAlchemyExecutionEngine, metric_fn_type="value")
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
    
    column_name = accessor_domain_kwargs["column"]
    column = sa.column(column_name)
    sqlalchemy_engine = execution_engine.engine
    dialect = sqlalchemy_engine.dialect
````
    
Here `sqlalchemy_engine` is a SQLAlchemy `Engine` object, whose `execute` method executes arbitrary SQL. The keyword arguments To define the `ColumnValuesEqualThree` metric we could 
````python
    query = sa.select(column.in_([3])).select_from(selectable)
    result = sqlalchemy_engine.execute(simple_query).fetchall()
````

</TabItem> 
<TabItem value="existingmetric">

When using the value of an existing metric, the method signature is the same as when defining a metric value. 
````python 
    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
````    
    
The `metrics` argument that the method is called with will be populated with your metric's dependencies, resolved by calling the `_get_evaluation_dependencies` class method. Suppose we wanted to implement a version of the `ColumnValuesEqualThree` expectation using the `column.value_counts` metric, which is already implemented for the SQLAlchemy execution engine. We would then modify `_get_evaluation_dependencies`: 
    
````python 
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

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            dependencies["column.value_counts"] = MetricConfiguration(
                metric_name="column.value_counts",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )
        return dependencies    
````
Then within the _sqlchemy function, we would add: 

````python
    column_value_counts = metrics.get("column.value_counts")
    return(all(column_value_counts.index==3))
````

</TabItem>
</Tabs> 
