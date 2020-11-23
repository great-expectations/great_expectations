**The Definitive Checklist to Building Your Own Custom Expectations**

In 0.13, building a custom Expectation is far easier than ever before  - a new Modular structure to Expectations has allowed 
GE to remove much of the boilerplate that once hampered the implementation of a custom Expectation (for a far more detailed look, 
please read *An Introductory Reference Guide to Modular Expectation Logic*). Now, we walk through the process of implementing our own
modular expectations!


**Step 1: Plan Metric Dependencies**

In the new Modular Expectation design, Expectations rely on Metrics defined by separate MetricProvider Classes, which are then referenced within the Expectation and used for computation.

Once you’ve decided on an Expectation to implement, think of the different aggregations, mappings, or metadata you’ll need to validate your data within the Expectation - each of these will be a separate metric that must be implemented prior to validating your Expectation. 

For example, in trying to compute a Z-score for each one of a column’s values and validating that it is between a min and max threshold, one would need to compute: the mean, the standard deviation, the z-score itself (which depends on the mean and standard deviation), and then check whether the z-scores are between the thresholds - all of which are separate metrics. 

Fortunately, many Metrics have already been implemented for pre-existing Expectations, so it is very possible you will find that the Metric you’d like to implement already exists within the GE framework and can be readily deployed within your Expectation. Even if it doesn’t, it is always worthwhile to check!


**Step 2: Implement your Metric (Sometimes)**

If your metric does not yet exist within the framework, you will need to implement it yourself within a new file - a task that is quick and simple within the new modular framework. 

Below lies the full implementation of an aggregate metric class, with implementations for Pandas, SQLAlchemy, and Apache Spark dialects:

from great_expectations.execution_engine import (
   PandasExecutionEngine,
   SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
   SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
   ColumnMetricProvider,
   column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.import_manager import F

class ColumnMean(ColumnMetricProvider):
   """MetricProvider Class for Aggregate Mean MetricProvider"""

   metric_name = "column.aggregate.mean"

   @column_aggregate_metric(engine=PandasExecutionEngine)
   def _pandas(cls, column, **kwargs):
       """Pandas Mean Implementation"""
       return column.mean()

   @column_aggregate_metric(engine=SqlAlchemyExecutionEngine)
   def _sqlalchemy(cls, column, **kwargs):
       """SqlAlchemy Mean Implementation"""
       return sa.func.avg(column)

   @column_aggregate_metric(engine=SparkDFExecutionEngine)
   def _spark(cls, column, **kwargs):
       """Spark Mean Implementation"""
       return F.mean(column)

As seen above, the class merely provides implementations for different dialects (named after the dialects themselves) and define a metric name. All parameters are provided to the implementation by the Great Expectations framework, and your function implementations will be accessed by the proper dialects to return a result once the data is fed in.

Most other implementations are similarly trivial and many examples can be found in the Dictionary of Metrics.


**Step 3: Define Parameters**

We have already reached the point where we can start building our Expectation! 

The structure of a Modular Expectation now exists within its own specialized class - indicating it will usually exist in a separate file from the Metric. This structure has 3 fundamental components: Expectation Parameters, Dependency Validation, and Expectation Validation. In this step, we will address setting up our parameters.

The parameters of an Expectation consist of the following:
Metric Dependencies - A tuple consisting of the names of all metrics necessary to evaluate the Expectation.
Success Keys - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success.
Examples: Thresholds, Value Sets to validate data against, etc.
Default Kwarg Values (Optional)  -  Default values for success keys and the defined domain, among other values.
An example of Expectation Parameters is shown below (notice that we are now in a new Expectation class and building our Expectation in a separate file from our Metric): 

class ExpectColumnMaxToBeBetween(ColumnExpectation):
   # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
   metric_dependencies = ("column.aggregate.max",)
   success_keys = ("min_value", "strict_min", "max_value", "strict_max")

   # Default values
   default_kwarg_values = {
       "row_condition": None,
       "condition_parser": None,
       "min_value": None,
       "max_value": None,
       "strict_min": None,
       "strict_max": None,
       "mostly": 1,
       "result_format": "BASIC",
       "include_config": True,
       "catch_exceptions": False,
   }
Notice that this class is of type ColumnExpectation, indicating that the Expectation Validation will be done on a column.

**Step 4: Validate Configuration**

We have almost reached the end of our journey in implementing an Expectation! Now, if we have requested certain parameters from the user, we would like to validate that the user has entered them correctly via a validate_configuration method:

def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
   """
   Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
   necessary configuration arguments have been provided for the validation of the expectation.

   Args:
       configuration (OPTIONAL[ExpectationConfiguration]): \
           An optional Expectation Configuration entry that will be used to configure the expectation
   Returns:
       True if the configuration has been validated successfully. Otherwise, raises an exception
   """
   min_val = None
   max_val = None

   # Setting up a configuration
   super().validate_configuration(configuration)
   if configuration is None:
       configuration = self.configuration

   # Ensuring basic configuration parameters are properly set
   try:
       assert (
           "column" in configuration.kwargs
       ), "'column' parameter is required for column map expectations"
   except AssertionError as e:
       raise InvalidExpectationConfigurationError(str(e))

   # Validating that Minimum and Maximum values are of the proper format and type
   if "min_value" in configuration.kwargs:
       min_val = configuration.kwargs["min_value"]

   if "max_value" in configuration.kwargs:
       max_val = configuration.kwargs["max_value"]

   try:
       # Ensuring Proper interval has been provided
       assert (
           min_val is not None or max_val is not None
       ), "min_value and max_value cannot both be none"
       assert min_val is None or isinstance(
           min_val, (float, int)
       ), "Provided min threshold must be a number"
       assert max_val is None or isinstance(
           max_val, (float, int)
       ), "Provided max threshold must be a number"

In this method, given a configuration the user has provided, we check that certain conditions are satisfied by the configuration. For example, if the user has given us a minimum and maximum threshold, it is important to verify that our minimum threshold does not exceed our maximum threshold.

**Step 5: Validate (Sometimes)**

In this final step, we simply need to validate that the results of our metrics meet our Expectations. For Expectations of type ColumnMapDatasetExpectation, which maps a column to a boolean series by asking questions that are fundamentally boolean in nature (Ex: are my column values nonnull?), this is implemented automatically by the GE machinery and does not require user implementation. If your data inquiry can be summed up by a true or false question, feel free to skip this step.

The validate method is implemented as _validate. This method takes a dictionary named Metrics, which contains all metrics requested by your metric dependencies, and performs a simple validation against your success keys (i.e. important thresholds) in order to return a dictionary indicating whether the Expectation has evaluated successfully or not. In order to obtain these success keys, the Expectation parent class has a get_success_kwargs method which returns a dictionary containing all necessary success keys:

def _validate(
   self,
   configuration: ExpectationConfiguration,
   metrics: Dict,
   runtime_configuration: dict = None,
   execution_engine: ExecutionEngine = None,
):
   """Validates the given data against the set minimum and maximum value thresholds for the column max"""
   column_max = metrics.get("column.aggregate.max")

   # Obtaining components needed for validation
   min_value = self.get_success_kwargs(configuration).get("min_value")
   strict_min = self.get_success_kwargs(configuration).get("strict_min")
   max_value = self.get_success_kwargs(configuration).get("max_value")
   strict_max = self.get_success_kwargs(configuration).get("strict_max")

   # Checking if mean lies between thresholds
   if min_value is not None:
       if strict_min:
           above_min = column_max > min_value
       else:
           above_min = column_max >= min_value
   else:
       above_min = True

   if max_value is not None:
       if strict_max:
           below_max = column_max < max_value
       else:
           below_max = column_max <= max_value
   else:
       below_max = True

   success = above_min and below_max

   return {"success": success, "result": {"observed_value": column_max}}

We have now implemented our own Custom Expectations! For more information about Expectations and Metrics, please reference (Link to core concepts).




