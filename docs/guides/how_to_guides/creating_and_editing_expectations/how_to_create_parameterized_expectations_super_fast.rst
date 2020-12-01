How to Create Parameterized Expectations - Super Fast
_____________________________________________________

This guide will walk you through the process of creating Parameterized Expectations - very quickly. This method is only available using the new Modular Expectations API in 0.13.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  
A Parameterized Expectation is a great new capability unlocked by new Modular Expectations. Now that Expectations are structured in class form, 
it is easier than every before to inherit fromthese classes and build similar Expectations that are adapted to your own needs. 

Steps
_____
1. Select an Expectation to inherit from
########################################

  For the purpose of this excercise, we will implement the new Expectation "expect_column_mean_to_be_positive" - a realistic Expectation of the data that
  can easily inherit from ``expect_column_mean_to_be_between``.

2. Select default values for your class
###########################################################

  As can be seen in the implementation below, we have chosen to keep our default minimum value at 0, given that we are validating that all our
  values are positive, and have set the upper threshold at ∞, allowing any positive value to validate successfully
  
.. code-block:: python

   class ExpectColumnMeanToBePositive(ExpectColumnMeanToBeBetween):

  # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
  metric_dependencies = ("column.mean",)
  success_keys = ("min_value", "strict_min", "max_value", "strict_max")

  # Default values
  default_kwarg_values = {
      "min_value": 0,
      "max_value": numpy.inf,
      "strict_min": True,
      "strict_max": None,
      "result_format": "BASIC",
      "include_config": True,
      "catch_exceptions": False,
  }

3. Use Parent Methods for the Rest of Your Class Implementation
###############################################################

  Now, we can simply call upon the methods of the parent class to do the rest of the work for us! We still need to validate our configuration and
  our Expectation, but this can be done very quickly:
  
.. code-block:: python

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)


    def _validate(
      self,
      configuration: ExpectationConfiguration,
      metrics: Dict,
      runtime_configuration: dict = None,
      execution_engine: ExecutionEngine = None,
    ):
      return super().self._validate_metric_value_between(
          metric_name="column.mean",
          configuration=configuration,
          metrics=metrics,
          runtime_configuration=runtime_configuration,
          execution_engine=execution_engine,
      )
