How to implement a custom Validation Operator
=============================================

If you wish to implement some validation handling logic that is not supported by the operators included in Great Expectations, follow these steps:

* Extend the :py:class:`great_expectations.validation_operators.ValidationOperator` base class
* Implement the `run` method in your new class
* Put your class in the plugins directory of your project (see `plugins_directory` property in the `great_expectations.yml configuration file`)

Once these steps are complete, your new Validation Operator can be configured and used.

If you think that the business logic of your operator can be useful to other data practitioners, please consider contributing it to Great Expectations.
