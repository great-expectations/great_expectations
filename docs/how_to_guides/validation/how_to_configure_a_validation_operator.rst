How to configure a ValidationOperator
=====================================

If you are using a Validation Operator that came with GE or was contributed by another developer,
you can get to a rich set of useful behaviors with very little coding. This is done by editing the operator's configuration in the GE configuration file and by extending the operator in case you want to add new behavior.

To use a Validation Operator (one that is included in Great Expectations or one that you implemented in your project's plugins directory), you need to configure an instance of the operator in your `great_expectations.yml` file and then invoke this instance from your Python code.


Configuring an operator
~~~~~~~~~~~~~~~~~~~~~~~

All Validation Operators configuration blocks should appear under `validation_operators` key in `great_expectations.yml`

To configure an instance of an operator in your Great Expectations context, create a key under `validation_operators`. This is the name of you will use to invoke this operator.

.. code-block:: yaml

    my_operator:
        class_name: TheClassThatImplementsMyOperator
        module_name: thefilethatyoutheclassisin
        foo: bar

In the example of an operator config block above:

* the `class_name` value is the name of the class that implements this operator.
* the key `module_name` must be specified if the class is not in the default module.
* the `foo` key specifies the value of the `foo` argument of this class' constructor. Since every operator class might define its own constructor, the keys will vary.

Invoking an operator
~~~~~~~~~~~~~~~~~~~~

This is an example of invoking an instance of a Validation Operator from Python:

.. code-block:: python

    results = context.run_validation_operator(
        assets_to_validate=[batch0, batch1, ...],
        run_id="some_string_that_uniquely_identifies_this_run",
        validation_operator_name="perform_action_list_operator",
    )

* `assets_to_validate` - an iterable that specifies the data assets that the operator will validate. The members of the list can be either batches or triples that will allow the operator to fetch the batch: (data_asset_name, expectation_suite_name, batch_kwargs) using this method: :py:meth:`~great_expectations.data_context.BaseDataContext.get_batch`
* run_id - pipeline run id, a timestamp or any other string that is meaningful to you and will help you refer to the result of this operation later
* validation_operator_name you can instances of a class that implements a Validation Operator

Each operator class is free to define its own object that the `run` method returns. Consult the reference of the specific Validation Operator.

