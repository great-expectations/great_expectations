.. _validation_operators_and_actions_intro:

####################################
Validation Operators And Actions
####################################

When you want to validate several Expectation Suites together and take an action based on the result of this
validation, Validation Operators provide a convenient abstraction for both bundling the validation of multiple
expectation suites and the actions that should be taken.

If you are using a Validation Operator that came with GE or was contributed by another developer,
you can get to a rich set of useful behaviors with very little coding. This is done by editing the operator's
configuration in the GE configuration file and by extending the operator in case you want to add new behavior.

Another (future) benefit of Validation Operators is that the grouping of multiple expectation suites will allow
compute optimization.

***********************************************
DefaultDataContextAwareValidationOperator
***********************************************

GE comes with a default operator - DefaultDataContextAwareValidationOperator.

It operates on one batch and evaluates three expectation suites - failures, warnings and quarantine.

This implements business logic that many data practitioners consider useful.

The failure Expectation Suite contains expectations that are considered important enough to justify stopping the
pipeline when they are violated.

The expectations in the warnings Expectation Suite are less critical. If a batch does not meet these expectations,
it is considered a warning - worth looking into, but the pipeline should be allowed to proceed.

The quarantine Expectation Suite provides a service of filtering out the rows that do not meet the expectations in
this suite and returning a clean dataset while saving the non-conforming rows into a quarantine queue for a later
review. The pipeline can use the returned dataset (without the non-conforming rows) for next steps.

The quarantine functionality is currently implemented only for Pandas.


The operator returns an object that contains sufficient information for the pipeline to decide if it should proceed:

.. code-block:: json

    {
        "success" : None,
        "validation_results" : {
            "failure" : (validation result id, validation result),
            "warning" : (validation result id, validation result),
            "quarantine" : (validation result id, validation result)
        },
        "data_assets" : {
            "original_batch" : ...,
            "nonquarantined_batch" : ...,
            "quarantined_batch" : ...,
        }
    }

In addition to returning this object, the operator has side effects. It will store the validation results and will
send a notification (e.g., Slack).

The stores for validation results and the data snapshots, and the webhook where the notifications should be sent are
specified in the operator's configuration.

***********************************************
Invoking an operator
***********************************************

First, define an operator in the config file, give it a name and edit its configuration.

The following code snippet shows how to invoke an operator, once it is configured:

.. code-block:: python

    results = data_context.run_validation_operator(
        data_asset= a batch from a data asset,
        data_asset_identifier=data_asset_identifier (if you want the operator to fetch the batch)
        run_identifier= uniquely identifies the run,
        validation_operator_name=name of the operator as specified in the config file,
    )


Current operator accepts one data asset, but it is possible to write one that accepts multiple.

There are no restrictions on what an operator should return.

***********************************************
Anatomy of an operator
***********************************************

Operators must define a run method that accepts one or more data assets (or data asset identifiers) and run id.

What happens inside an operator is completely up to the implementor.

In practice a typical operator will invoke actions.

An action is a way to take an arbitrary method and make it configurable and runnable within a data context.

The only requirement from an action is for it to have a take_action method.

GE comes with a list of actions that we consider useful and you can reuse in your pipelines. Most of them take in
validation results and do something with it.

***************************************************
Can an operator be used outside of a data context?
***************************************************
In theory yes, but most useful operators would be data context aware.




