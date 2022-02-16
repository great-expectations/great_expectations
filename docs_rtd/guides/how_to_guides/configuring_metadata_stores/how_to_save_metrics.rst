.. _saving_metrics:

.. role:: raw-html(raw)
   :format: html

.. raw:: html

  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.11.2/css/all.min.css">
  <style>
      .text-danger {
          color: #dc3545;
      }
      .text-warning {
          color: #ffc107;
      }
      .text-success {
          color: #28a745;
      }
      .legend-table td{
          border: 1px solid #ddd;
          padding: 5px;
      }
  </style>

.. sidebar:: This guide is :raw-html:`<br/><i class="fas fa-circle text-warning"></i> <a href="https://docs.greatexpectations.io/en/latest/contributing/levels_of_maturity.html?highlight=early%20developers#levels-of-maturity">Beta: Ready for early adopters</a>`

  Find more discussion `here <https://discuss.greatexpectations.io/t/ge-with-databricks-delta/82/3>`_

  Thanks to contributors

    * `@joe_gargery <google.com>`_
    * `@jaggers <google.com>`_
    * `@esthav2002 <google.com>`_


###############################
How to configure a MetricsStore
###############################

Saving metrics during Validation makes it easy to construct a new data series based on observed
dataset characteristics computed by Great Expectations. That data series can serve as the source for a dashboard or
overall data quality metrics, for example.

Storing metrics is still a **beta** feature of Great Expectations, and we expect configuration and
capability to evolve rapidly.

*********************
Adding a MetricsStore
*********************

A MetricStore is a special store that can store Metrics computed during Validation. A Metric store tracks the run_id
of the validation and the expectation suite name in addition to the metric name and metric kwargs.

In most cases, a MetricStore will be configured as a SQL database. To add a MetricStore to your DataContext, add the
following yaml block to the "stores" section:

.. code-block:: yaml

    stores:
        #  ...
        metrics_store:  # You can choose any name for your metric store
            class_name: MetricStore
            store_backend:
                class_name: DatabaseStoreBackend
                # These credentials can be the same as those used in a Datasource configuration
                credentials: ${my_store_credentials}


The next time your DataContext is loaded, it will connect to the database and initialize a table to store metrics if
one has not already been created. See the :ref:`metrics_reference` for more information on additional configuration
options.

*******************************
Configuring a Validation Action
*******************************

Once a MetricStore is available, it is possible to configure a new `StoreMetricsAction` to save metrics during
validation.

Add the following yaml block to your DataContext validation operators configuration:

.. code-block:: yaml

    validation_operators:
      # ...
      action_list_operator:
        class_name: ActionListValidationOperator
        action_list:
          # ...
          - name: store_metrics
            action:
              class_name: StoreMetricsAction
              target_store_name: metrics_store  # Keep the space before this hash so it's not read as the name. This should match the name of the store configured above
              # Note that the syntax for selecting requested metrics will change in a future release
              requested_metrics:
                "*":  # The asterisk here matches *any* expectation suite name
                  # use the 'kwargs' key to request metrics that are defined by kwargs,
                  # for example because they are defined only for a particular column
                  # - column:
                  #     Age:
                  #       - expect_column_min_to_be_between.result.observed_value
                  - statistics.evaluated_expectations
                  - statistics.successful_expectations


The `StoreMetricsValidationAction` processes an `ExpectationValidationResult` and stores Metrics to a configured Store.
Now, when your operator is executed, the requested metrics will be available in your database!

.. code-block:: python

    context.run_validation_operator('action_list_operator', (batch_kwargs, expectation_suite_name))


.. note::
  To discuss with the Great Expectations community, please visit this topic in our community discussion forum: `https://discuss.greatexpectations.io/t/ge-with-databricks-delta/82/3 <https://discuss.greatexpectations.io/t/ge-with-databricks-delta/82/3>`_
