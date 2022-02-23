.. _how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations:

#################################
How to create custom Expectations
#################################

This document provides examples that walk through several methods for building and deploying custom expectations.

Beginning in version 0.13, we have introduced a new API focused on enabling Modular Expectations that are defined in individual classes.

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        Most of the core Great Expectations expectations are built using expectation decorators, and using decorators on existing logic can make bringing custom integrations into your pipeline tests easy.

        .. warning:: This doc has been partially migrated from old documentation. It's potentially helpful, but may be incomplete, incorrect, or confusing.

        ****************************************
        Using Expectation Decorators
        ****************************************

        Under the hood, great_expectations evaluates similar kinds of expectations using standard logic, including:

        * `column_map_expectations`, which apply their condition to each value in a column independently of other values
        * `column_aggregate_expectations`, which apply their condition to an aggregate value or values from the column

        In general, if a column is empty, a column_map_expectation will return True (vacuously), whereas a
        column_aggregate_expectation will return False (since no aggregate value could be computed).

        Adding an expectation about element counts to a set of expectations is usually therefore very important to ensure
        the overall set of expectations captures the full set of constraints you expect.


        High-level decorators
        ========================================
        High-level decorators may modify the type of arguments passed to their decorated methods and do significant
        computation or bookkeeping to augment the work done in an expectation. That makes implementing many common types of
        operations using high-level decorators very easy.

        To use the high-level decorators (e.g. ``column_map_expectation`` or ```column_aggregate_expectation``):

        1. Create a subclass from the dataset class of your choice
        2. Define custom functions containing your business logic
        3. Use the `column_map_expectation` and `column_aggregate_expectation` decorators to turn them into full Expectations. Note that each dataset class implements its own versions of `@column_map_expectation` and `@column_aggregate_expectation`, so you should consult the documentation of each class to ensure you are returning the correct information to the decorator.

        Note: following Great Expectations naming conventions for Expectations is highly recommended, but not
        strictly required. If you want to confuse yourself with bad names, the package won't stop you.

        For example, in Pandas:







        .. code-block:: python

            from great_expectations.dataset import PandasDataset, MetaPandasDataset

            class CustomPandasDataset(PandasDataset):

                _data_asset_type = "CustomPandasDataset"

                @MetaPandasDataset.column_map_expectation
                def expect_column_values_to_equal_2(self, column):
                    return column.map(lambda x: x==2)

                @MetaPandasDataset.column_aggregate_expectation
                def expect_column_mode_to_equal_0(self, column):
                    mode = self[column].mode[0]
                    return {
                        "success" : mode == 0,
                        "result": {
                            "observed_value": mode,
                        }
                    }

        For SqlAlchemyDataset and SparkDFDataset, the decorators work slightly differently. See the respective Meta-class
        docstrings for more information; the example below shows SqlAlchemy implementations of the same custom expectations.

        .. code-block:: python

            import sqlalchemy as sa
            from great_expectations.dataset import SqlAlchemyDataset, MetaSqlAlchemyDataset

            class CustomSqlAlchemyDataset(SqlAlchemyDataset):

                _data_asset_type = "CustomSqlAlchemyDataset"

                @MetaSqlAlchemyDataset.column_map_expectation
                def expect_column_values_to_equal_2(self, column):
                    return (sa.column(column) == 2)

                @MetaSqlAlchemyDataset.column_aggregate_expectation
                def expect_column_mode_to_equal_0(self, column):
                    mode_query = sa.select([
                        sa.column(column).label('value'),
                        sa.func.count(sa.column(column)).label('frequency')
                    ]).select_from(self._table).group_by(sa.column(column)).order_by(sa.desc(sa.column('frequency')))

                    mode = self.engine.execute(mode_query).scalar()
                    return {
                        "success": mode == 0,
                        "result": {
                            "observed_value": mode,
                        }
                    }



        Using the base Expectation decorator
        ========================================
        When the high-level decorators do not provide sufficient granularity for controlling your expectation's behavior, you
        need to use the base expectation decorator, which will handle storing and retrieving your expectation in an
        expectation suite, and facilitate validation using your expectation. You will need to explicitly declare the parameters.

        1. Create a subclass from the dataset class of your choice
        2. Write the whole expectation yourself
        3. Decorate it with the `@expectation` decorator, declaring the parameters you will use.

        This is more complicated, since you have to handle all the logic of additional parameters and output formats.
        Pay special attention to proper formatting of :ref:`result_format`.

        .. code-block:: python

            from great_expectations.data_asset import DataAsset
            from great_expectations.dataset import PandasDataset

            class CustomPandasDataset(PandasDataset):

                _data_asset_type = "CustomPandasDataset"

                @DataAsset.expectation(["column", "mostly"])
                def expect_column_values_to_equal_1(self, column, mostly=None):
                    not_null = self[column].notnull()

                    result = self[column][not_null] == 1
                    unexpected_values = list(self[column][not_null][result==False])

                    if mostly:
                        #Prevent division-by-zero errors
                        if len(not_null) == 0:
                            return {
                                "success":True,
                                "result": {
                                    'unexpected_list':unexpected_values,
                                    'unexpected_index_list':self.index[result],
                                }
                            }

                        percent_equaling_1 = float(sum(result))/len(not_null)
                        return {
                            "success" : percent_equaling_1 >= mostly,
                            "result": {
                                "unexpected_list" : unexpected_values[:20],
                                "unexpected_index_list" : list(self.index[result==False])[:20],
                                }
                        }
                    else:
                        return {
                            "success" : len(unexpected_values) == 0,
                            "result": {
                                "unexpected_list" : unexpected_values[:20],
                                "unexpected_index_list" : list(self.index[result==False])[:20],
                            }
                        }


        A similar implementation for SqlAlchemy would also import the base decorator:

        .. code-block:: python

            import sqlalchemy as sa
            from great_expectations.data_asset import DataAsset
            from great_expectations.dataset import SqlAlchemyDataset

            import numpy as np
            import scipy.stats as stats
            import scipy.special as special

            if sys.version_info.major >= 3 and sys.version_info.minor >= 5:
                from math import gcd
            else:
                from fractions import gcd

            class CustomSqlAlchemyDataset(SqlAlchemyDataset):

                _data_asset_type = "CustomSqlAlchemyDataset"

                @DataAsset.expectation(["column_A", "column_B", "p_value", "mode"])
                def expect_column_pair_histogram_ks_2samp_test_p_value_to_be_greater_than(
                        self,
                        column_A,
                        column_B,
                        p_value=0.05,
                        mode='auto'
                ):
                    """Execute the two sample KS test on two columns of data that are expected to be **histograms** with
                    aligned values/points on the CDF. ."""
                    LARGE_N = 10000  # 'auto' will attempt to be exact if n1,n2 <= LARGE_N

                    # We will assume that these are already HISTOGRAMS created as a check_dataset
                    # either of binned values or of (ordered) value counts
                    rows = sa.select([
                        sa.column(column_A).label("col_A_counts"),
                        sa.column(column_B).label("col_B_counts")
                    ]).select_from(self._table).fetchall()

                    cols = [col for col in zip(*rows)]
                    cdf1 = np.array(cols[0])
                    cdf2 = np.array(cols[1])
                    n1 = cdf1.sum()
                    n2 = cdf2.sum()
                    cdf1 = cdf1 / n1
                    cdf2 = cdf2 / n2

                    # This code is taken verbatim from scipy implementation,
                    # skipping the searchsorted (using sqlalchemy check asset as a view)
                    # https://github.com/scipy/scipy/blob/v1.3.1/scipy/stats/stats.py#L5385-L5573
                    cddiffs = cdf1 - cdf2
                    minS = -np.min(cddiffs)
                    maxS = np.max(cddiffs)
                    alt2Dvalue = {'less': minS, 'greater': maxS, 'two-sided': max(minS, maxS)}
                    d = alt2Dvalue[alternative]
                    g = gcd(n1, n2)
                    n1g = n1 // g
                    n2g = n2 // g
                    prob = -np.inf
                    original_mode = mode
                    if mode == 'auto':
                        if max(n1, n2) <= LARGE_N:
                            mode = 'exact'
                        else:
                            mode = 'asymp'
                    elif mode == 'exact':
                        # If lcm(n1, n2) is too big, switch from exact to asymp
                        if n1g >= np.iinfo(np.int).max / n2g:
                            mode = 'asymp'
                            warnings.warn(
                                "Exact ks_2samp calculation not possible with samples sizes "
                                "%d and %d. Switching to 'asymp' " % (n1, n2), RuntimeWarning)

                    saw_fp_error = False
                    if mode == 'exact':
                        lcm = (n1 // g) * n2
                        h = int(np.round(d * lcm))
                        d = h * 1.0 / lcm
                        if h == 0:
                            prob = 1.0
                        else:
                            try:
                                if alternative == 'two-sided':
                                    if n1 == n2:
                                        prob = stats._compute_prob_outside_square(n1, h)
                                    else:
                                        prob = 1 - stats._compute_prob_inside_method(n1, n2, g, h)
                                else:
                                    if n1 == n2:
                                        # prob = binom(2n, n-h) / binom(2n, n)
                                        # Evaluating in that form incurs roundoff errors
                                        # from special.binom. Instead calculate directly
                                        prob = 1.0
                                        for j in range(h):
                                            prob = (n1 - j) * prob / (n1 + j + 1)
                                    else:
                                        num_paths = stats._count_paths_outside_method(n1, n2, g, h)
                                        bin = special.binom(n1 + n2, n1)
                                        if not np.isfinite(bin) or not np.isfinite(num_paths) or num_paths > bin:
                                            raise FloatingPointError()
                                        prob = num_paths / bin

                            except FloatingPointError:
                                # Switch mode
                                mode = 'asymp'
                                saw_fp_error = True
                                # Can't raise warning here, inside the try
                            finally:
                                if saw_fp_error:
                                    if original_mode == 'exact':
                                        warnings.warn(
                                            "ks_2samp: Exact calculation overflowed. "
                                            "Switching to mode=%s" % mode, RuntimeWarning)
                                else:
                                    if prob > 1 or prob < 0:
                                        mode = 'asymp'
                                        if original_mode == 'exact':
                                            warnings.warn(
                                                "ks_2samp: Exact calculation incurred large"
                                                " rounding error. Switching to mode=%s" % mode,
                                                RuntimeWarning)

                    if mode == 'asymp':
                        # The product n1*n2 is large.  Use Smirnov's asymptoptic formula.
                        if alternative == 'two-sided':
                            en = np.sqrt(n1 * n2 / (n1 + n2))
                            # Switch to using kstwo.sf() when it becomes available.
                            # prob = distributions.kstwo.sf(d, int(np.round(en)))
                            prob = distributions.kstwobign.sf(en * d)
                        else:
                            m, n = max(n1, n2), min(n1, n2)
                            z = np.sqrt(m*n/(m+n)) * d
                            # Use Hodges' suggested approximation Eqn 5.3
                            expt = -2 * z**2 - 2 * z * (m + 2*n)/np.sqrt(m*n*(m+n))/3.0
                            prob = np.exp(expt)

                    prob = (0 if prob < 0 else (1 if prob > 1 else prob))

                    return {
                        "success": prob > p_value,
                        "result": {
                            "observed_value": prob,
                            "details": {
                                "ks_2samp_statistic": d
                            }
                        }
                    }






        **************************************************
        Using custom expectations
        **************************************************

        Let's suppose you've defined `CustomPandasDataset` in a module called `custom_dataset.py`. You can instantiate a
        dataset with your custom expectations simply by adding `dataset_class=CustomPandasDataset` in `ge.read_csv`.

        Once you do this, all the functionality of your new expectations will be available for uses.

        .. code-block:: bash

            >> import great_expectations as ge
            >> from custom_dataset import CustomPandasDataset

            >> my_df = ge.read_csv("my_data_file.csv", dataset_class=CustomPandasDataset)

            >> my_df.expect_column_values_to_equal_1("all_twos")
            {
                "success": False,
                "unexpected_list": [2,2,2,2,2,2,2,2]
            }

        A similar approach works for the command-line tool.

        .. code-block:: bash

            >> great_expectations validation csv \
                my_data_file.csv \
                my_expectations.json \
                dataset_class=custom_dataset.CustomPandasDataset

        .. _custom_expectations_in_datasource:

        Using custom expectations with a Datasource
        ==================================================

        To use custom expectations in a datasource or DataContext, you need to define the custom DataAsset in the datasource
        configuration or batch_kwargs for a specific batch. Following the same example above, let's suppose you've defined
        `CustomPandasDataset` in a module called `custom_dataset.py`. You can configure your datasource to return instances
        of your custom DataAsset type by declaring that as the data_asset_type for the datasource to build.

        If you are working a DataContext, simply placing `custom_dataset.py` in your configured plugin directory will make it
        accessible, otherwise, you need to ensure the module is on the import path.

        Once you do this, all the functionality of your new expectations will be available for use. For example, you could use
        the datasource snippet below to configure a PandasDatasource that will produce instances of your new
        CustomPandasDataset in a DataContext. Note the use of standard python dot notation to import.

        .. code-block:: yaml

            datasources:
              my_datasource:
                class_name: PandasDatasource
                data_asset_type:
                  module_name: custom_module.custom_dataset
                  class_name: CustomPandasDataset
                generators:
                  default:
                    class_name: SubdirReaderBatchKwargsGenerator
                    base_directory: /data
                    reader_options:
                      sep: \t

        Note that we need to have added our **custom_dataset.py** to a directory called **custom_module** as in the directory
        structure below.

        .. code-block:: bash

            great_expectations
            ├── .gitignore
            ├── datasources
            ├── expectations
            ├── great_expectations.yml
            ├── notebooks
            │   ├── pandas
            │   ├── spark
            │   └── sql
            ├── plugins
            │   └── custom_module
            │       └── custom_dataset.py
            └── uncommitted
                ├── config_variables.yml
                ├── data_docs
                │   └── local_site
                ├── samples
                └── validations



        .. code-block:: bash

            >> import great_expectations as ge
            >> context = ge.DataContext()
            >> my_df = context.get_batch(
                "my_datasource/default/my_file",
                "warning",
                context.yield_batch_kwargs("my_datasource/default/my_file"))

            >> my_df.expect_column_values_to_equal_1("all_twos")
            {
                "success": False,
                "unexpected_list": [2,2,2,2,2,2,2,2]
            }

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        How to define a custom Expectation type
        _________________________________________________________________

        This guide will walk you through the process of creating your own Modular Expectations in 6 simple steps!

        See also this `complete example <https://github.com/superconductive/ge_tutorials/blob/main/getting_started_tutorial_final_v3_api/great_expectations/plugins/column_custom_max_expectation.py>`_.

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

          - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

        Modular Expectations are new in version 0.13. They utilize a class structure that is significantly easier to build than
        ever before and are explained below!


        Steps:
        -------

        #. **Plan Metric Dependencies**

           In the new Modular Expectation design, Expectations rely on Metrics defined by separate MetricProvider Classes, which are then referenced within the Expectation and used for computation. For more on Metric Naming Conventions, see our guide on :ref:`metric naming conventions <reference__core_concepts__metrics__naming_conventions>`.

           Once you’ve decided on an Expectation to implement, think of the different aggregations, mappings, or metadata you’ll need to validate your data within the Expectation - each of these will be a separate metric that must be implemented prior to validating your Expectation.

           Fortunately, many Metrics have already been implemented for pre-existing Expectations, so it is possible you will find that the Metric you’d like to implement already exists within the GE framework and can be readily deployed.


        #. **Implement your Metric**

           If your metric does not yet exist within the framework, you will need to implement it yourself within a new file - a task that is quick and simple within the new modular framework.

           Below lies the full implementation of an aggregate metric class, with implementations for Pandas, SQLAlchemy, and Apache Spark dialects. (Other implementations can be found in the dictionary of metrics).


        .. code-block:: python

            from great_expectations.execution_engine import (
               PandasExecutionEngine,
               SparkDFExecutionEngine,
               SqlAlchemyExecutionEngine,
            )
            from great_expectations.expectations.metrics import (
               ColumnAggregateMetricProvider,
               column_aggregate_value, column_aggregate_partial,
            )
            from great_expectations.expectations.metrics.import_manager import F, sa

            class ColumnCustomMax(ColumnAggregateMetricProvider):
                """MetricProvider Class for Custom Aggregate Max MetricProvider"""

                metric_name = "column.aggregate.custom.max"

                @column_aggregate_value(engine=PandasExecutionEngine)
                def _pandas(cls, column, **kwargs):
                    """Pandas Max Implementation"""
                    return column.max()

                @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
                def _sqlalchemy(cls, column, **kwargs):
                    """SqlAlchemy Max Implementation"""
                    return sa.func.max(column)

                @column_aggregate_partial(engine=SparkDFExecutionEngine)
                def _spark(cls, column, _table, _column_name, **kwargs):
                    """Spark Max Implementation"""
                    types = dict(_table.dtypes)
                    return F.maxcolumn()



        3. **Define Parameters**

           We have already reached the point where we can start building our Expectation!

           The structure of a Modular Expectation now exists within its own specialized class - indicating it will usually exist in a separate file from the Metric. This structure has 3 fundamental components: Metric Dependencies, Configuration Validation, and Expectation Validation. In this step, we will address setting up our parameters.

            In this guide, we focus on a ``ColumnExpectation`` which can define metric dependencies simply using the metric_dependencies property.

            Add the following attributes to your Expectation class:

           - **Metric Dependencies** - A tuple consisting of the names of all metrics necessary to evaluate the Expectation. Using this shortcut tuple will provide the dependent metric with the same domain kwargs and value kwargs as the Expectation.
           - **Success Keys** - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success.
           - **Default Kwarg Values (Optional)**  -  Default values for success keys and the defined domain, among other values.

           An example of Expectation Parameters is shown below (notice that we are now in a new Expectation class and building our Expectation in a separate file from our Metric):


        .. code-block:: python

           class ExpectColumnMaxToBeBetweenCustom(ColumnExpectation):
              # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
              metric_dependencies = ("column.aggregate.custom.max",)
              success_keys = ("min_value", "strict_min", "max_value", "strict_max")

              # Default values
              default_kwarg_values = {
                  "row_condition": None,
                  "condition_parser": None,
                  "min_value": None,
                  "max_value": None,
                  "strict_min": None,
                  "strict_max": None,
                  "mostly": 1
              }


        4. **Validate Configuration**

           We have almost reached the end of our journey in implementing an Expectation! Now, if we have requested certain parameters from the user, we would like to validate that the user has entered them correctly via a validate_configuration method.

           In this method, the user provides a configuration and we check that certain conditions are satisfied by the configuration. For example, if the user has given us a minimum and maximum threshold, it is important to verify that our minimum threshold does not exceed our maximum threshold:


        .. code-block:: python

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


        5. **Validate**

           In this step, we simply need to validate that the results of our metrics meet our Expectation.

           The validate method is implemented as _validate. This method takes a dictionary named Metrics, which contains all metrics requested by your metric dependencies, and performs a simple validation against your success keys (i.e. important thresholds) in order to return a dictionary indicating whether the Expectation has evaluated successfully or not:

        .. code-block:: python

           def _validate(
              self,
              configuration: ExpectationConfiguration,
              metrics: Dict,
              runtime_configuration: dict = None,
              execution_engine: ExecutionEngine = None,
           ):
              """Validates the given data against the set minimum and maximum value thresholds for the column max"""
              column_max = metrics["column.aggregate.custom.max"]

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

        6. **Test**

           When developing an Expectation, there are several different points at which you should test what you have written:

           1. During development, you should import and run your Expectation, writing additional tests for get_evaluation parameters if it is complicated
           2. It is often helpful to generate examples showing the functionality of your Expectation, which helps verify the Expectation works as intended.
           3. If you plan on contributing your Expectation back to the library of main Expectations, you should build a JSON test for it in the ``tests/test_definitions/name_of_your_expectation`` directory.

        7. **Import**: To use a custom Expectation, you need to ensure it has been imported into the running python interpreter. While including the module in your `plugins/` directory will make it *available* to import, you must still import the Expectation:

        .. code-block:: python

           # get a validator
           # Note: attempting to run our expectation now would fail, because even though
           # our Expectation is in our DataContext plugins/ directory it has not been imported.

           from custom_module import ExpectColumnMaxToBeBetweenCustom

           # now we can run our expectation
           validator.expect_column_max_to_be_between_custom('col', min_value=0, max_value=5)

        8. **Optional:** Implement :ref:`Custom Data Docs Renderers <how_to_guides__configuring_data_docs__how_to_create_renderers_for_custom_expectations>`

        We have now implemented our own Custom Expectations! For more information about Expectations and Metrics, please reference the core concepts documentation.

        Additional Notes:
        -----------------

        1. Arguments for Custom Expectations currently **must be provided as keyword arguments**; positional arguments should be avoided.
