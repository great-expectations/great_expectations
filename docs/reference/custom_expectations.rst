.. _custom_expectations:

####################################
Custom expectations
####################################

It's common to want to extend Great Expectations with application- or domain-specific Expectations. For example:

.. code-block:: bash

    expect_column_text_to_be_in_english
    expect_column_value_to_be_valid_medical_diagnosis_code
    expect_column_value_to_be_be_unicode_encodable

These Expectations aren't included in the default set, but could be very useful for specific applications.

Fear not! Great Expectations is designed for customization and extensibility.

Side note: in future versions, Great Expectations will probably grow to include additional Expectations. If you have an Expectation that could be universally useful, please make the case on the `Great Expectations issue tracker on github <https://github.com/great-expectations/great_expectations/issues>`_.

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

Note: following Great Expectations patterns for :ref:`extending_great_expectations` is highly recommended, but not
strictly required. If you want to confuse yourself with bad names, the package won't stop you.

For example, in Pandas:

`@MetaPandasDataset.column_map_expectation` decorates a custom function, wrapping it with all the business logic
required to turn it into a fully-fledged Expectation. This spares you the hassle of defining logic to handle required
arguments like `mostly` and `result_format`. Your custom function can focus exclusively on the business logic of
passing or failing the expectation.

To work with these decorators, your custom function must accept two arguments: `self` and `column`. When your function
is called, `column` will contain all the non-null values in the given column. Your function must return a series of
boolean values in the same order, with the same index.

`@MetaPandasDataset.column_aggregate_expectation` accepts `self` and `column`. It must return a dictionary containing
a boolean `success` value, and a nested dictionary called `result` which contains an `observed_value` argument.

Setting the _data_asset_type is not strictly necessary, but doing so allows GE to recognize that you have added
expectations rather than simply added support for the same expectation suite on a different
backend/compute environment.

.. code-block:: python

    from great_expectations.dataset import PandasDataset, MetaPandasDataset

    class CustomPandasDataset(PandasDataset):

        _data_asset_type = "CustomPandasDataset"

        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_equal_2(self, column):
            return column.map(lambda x: x==2)

        @MetaPandasDataset.column_aggregate_expectation
        def expect_column_mode_to_equal_0(self, column):
            mode = column.mode[0]
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
                        'success':True,
                        'unexpected_list':unexpected_values,
                        'unexpected_index_list':self.index[result],
                    }

                percent_equaling_1 = float(sum(result))/len(not_null)
                return {
                    "success" : percent_equaling_1 >= mostly,
                    "unexpected_list" : unexpected_values[:20],
                    "unexpected_index_list" : list(self.index[result==False])[:20],
                }
            else:
                return {
                    "success" : len(unexpected_values) == 0,
                    "unexpected_list" : unexpected_values[:20],
                    "unexpected_index_list" : list(self.index[result==False])[:20],
                }


A similar implementation for SqlAlchemy would also import the base decorator:

.. code-block:: python

    import sqlalchemy as sa
    from great_expectations.dataset import SqlAlchemyDataset

    import scipy.stats as stats

    class CustomSqlAlchemyDataset(SqlAlchemyDataset):

        _data_asset_type = "CustomSqlAlchemyDataset"

        @DataAsset.expectation(["column_A", "column_B", "p_value"])
        def expect_column_pair_histogram_ks_2samp_test_p_value_to_be_greater_than(
                column_A,
                column_B,
                p_value,
                already_sorted=False
        ):
        """Our very nice docstring."""
            # We will assume that these are already HISTOGRAMS created as a check_dataset
            rows = sa.select([
                sa.column(column_A).label("col_A_weights"),
                sa.column(column_B).label("col_B_weights")
            ]).select_from(self._table).fetchall()

            cols = [col for col in zip(*rows)]

            if not already_sorted:
                v0 = sorted(cols[0])
                v1 = sorted(cols[1])
            else:
                v0 = cols[0]
                v1 = cols[1]

            stat_, pval = stats.fancy_test(v0, v1)

            return {
                "success": pval < p_value,
                "result": {
                    "observed_value": pval,
                    "details": {
                        "fancy_stat": stat_
                    }
                }
            }

Rapid Prototyping
========================================

For rapid prototyping, the following syntax allows quick iteration on the logic for expectations.

.. code-block:: bash

    >> DataAsset.test_expectation_function(my_func)
    
    >> Dataset.test_column_map_expectation_function(my_map_func, column='my_column')
    
    >> Dataset.test_column_aggregate_expectation_function(my_agg_func, column='my_column')

These functions will return output just like regular expectations. However, they will NOT save a copy of the
expectation to the current expectation suite.

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

    >> great_expectations validate \
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
CustomPandasDataset in a DataContext.

.. code-block:: yaml

    datasources:
      my_datasource:
        class_name: PandasDatasource
        data_asset_type:
          module_name: custom_dataset
          class_name: CustomPandasDataset
        generators:
          default:
            class_name: SubdirReaderGenerator
            base_directory: /data
            reader_options:
              sep: \t

.. code-block:: bash

    >> import great_expectations as ge
    >> context = ge.DataContext()
    >> my_df = context.get_batch("my_datasource/default/my_file")

    >> my_df.expect_column_values_to_equal_1("all_twos")
    {
        "success": False,
        "unexpected_list": [2,2,2,2,2,2,2,2]
    }
