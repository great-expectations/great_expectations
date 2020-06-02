.. _how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations_for_sqlalchemy:

How to create custom Expectations for SQLAlchemy
================================================

Custom Expectations let you extend the logic for validating data to use any criteria you choose. This guide will show you how to extend the ``PandasDataset`` class with your own :ref:`Expectations`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - Installed Great Expectations (e.g. ``pip install great_expectations``)
    - Have access to a notebook (e.g. ``jupyter notebook``, ``jupyter lab``, etc.)
    - Be able to access data from your notebook
    - Nothing else. Unlike most how-to guides, these instructions do *not* assume that you have configured a Data Context by running ``great_expectations init``.

Steps
-----

1. **Import sqlalchemy, SqlAlchemyDataset and MetaSqlAlchemyDataset**

    .. code-block:: python

        import sqlalchemy as sa
        from great_expectations.dataset import (
            SqlAlchemyDataset,
            MetaSqlAlchemyDataset,
        )

    ``SqlAlchemyDataset`` is the parent class used for executing Expectations using sqlalchemy. Most of the core Expectations are built using decorators defined in ``MetaSqlAlchemyDataset``. These decorators greatly streamline the task of extending Great Expectations with custom Expectation logic.

2. **Define a class inheriting from SqlAlchemyDataset**

    .. code-block:: python

        class CustomSqlAlchemyDataset(SqlAlchemyDataset):

            _data_asset_type = "CustomSqlAlchemyDataset"

    Setting the ``_data_asset_type`` is not strictly necessary, but can be helpful for tracking the lineage of instantiated Expectations and :ref:`Validation Results`.

3. **Within your new class, define Expectations using decorators from MetaSqlAlchemyDataset**

    ``column_map_expectations`` are Expectations that are applied to a single column, on a row-by-row basis. To learn about other Expectation types, please see :ref:`Other Expectation decorators` <<<somewhere_else>>>.

    The ``@MetaSqlAlchemyDataset.column_map_expectation`` decorator wraps your custom function with all the business logic required to turn it into a fully-fledged Expectation. This spares you the hassle of defining logic to handle required arguments like ``mostly`` and ``result_format``. Your custom function can focus exclusively on the business logic of passing or failing the Expectation.

    In the simplest case, they could be as simple as one-line lambda functions.

    .. code-block:: python

        @MetaSqlAlchemyDataset.column_map_expectation
        def expect_column_values_to_be_even(self, column):
            return (sa.column(column) % 2 == 0)

    To use the ``column_map_expectation`` decorator, your custom function must accept at least two arguments: ``self`` and ``column``. When the user invokes your Expectation, they will pass a string containing the column name. <<#FIXME: The decorator will then fetch the appropriate column and pass all of the non-null values to your function as a pandas ``Series``. Your function must then return a Series of boolean values in the same order, with the same index.>>

    By convention, ``column_map_expectations`` always start with ``expect_column_values_...`` or ``expect_column_value_...`` (Ex: ``expect_column_value_word_counts_to_be_between``). Following this pattern is highly recommended, but not strictly required. If you want to confuse yourself with bad names, the package won't stop you.

    Please see <<<XXX>>> for additional details on implementing ``column_map_expectations``.

4. **Load some data**

    To make your new Expectations available for validation, you can instantiate a ``CustomSqlAlchemyDataset`` as follows:

    .. code-block:: python

        my_data_asset = CustomSqlAlchemyDataset(
            "employees",
            sa.create_engine("sqlite:///data/chinook.db")
        )

    If you have a Data Context configured, you can use ``DataContext.get_batch()`` to fetch a batch using a pre-configured Datasource. See :ref:`Configuring Datasources` and :ref:`Creating Batches` for instructions.

    .. code-block:: python

        import great_expectations as ge
        context = ge.DataContext()

        # You'll need to define this to create your batch:
        my_batch_kwargs = ...

        my_data_asset = context.get_batch(
            my_batch_kwargs,
            ExpectationSuite("my_temporary_test_suite"),
            CustomSqlAlchemyDataset,
        )

5. **Test your Expectations**

    At this point, you can test your new Expectations exactly like built-in Expectations. All out-of-the-box Expectations will still be available, plus your new methods.

    .. code-block:: python

        my_data_asset.expect_column_values_to_be_even("ReportsTo")

    returns

    .. code-block:: json

        {
            "exception_info": null,
            "success": false,
            "result": {
                "element_count": 8,
                "missing_count": 1,
                "missing_percent": 12.5,
                "unexpected_count": 2,
                "unexpected_percent": 25.0,
                "unexpected_percent_nonmissing": 28.57142857142857,
                "partial_unexpected_list": [
                    1,
                    1
                ]
            },
            "meta": {}
        }

    As mentioned previously, that the ``column_map_expectation`` decorator extends the arguments to include other arguments, like ``mostly``. Please see the module documentation for full details.

    .. code-block:: python

        my_df.expect_column_values_to_be_even("ReportsTo", mostly=.7)

    returns

    .. code-block:: json

        {
            "exception_info": null,
            "success": true,
            "result": {
                "element_count": 8,
                "missing_count": 1,
                "missing_percent": 12.5,
                "unexpected_count": 2,
                "unexpected_percent": 25.0,
                "unexpected_percent_nonmissing": 28.57142857142857,
                "partial_unexpected_list": [
                    1,
                    1
                ]
            },
            "meta": {}
        }

    Often, the best development loop for custom Expectations is iterative: editing Expectations in ``MyCustomPandasDataset``, then re-running the cells to load data and execute Expectations on data.

Additional notes
----------------


Other Expectation decorators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Aside from ``column_map_expectations``, there are several other types of Expectations you can create.

- ``column_aggregate_expectations`` generate a single observed value for a whole column.
- ``column_pair_map_`` and ``column_pair_aggregate_expectations`` apply to pairs of columns.
- ``multicolumn_map_`` and ``multicolumn_aggregate_expectations`` apply to multiple columns.
- It's also possible to define table-level Expectations using the ``@expectations`` decorator.
- Not to mention non-tabular Expectations, using other DataAsset types, like :ref:`FileDataAsset`.

Please refere to the module documentation and tests for details on how to implement each of these.


Additional resources
--------------------

Here's a single code block containing all the code in this article:

.. code-block:: python

    import sqlalchemy as sa
    from great_expectations.dataset import SqlAlchemyDataset, MetaSqlAlchemyDataset

    class CustomSqlAlchemyDataset(SqlAlchemyDataset):

        _data_asset_type = "CustomSqlAlchemyDataset"

        def expect_column_values_to_be_even(self, column):
            return (sa.column(column) % 2 == 0)

    # Loading a DataAsset using bare SQLAlchemy
    my_data_asset = CustomSqlAlchemyDataset("employees", sa.create_engine("sqlite:///data/chinook.db"))
    assert my_data_asset.expect_column_values_to_be_equal("ReportsTo").success = False
    assert my_data_asset.expect_column_values_to_be_equal("ReportsTo", mostly=.7).success = True

    # Loading a DataAsset using a DataContext
    import great_expectations as ge
    context = ge.DataContext()

    my_data_asset = context.get_batch(
        my_batch_kwargs,
        ExpectationSuite("my_temporary_test_suite"),
        CustomSqlAlchemyDataset,
    )
    assert my_data_asset.expect_column_values_to_be_equal("ReportsTo").success = False
    assert my_data_asset.expect_column_values_to_be_equal("ReportsTo", mostly=.7).success = True


Comments
--------

.. discourse::
    :topic_identifier: 203
