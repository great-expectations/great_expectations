.. _how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations_for_pandas:

How to create custom Expectations for pandas
============================================

Custom Expectations let you extend the logic for validating data to use any criteria you choose. This guide will show you how to extend the ``PandasDataset`` class with your own :ref:`Expectations`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - Launched a generic notebook (e.g. ``jupyter notebook``, ``jupyter lab``, etc.)
    - Obtained data that can be accessed from your notebook
    - :ref:`Configured a pandas Datasource <how_to_guides__configuring_datasources__how_to_configure_a_pandas_filesystem_datasource>`

Steps
-----

1. **Import great_expectations and PandasDataset and MetaPandasDataset**

    .. code-block:: python

        import great_expectations as ge
        from great_expectations.dataset import (
            PandasDataset,
            MetaPandasDataset,
        )

    ``PandasDataset`` is the parent class used for executing Expectations on pandas Dataframes. Most of the core Expectations are built using decorators defined in ``MetaPandasDataset``. These decorators greatly streamline the task of extending Great Expectations with custom Expectation logic.

2. **Define a class inheriting from PandasDataset**

    .. code-block:: python

        class MyCustomPandasDataset(PandasDataset):

            _data_asset_type = "MyCustomPandasDataset"

    Setting the ``_data_asset_type`` is not strictly necessary, but can be helpful for tracking the lineage of instantiated Expectations and :ref:`Validation Results <reference__core_concepts__validation__expectation_validation_result>`.

3. **Within your new class, define Expectations using decorators from MetaPandasDataset**

    ``column_map_expectations`` are Expectations that are applied to a single column, on a row-by-row basis. To learn about other Expectation types, please see :ref:`Other Expectation decorators <other_expectation_decorators>` below.

    The ``@MetaPandasDataset.column_map_expectation`` decorator wraps your custom function with all the business logic required to turn it into a fully-fledged Expectation. This spares you the hassle of defining logic to handle required arguments like ``mostly`` and ``result_format``. Your custom function can focus exclusively on the business logic of passing or failing the Expectation.

    In the simplest case, they could be as simple as one-line lambda functions.

    .. code-block:: python

        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_even(self, column):
            return column.map(lambda x: x%2==0)


    To use the ``column_map_expectation`` decorator, your custom function must accept at least two arguments: ``self`` and ``column``. When the user invokes your Expectation, they will pass a string containing the column name. The decorator will then fetch the appropriate column and pass all of the non-null values to your function as a pandas ``Series``. Your function must then return a Series of boolean values in the same order, with the same index.

    Custom functions can also accept additional arguments:

    .. code-block:: python

        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_less_than(self, column, value):
            return column.map(lambda x: x<value)

    Custom functions can have complex internal logic:

    .. code-block:: python

        @MetaPandasDataset.column_map_expectation
        def expect_column_value_word_counts_to_be_between(self, column, min_value=None, max_value=None):        
            def count_words(string):
                word_list = re.findall("(\S+)", string)
                return len(word_list)

            word_counts = column.map(lambda x: count_words(str(x)))

            if min_value is not None and max_value is not None:
                return word_counts.map(lambda x: min_value <= x <= max_value)
            elif min_value is not None and max_value is None:
                return word_counts.map(lambda x: min_value <= x)
            elif min_value is None and max_value is not None:
                return word_counts.map(lambda x: x <= max_value)
            else:
                return word_counts.map(lambda x: True)

    Custom functions can reference external modules and methods:

    .. code-block:: python

        import pytz

        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_valid_timezones(self, column, timezone_values=pytz.all_timezones):
            return column.map(lambda x: x in timezone_values)

    By convention, ``column_map_expectations`` always start with ``expect_column_values_...`` or ``expect_column_value_...`` (Ex: ``expect_column_value_word_counts_to_be_between``). Following this pattern is highly recommended, but not strictly required. If you want to confuse yourself with bad names, the package won't stop you.


4. **Load some data**

    To make your new Expectations available for validation, you can instantiate a ``MyCustomPandasDataset`` as follows:

    .. code-block:: python

        my_df = ge.read_csv("./data/Titanic.csv", dataset_class=MyCustomPandasDataset)

    You can also coerce an existing pandas DataFrame to your class using ``from_pandas``:

    .. code-block:: python

        my_pd_df = pd.read_csv("./data/Titanic.csv")
        my_df = ge.from_pandas(my_pd_df, dataset_class=MyCustomPandasDataset)

    As a third option:

    .. code-block:: python

        my_pd_df = pd.read_csv("./data/Titanic.csv")
        my_df = MyCustomPandasDataset(my_pd_df)

    Note: We're using the ``read_csv`` method to fetch data, instead of the more typical ``DataContext.get_batch``. This is for convenience: it allows us to handle the full development loop for a custom Expectation within a notebook with a minimum of configuration.
    
    In a moment, we'll demonstrate how to configure a Datasource to use ``MyCustomPandasDataset`` when calling ``get_batch``.

5. **Test your Expectations**

    At this point, you can test your new Expectations exactly like built-in Expectations. All out-of-the-box Expectations will still be available, plus your new methods.

    .. code-block:: python

        my_df.expect_column_values_to_be_even("Survived")

    returns

    .. code-block:: json

        {
            "success": false,
            "meta": {},
            "result": {
                "element_count": 1313,
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_count": 450,
                "unexpected_percent": 34.27265803503427,
                "unexpected_percent_nonmissing": 34.27265803503427,
                "partial_unexpected_list": [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
            },
            "exception_info": null
        }

    As mentioned previously, the ``column_map_expectation`` decorator extends the arguments to include other arguments, like ``mostly``. Please see the module documentation for full details.

    .. code-block:: python

        my_df.expect_column_values_to_be_even("Survived", mostly=.6)

    returns

    .. code-block:: json

        {
            "success": true,
            "meta": {},
            "result": {
                "element_count": 1313,
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_count": 450,
                "unexpected_percent": 34.27265803503427,
                "unexpected_percent_nonmissing": 34.27265803503427,
                "partial_unexpected_list": [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
            },
            "exception_info": null
        }

    Often, the best development loop for custom Expectations is iterative: editing Expectations in ``MyCustomPandasDataset``, then re-running the cells to load data and execute Expectations on data.

    |

    At this point, your custom Expectations work---but only within a notebook. Next, let's configure them to work from within a Datasource in your Data Context.

#. **Save your MyCustomPandasDataset class to a Plugin module**

    The simplest way to do this is to create a new, single-file python module within your ``great_expectations/plugins/`` directory. Name it something like ``custom_pandas_dataset.py``. Copy the full contents of your ``MyCustomPandasDataset`` class into this file. Make sure to include any required imports, too.

    When you instantiate a Data Context, Great Expectations automatically adds ``plugins/`` to the python namespace, so your class can be imported as ``custom_pandas_dataset.MyCustomPandasDataset``.
    
#. **Configure your Datasource(s)**

    Now, open your ``great_expectations.yml`` file. Assuming that you've previously :ref:`configured a pandas Datasource <how_to_guides__configuring_datasources__how_to_configure_a_pandas_filesystem_datasource>`, you should see a configuration block similar to this, under the ``datasources`` key:

    .. code-block:: yaml

        my_data__dir:
            module_name: great_expectations.datasource
            class_name: PandasDatasource

            data_asset_type:
                module_name: great_expectations.dataset
                class_name: PandasDataset

            batch_kwargs_generators:
                subdir_reader:
                class_name: SubdirReaderBatchKwargsGenerator
                base_directory: ../my_data

    In the ``data_asset_type`` section, replace ``module_name`` and ``class_name`` with names for your module and class:

    .. code-block:: yaml

        data_asset_type:
            module_name: custom_pandas_dataset
            class_name: MyCustomPandasDataset

    Now, any time you load data through the ``my_data__dir`` Datasource, it will be loaded as a ``MyCustomPandasDataset``, with all of your new Expectations available.

    If you have other ``PandasDatasources`` in your configuration, you may want to switch them to use your new ``data_asset_type``, too.

#. **Test loading a new Batch through the DataContext**

    You can test this configuration as follows:

    .. code-block:: python

        context = ge.DataContext()
        context.create_expectation_suite("my_new_suite")
        my_batch = context.get_batch({
            "path": "my_data/Titanic.csv",
            "datasource": "my_data__dir"
        }, "my_new_suite")

        my_batch.expect_column_values_to_be_even("Age")


    Executing this Expectation should return something like:

    .. code-block::

        {
            "result": {
                "element_count": 1313,
                "missing_count": 557,
                "missing_percent": 42.421934501142424,
                "unexpected_count": 344,
                "unexpected_percent": 26.199543031226202,
                "unexpected_percent_nonmissing": 45.5026455026455,
                "partial_unexpected_list": [
                    29.0,
                    25.0,
                    0.92,
                    ...
                    59.0,
                    45.0
                ]
            },
            "success": false,
            "meta": {},
            "exception_info": null
        }

Additional notes
----------------

.. _other_expectation_decorators:

Other Expectation decorators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Aside from ``column_map_expectations``, there are several other types of Expectations you can create. Please see the module docs for :py:class:`~great_expectations.dataset.pandas_dataset.MetaPandasDataset` for details.


Additional resources
--------------------

Here's a single code block containing all the notebook code in this article:

.. code-block:: python

    import re
    import pytz

    import great_expectations as ge
    from great_expectations.dataset import (
        PandasDataset,
        MetaPandasDataset,
    )

    class MyCustomPandasDataset(PandasDataset):
        _data_asset_type = "MyCustomPandasDataset"

        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_even(self, column):
            return column.map(lambda x: x%2==0)

        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_less_than(self, column, value):
            return column.map(lambda x: x < value)

        @MetaPandasDataset.column_map_expectation
        def expect_column_value_word_counts_to_be_between(self, column, min_value=None, max_value=None):
            def count_words(string):
                word_list = re.findall("(\S+)", string)
                return len(word_list)

            word_counts = column.map(lambda x: count_words(str(x)))

            if min_value is not None and max_value is not None:
                return word_counts.map(lambda x: min_value <= x <= max_value)
            elif min_value is not None and max_value is None:
                return word_counts.map(lambda x: min_value <= x)
            elif min_value is None and max_value is not None:
                return word_counts.map(lambda x: x <= max_value)
            else:
                return word_counts.map(lambda x: True)

        @MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_valid_timezones(self, column, timezone_values=pytz.all_timezones):
            return column.map(lambda x: x in timezone_values)

    
    #Instantiate the class in several different ways
    my_df = ge.read_csv("my_data/Titanic.csv", dataset_class=MyCustomPandasDataset)

    my_other_df = pd.read_csv("my_data/Titanic.csv")
    ge.from_pandas(my_other_df, dataset_class=MyCustomPandasDataset)

    my_other_df = ge.read_csv("my_data/Titanic.csv")
    ge.from_pandas(my_other_df, dataset_class=MyCustomPandasDataset)

    # Run Expectations in assertions so that they can be used as tests for this guide
    assert my_df.expect_column_values_to_be_in_set("Sex", value_set=["Male", "Female"]).success == False
    assert my_df.expect_column_values_to_be_even("Survived").success == False
    assert my_df.expect_column_values_to_be_even("Survived", mostly=.6).success == True
    assert my_df.expect_column_value_word_counts_to_be_between("Name", 3, 5).success == False
    assert my_df.expect_column_value_word_counts_to_be_between("Name", 3, 5, mostly=.9).success == True
    assert my_df.expect_column_values_to_be_valid_timezones("Name", mostly=.9).success == False

Comments
--------

.. discourse::
    :topic_identifier: 201
