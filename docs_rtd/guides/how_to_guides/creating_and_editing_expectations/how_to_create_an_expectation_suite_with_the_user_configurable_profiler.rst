.. _how_to_guides__creating_and_editing_expectations__how_to_create_an_expectation_suite_with_the_user_configurable_profiler:

How to create an Expectation Suite with the User Configurable Profiler
=========================================================================

This guide will help you create a new Expectation Suite by profiling your data with the User Configurable Profiler.

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Configured and loaded a DataContext <how_to_guides__configuring_data_contexts>`
            - Configured a :ref:`Datasource <how_to_guides__configuring_datasources>`
            - Identified the table that you would like to profile.

        **Steps**

        1. Load or create a Data Context

            The ``context`` referenced below can be loaded from disk or configured in code.

            Load an on-disk Data Context via:

            .. code-block:: python

                import great_expectations as ge
                from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
                context = ge.data_context.DataContext(
                    context_root_dir='path/to/my/context/root/directory/great_expectations'
                )

            Create an in-code Data Context using these instructions: :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>`


        2. Create a new Expectation Suite

            .. code-block:: python

                expectation_suite_name = "insert_your_expectation_suite_name_here"
                suite = context.create_expectation_suite(
                    expectation_suite_name
                )


        3. Construct batch_kwargs and get a Batch

            ``batch_kwargs`` describe the data you plan to validate. In the example below we use a Datasource configured for SQL Alchemy. This will be a little different for SQL, Pandas, and Spark  back-ends - you can get more info on the different ways of instantiating a batch here: :ref:`How to create a batch <how_to_guides__creating_batches>`

            .. code-block:: python

                batch_kwargs = {
                    "datasource": "insert_your_datasource_name_here",
                    "schema": "your_schema_name",
                    "table": "your_table_name",
                    "data_asset_name": "your_table_name",
                }


            Then we get the Batch via:

            .. code-block:: python

                batch = context.get_batch(
                    batch_kwargs=batch_kwargs,
                    expectation_suite_name=expectation_suite_name
                )


        4. Check your data

            You can check that the first few lines of your Batch are what you expect by running:

            .. code-block:: python

                batch.head()


        5. Instantiate a UserConfigurableProfiler

            Next, we instantiate a UserConfigurableProfiler, passing in our batch

            .. code-block:: python

                profiler = UserConfigurableProfiler(dataset=batch)


        6. Use the profiler to build a suite

            Now that we have our profiler set up with our batch, we can use the build_suite method on the profiler. This will print a list of all the expectations created by column, and return the Expectation Suite object.

            .. code-block:: python

                suite = profiler.build_suite()

        7. (Optional) Running validation, saving the suite and building Data Docs

            If you'd like, you can validate your data with the new suite, save your Expectation Suite and build Data Docs to take a closer look

            .. code-block:: python

                # We need to re-create our batch to link the batch with our new suite
                batch = context.get_batch(
                batch_kwargs=batch_kwargs,
                expectation_suite_name=expectation_suite_name)

                # Running validation
                results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch])
                validation_result_identifier = results.list_validation_result_identifiers()[0]

                # Saving our expectation suite
                context.save_expectation_suite(suite, expectation_suite_name)

                # Building and opening Data Docs
                context.build_data_docs()
                context.open_data_docs(validation_result_identifier)


        **Optional Parameters**

        The UserConfigurableProfiler can take a few different parameters to further hone the results. These parameter are:

            - ``excluded_expectations``: Takes a list of expectation names which you want to exclude from the suite


            - ``ignored_columns``: Takes a list of columns for which you may not want to build expectations (i.e. if you have metadata which might not be the same between tables


            - ``not_null_only``: Takes a boolean. By default, each column is evaluated for nullity. If the column values contain fewer than 50% null values, then the profiler will add ``expect_column_values_to_not_be_null``; if greater than 50% it will add ``expect_column_values_to_be_null``. If ``not_null_only`` is set to True, the profiler will add a not_null expectation irrespective of the percent nullity (and therefore will not add an ``expect_column_values_to_be_null``)


            - ``primary_or_compound_key``: Takes a list of one or more columns. This allows you to specify one or more columns as a primary or compound key, and will add ``expect_column_values_to_be_unique`` or ``expect_compound_column_values_to_be_unique``


            - ``table_expectations_only``: Takes a boolean. If True, this will only create table-level expectations (i.e. ignoring all columns). Table-level expectations include ``expect_table_row_count_to_equal`` and ``expect_table_columns_to_match_ordered_list``


            - ``value_set_threshold``: Takes a string from the following ordered list - "none", "one", "two", "very_few", "few", "many", "very_many", "unique". When the profiler runs, each column is profiled for cardinality. This threshold determines the greatest cardinality for which to add ``expect_column_values_to_be_in_set``. For example, if ``value_set_threshold`` is set to "unique", it will add a value_set expectation for every included column. If set to "few", it will add a value_set expectation for columns whose cardinality is one of "one", "two", "very_few" or "few". The default value here is "many". For the purposes of comparing whether two tables are identical, it might make the most sense to set this to "unique".


            - ``semantic_types_dict``: Takes a dictionary. Described in more detail below.

        If you would like to make use of these parameters, you can specify them while instantiating your profiler.

            .. code-block:: python

                excluded_expectations = ["expect_column_quantile_values_to_be_between"]
                ignored_columns = ['c_comment', 'c_acctbal', 'c_mktsegment', 'c_name', 'c_nationkey', 'c_phone']
                not_null_only = True
                table_expectations_only = False
                value_set_threshold = "unique"

                suite = context.create_expectation_suite(
                expectation_suite_name, overwrite_existing=True)

                batch = context.get_batch(batch_kwargs, suite)

                profiler = UserConfigurableProfiler(
                    dataset=batch,
                    excluded_expectations=excluded_expectations,
                    ignored_columns=ignored_columns,
                    not_null_only=not_null_only,
                    table_expectations_only=table_expectations_only,
                    value_set_threshold=value_set_threshold)

                suite = profiler.build_suite()


            **Once you have instantiated a profiler with parameters specified, you must re-instantiate the profiler if you wish to change any of the parameters.**


        **Semantic Types Dictionary Configuration**

        The profiler is fairly rudimentary - if it detects that a column is numeric, it will create numeric expectations (e.g. ``expect_column_mean_to_be_between``). But if you are storing foreign keys or primary keys as integers, then you might not want numeric expectations on these columns. This is where the semantic_types dictionary comes in.

        The available semantic types that can be specified in the UserConfigurableProfiler are "numeric", "value_set", and "datetime". The expectations created for each of these types is below. You can pass in a dictionary where the keys are the semantic types, and the values are lists of columns of those semantic types.

        When you pass in a ``semantic_types_dict``, the profiler will still create table-level expectations, and will create certain expectations for all columns (around nullity and column proportions of unique values). It will then only create semantic-type-specific expectations for those columns specified in the semantic_types dict.


            .. code-block:: python

                semantic_types_dict = {
                "numeric": ["c_acctbal"],
                "value_set": ["c_nationkey","c_mktsegment", 'c_custkey', 'c_name', 'c_address', 'c_phone', "c_acctbal"]
                }
                suite = context.create_expectation_suite(
                    expectation_suite_name, overwrite_existing=True)
                batch = context.get_batch(batch_kwargs, suite)
                profiler = UserConfigurableProfiler(
                    dataset=batch,
                    semantic_types_dict=semantic_types_dict
                )
                suite = profiler.build_suite()


            The expectations added using a `semantics_type` dict are the following:

            **Table expectations:**

            - ``expect_table_row_count_to_be_between``

            - ``expect_table_columns_to_match_ordered_list``

            **Expectations added for all included columns**

            - ``expect_column_value_to_not_be_null`` (if a column consists of more than 50% null values, this will instead add ``expect_column_values_to_be_null``)

            - ``expect_column_proportion_of_unique_values_to_be_between``

            - ``expect_column_values_to_be_in_type_list``

            **Value set expectations**

            - ``expect_column_values_to_be_in_set``

            **Datetime expectations**

            - ``expect_column_values_to_be_between``

            **Numeric expectations**

            - ``expect_column_min_to_be_between``

            - ``expect_column_max_to_be_between``

            - ``expect_column_mean_to_be_between``

            - ``expect_column_median_to_be_between``

            - ``expect_column_quantile_values_to_be_between``

            **Other expectations**

            - ``expect_column_values_to_be_unique`` (if a single key is specified for ``primary_or_compound_key``)

            - ``expect_compound_columns_to_be_unique`` (if a compound key is specified for ``primary_or_compound_key``)

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. admonition:: The UserConfigurableProfiler is not currently configured to work with the 0.13 Experimental API.

            We expect to update this in the near future.


.. discourse::
    :topic_identifier: 634
