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

        .. admonition:: The UserConfigurableProfiler is Experimental

            - The UserConfigurableProfiler is Experimental and this document is aspirational and purely meant to capture our collective understanding as we design and build this feature. It will need significant edits to transition this into a how-to guide. We expect to update this in the near future. Some notes on items to edit / investigate are below.
            - Profiler is the proposed name for the new profiler, and it will likely assimilate the UserConfigurableProfiler functionality.
            - Table-based data is assumed for now in this doc for simplicity. This should be extended when appropriate.
            - GEN1 refers to our current plan, GEN2 refers to ideas that may or may not be addressed in future versions. Currently the only difference is that GEN2 allows for globally defined parameters and domains that can be used in multiple rules. It does not support multiple domains in a single rule.


        NOTE: ONLY A SECTION OF THIS DOCUMENT HAS BEEN UPDATED - PLEASE SEE NOTES


        TODO: This text is just for internal communication so far and must be cross referenced with existing docs especially the Core Concepts docs before being released.

        The aim of the Great Expectations Profiler is to describe your existing data automatically by scanning it (or a subset of it) to create an Expectation Suite. The resulting Expectation Suite should describe your existing data well enough that new data (that is well behaved) should still pass when validated against that Expectation Suite. Of course the generated Expectation Suite can be manually modified using the existing Great Expectations Suite Edit functionality or re-generated via a modified Profiler if the data changes.

        To accomplish that aim, it is highly configurable. To describe its functionality and configuration, we will walk through some of the core concepts followed by examples of increasing complexity. For now, we will use table-based data in this document for simplicity of explanation.

        A Profiler is defined by a set of ProfilerRules that create Expectations for configurable Domains. These ProfilerRules and Expectations can be built using Parameters calculated at runtime, or hard-coded. GEN1: These Parameters are local to a domain. GEN2: These Parameters can be local to a domain or global to all domains (QUESTION: should the ``variables`` key be renamed to something more descriptive like ``global_profiler_parameters``? ANSWER: TBD, keep as variables for now).

        Here is a pseudocode hirearchy to show some of the various core concepts and their relationships (more complex concepts are covered later). Everything prefaced with a ``my_`` is user configurable. Note that items ending with a colon ``:`` or lists ending with ellipses ``...`` in this pseudocode likely require more configuration or support additional items that are not shown.

        (QUESTION: Can there be a hirearchy of domains or at least multiple domains for a given rule or should the rule-domain pair be one-to-one? Maybe domains / parameters can be defined at the same level as rules and then referenced (rather than having to be defined within a rule). Currently it looks like the idea is that more complex domain configurations are meant to be captured in specific domain builder classes e.g. ``SimpleSemanticTypeColumnDomainBuilder`` and that there is a single domain per rule. ANSWER: Group consensus seems to be push complexity into code rather than config i.e. single Domain per ProfilerRule that is defined by a subclass of DomainBuilder)

        NOTE: Please see the last example, it was edited as of 20210423, these have not yet been updated.

        GEN1 (Pseudocode):

        .. code-block:: yaml

          variables: # global variables accessed in any domain_builder, parameter_builder or expectation_configuration_builder via $ substitution e.g. $variable_name, config not shown
          rules:
            - my_rule_1
              domain_builder:
                - my_domain_builder_for_my_rule_1 # (Note: single domain builder per rule): # config not shown
              parameter_builders:
                - my_rule_1_parameter_builder_1: # config not shown
                # e.g. reference columns from my_domain_builder_for_my_rule_1 to calculate metric on each of them (e.g. ``column.min`` metric)
                - my_rule_2_parameter_builder_2: # config not shown
                ...
              expectation_configuration_builders:
                - expectation_configuration_1_builder: # config not shown
                # e.g. expect_column_values_to_be_between where domain is set via columns from the domain defined above and min/max set via parameters defined above. Can also contain if/then logic.
                ...

        GEN2 (Pseudocode):

        .. code-block:: yaml

          variables: # global variables accessed in any domain_builder, parameter_builder or expectation_configuration_builder via $ substitution e.g. $variable_name, config not shown
          global_domain_builders:
            # To be referenced in rules (supports multiple rules using the same domain)
            - my_global_domain_builder_1: # config not shown
            - my_global_domain_builder_2: # config not shown
            ...
          global_parameter_builders:
            # To be referenced in rules (supports multiple rules using the same parameter)
            - my_global_param_builder_1: # config not shown
            - my_global_param_builder_2: # config not shown
            ...
          rules:
            - my_rule_1
              rule_specific_domain_builder:
                - my_rule_1_domain_builder: # single domain builder as in GEN1, but may be re-used if globally defined. config not shown
              rule_specific_parameter_builders:
                - my_rule_1_parameter_builder_1: # config not shown
                # e.g. reference columns from my_rule_1_domain_builder to calculate metric on each of them (e.g. ``column.min`` metric)
                - my_rule_2_parameter_builder_2: # config not shown
                - my_global_param_builder_1 # Reference global parameter builder
                ...
              expectation_configuration_builders:
                - expectation_1_builder:
                # e.g. expect_column_values_to_be_between where domain is set via columns from the domain defined above and min/max set via parameters defined above. Can also contain if/then logic.
            - my_rule_2
              rule_specific_domain_builder:
                - my_global_domain_builder_1 # Reference global domain builder
              rule_specific_parameter_builders:
                - my_global_param_builder_1 # Reference global parameter builder (note used more than once)
              ...
            - my_rule_3
              rule_specific_domain_builder:
                - my_global_domain_builder_1 # Reference global domain builder (note used more than once)
              ...


        Here is a slightly more concrete example:

        GEN1:

        .. code-block:: yaml

          variables: # global variables accessed in any domain_builder, parameter_builder or expectation_configuration_builder via $ substitution e.g. $variable_name, config not shown
            - global_var_1:
          rules:
            my_rule_for_ids: # Could be a semantic type
              domain_builder:
                my_id_domain:
                  # config not shown: columns of type ``integer`` with name ``id`` or suffix ``_id`` (QUESTION: Is the domain just a series of columns or does it also define a specific batch or batches?)
              parameter_builders:
                - my_id_parameter_1_min:
                  # config not shown: min value of column, using domain above
                - my_id_parameter_2_max:
                  # config not shown: max value of column, using domain above
                - my_id_parameter_3_min_5_batches_ago:
                  # config not shown: min value of column from 5 batches ago, using domain above and using a ``batch_request`` modifier to index to ``-5`` batches.
                - my_id_parameter_4_mean_of_last_10_batches:
                  # config not shown: mean of last 10 batches, using domain above and using a ``batch_request`` modifier to index via slice the last ``-10:`` batches.
              expectation_configuration_builders:
                - expectation: expect_column_values_to_be_between # Expectation name as string
                  column: $my_id_domain_1.domain_kwargs.column # Reference to all columns in referenced domain (expectation built with each one in turn) (QUESTION: Can we omit / guess that ``domain_kwargs`` is part of this column definition so it can be shortened to ``$my_id_domain_1.column``?)
                  min_value: $my_id_parameter_1_min.parameter.min # Reference to parameter defined above
                    (QUESTION: 1. How do we define a relative parameter e.g. like a window function of sorts - e.g. the parameter is "mean of column from 5 batches before the batch that this expectation is built from" not 5 batches back from the domain definition (I may be thinking about this wrong). 2. Can we omit / guess that ``parameter`` is part of this parameter definition so it can be shortened to ``$my_id_parameter_2_max.max``?)
                  max_value: $my_id_parameter_2_max.parameter.max
                - expectation: expect_column_values_to_be_between
                  column: $my_id_domain_2.column # Note "domain_kwargs" assumed
                  min_value: $my_id_parameter_3_min_5_batches_ago.parameter.min
                  max_value: $global_var_1 # Note use of global variable
                - expectation: expect_column_values_to_be_between
                  column: $my_id_domain_2.column
                  min_value: $my_id_parameter_4_mean_of_last_10_batches.parameter.mean
                  max_value: $global_var_1
                  mostly: 0.5
                - expectation: expect_column_values_to_not_be_null
                  column: $domains.column # columns for all domains

            my_rule_for_datetimes: # semantic type
              domains:
                - my_datetime_domain_1: # QUESTION: Should we allow for multiple domains in a rule? QUESTION: Can / should domains be defined outside of the scope of a specific rule?
                  # columns of type ``datetime`` (using the ``semantic_types`` field)
                - my_datetime_domain_2
                  # columns with the suffix ``_dt``
              parameters:
                - $my_dateformat_parameter_1:
                  # string format
              expectation_configuration_builders:
                - expectation: expect_column_values_to_match_strftime_format
                column: $domains.column # columns for all domains
                strftime_format: $my_dateformat_parameter_1.parameter.date_string


        .. admonition:: The below examples are not yet modified per our conversations.

            - TODO: The below examples are not yet modified per our conversations.

        GEN2:

        .. code-block:: yaml

          variables: # global variables accessed in any domain_builder, parameter_builder or expectation_configuration_builder via $ substitution e.g. $variable_name, config not shown
          global_domain_builders:
            # To be referenced in rules (supports multiple rules using the same domain)
            - my_global_domain_builder_1: # config not shown
            - my_global_domain_builder_2: # config not shown
            ...
          global_parameter_builders:
            # To be referenced in rules (supports multiple rules using the same parameter)
            - my_global_param_builder_1: # config not shown
            - my_global_param_builder_2: # config not shown
            ...
          rules:
            my_rule_for_ids: # Could be a semantic type
              domain_builder:
                my_id_domain:
                  # config not shown: columns of type ``integer`` with name ``id`` or suffix ``_id`` (QUESTION: Is the domain just a series of columns or does it also define a specific batch or batches?)
              parameter_builders:
                - my_id_parameter_1_min:
                  # config not shown: min value of column, using domain above
                - my_id_parameter_2_max:
                  # config not shown: max value of column, using domain above
                - my_id_parameter_3_min_5_batches_ago:
                  # config not shown: min value of column from 5 batches ago, using domain above and using a ``batch_request`` modifier to index to ``-5`` batches.
                - my_id_parameter_4_mean_of_last_10_batches:
                  # config not shown: mean of last 10 batches, using domain above and using a ``batch_request`` modifier to index via slice the last ``-10:`` batches.

BOOKMARK
              expectation_configuration_builders:
                - expectation: expect_column_values_to_be_between # Expectation name as string
                  column: $domain.my_id_domain_1.column # All columns in referenced domain (QUESTION: Why ``domain_kwargs`` in examples e.g. ``$domain.domain_kwargs.column``?)
                  min_value: $my_id_parameter_1_min.parameter.min # Reference to parameter defined above
                  max_value: $my_id_parameter_2_max.parameter.min
                - expectation: expect_column_values_to_be_between
                  column: $domain.my_id_domain_2.column
                  min_value: $my_id_parameter_3_min_5_batches_ago.parameter.min
                  max_value: $global_var_1
                - expectation: expect_column_values_to_not_be_null
                  column: $domains.column # columns for all domains

            my_rule_for_datetimes: # semantic type
              domains:
                - my_datetime_domain_1: # QUESTION: Should we allow for multiple domains in a rule? QUESTION: Can / should domains be defined outside of the scope of a specific rule?
                  # columns of type ``datetime`` (using the ``semantic_types`` field)
                - my_datetime_domain_2
                  # columns with the suffix ``_dt``
              parameters:
                - $my_dateformat_parameter_1:
                  # string format
              expectation_configuration_builders:
                - expectation: expect_column_values_to_match_strftime_format
                column: $domains.column # columns for all domains
                strftime_format: $my_dateformat_parameter_1.parameter.date_string


        TODO: insert more examples with actual code.

        TODO: This is an example from James' spec document copied over verbatim, and should be edited. It has been edited as of 20210423

        # QUESTION - what is the $something.something syntax?
        # PROPOSED ANSWER: $instance_name(parameter_name,domain_name - maybe `domain` is ok and assume the rule domain).attribute(instance or class).sub_attribute.sub_sub_attribute...
        # TODO: DomainBuilders are 1:1, not Domains


        # NOTE: PLEASE SEE BELOW FOR UPDATED CODE AS OF 20210423 ------------------------------------------------------

        .. code-block:: yaml

          variables:
            false_positive_threshold: 0.01
          rules:
            my_rule_for_datetimes:  # "just-a-name"
              # JPC: what is happening here -- we're asking, "Which columns in this data are datetimes?"
              domain_builder:
                name: my_domain_builder_name # QUESTION: This is added - is that OK?
                class_name: SimpleSemanticTypeColumnDomainBuilder
                semantic_types:  # AJB: semantic_types are defined in the Domain Builder and referenced here by name
                                 #      domains are checked against these types for inclusion in the rule domain.
                                 #      All domains corresponding to any of the semantic_types are included.
                                 # semantic_type key is determined by the domain builder not global e.g. SimpleSuffixColumnDomainBuilder where suffix is the keyword.
                  - datetime
                user_input_list_of_domain_names: # TBD - this is maybe batch_id,domain_id together - probably Typed Object DomainId - can add human readable IDs. Probably too much complexity for first version.
              parameter_builders:
                - name: my_dateformat  # This is shorthand for `parameter_builder_name`
                  # JPC: what is happening here -- we're asking, "What date format matches the data in this column?"
                  class_name: SimpleDateFormatStringParameterBuilder
                  module_name: # OPTIONAL
                  domain_builder_kwargs: my_domain_builder_name.domain_kwargs # QUESTION: if there is only one domain_builder, can this just be assumed?
              expectation_configuration_builders:
                # TODO: BranchingExpectationConfigurationBuilder - with parameters that take key value for gt lt gte lte
                # Keep this if-then complexity in the Expectation rather than in the config
                # Propose delaying until later versions, but this is directional design

                - branch: # branch is optional if/then syntax. You can also just put the expectation
                    if: $my_dateformat.success_ratio >= 0.8  # if evaluates to true in python, i.e. "" is FALSE, "%Y" is TRUE
                                                                   # This success_ratio is provided via `details` of a parameter
                                                                   # It is matched with the correct domain
                                                                   # QUESTION: I removed the `parameter` in between the
                                                                   # parameter_name and success_ratio - perhaps we can
                                                                   # simplify these lookups - ANSWER - Cool, not sure exaclty how to implement but let's try it.
                    then:
                      # ExpectationConfigurationBuilder configuration goes here
                      - name: my_expectation_configuration_builder_1
                        class_name: DefaultExpectationConfigurationBuilder # Optional, will use Default if not supplied, otherwise will use supplied
                        module_name: tbd # Optional, for all other
                        expectation_type: expect_column_values_to_match_strftime_format # QUESTION: Should we change this from expectation to expectation_type?
                        column: $my_domain_builder.domain_kwargs.column  # is this obvious/inferrable?
                                                              # AJB - I think some may wish to be more explicit but yes
                                                              # this should be inferrable when there is a 1-1 between
                                                              # ProfilerRule and Domain Builder. So make this line optional.
                        strftime_format: $my_dateformat.date_format_string # This is an expectation kwarg
                                                                                     # (Parameters in our docs)
                                                                                     # QUESTION: removed `parameter`
                - branch:
                    if: $domain.column_type in my_dateformat.DATETIME_TYPES_CONST  # Note that domain returned "column_type" in addition to "domain_kwargs" - Note also that DATEITME_TYPES_CONST doesn't yet exist but would be a class variable of the SimpleDateFormatStringParameterBuilder
                                                                                   # QUESTION: Is this a "subdomain"? It kind of feels against the 1 domain per ProfilerRule, but makes sense. Suppose I want to reference this elsewhere, I guess for now we copy paste.
                    then:
                      - expectation: expect_column_values_to_be_in_type_list
                        column: $domain_builder.domain_kwargs.column  # is this obvious/inferrable? # YES optional, see above
                        type_list: my_dateformat.DATETIME_TYPES_CONST # See above note
            my_rule_for_numerics:  # "just-a-name"
              # Which columns in this data are numeric?
              class_name: SemanticTypeColumnDomainBuilder
              semantic_types:
                - numeric
              parameter_builders:
                - parameter_name: my_parameter_mean
                  class_name: MetricParameterBuilder
                  metric_name: column.mean
                  metric_domain_kwargs: $domain.domain_kwargs # QUESTION: Should this be optional and inferred?
                - parameter_name: my_parameter_min
                  class_name: MetricParameterBuilder
                  metric_name: column.min
                  metric_domain_kwargs: $domain.domain_kwargs # QUESTION: Should this be optional and inferred?
                - parameter_name: my_parameter_false_positive_threshold_min
                  class_name: MultiBatchBootstrappedMetricDistributionParameterBuilder
                  batch_request:  # this should be unioned with the rest of the batch request from the call to profile
                    partition_request:
                      partition_index: "-10:"
                  metric_configuration:
                    metric_name: column.min
                    metric_domain_kwargs: $domain.domain_kwargs # QUESTION: Should this be optional and inferred?
                  p_values: # TBD Not yet implemented
                    min_value: $variables.false_positive_threshold
                    max_value: 1.0
                - parameter_name: my_parameter_quantile_ranges
                  class_name: MultiBatchBootstrappedMetricDistributionParameterBuilder
                  batch_request:
                    partition_request:
                      partition_index: "-10:"
                  metric_configuration:
                    metric_name: column.quantile_values
                    metric_domain_kwargs: $domain.domain_kwargs # QUESTION: Should this be optional and inferred?
                    metric_value_kwargs:
                      quantiles:
                        - 0.05
                        - 0.25
                        - 0.50
                        - 0.75
                        - 0.95
                  p_values:
                    min_value: ($false_positive_threshold / 2)
                    max_value: 1 - ($false_positive_threshold / 2)
                - parameter_name: my_parameter_max
                  class_name: MetricParameterBuilder
                  metric_name: column.max
                  metric_domain_kwargs: $domain.domain_kwargs # QUESTION: Should this be optional and inferred?
                - id: my_parameter_quantiles
                  class_name: MetricParameterBuilder
                  metric_name: column.quantile_values
                  metric_domain_kwargs: $domain.domain_kwargs # QUESTION: Should this be optional and inferred?
                  metric_value_kwargs:
                    quantiles:
                      - 0.05
                      - 0.25
                      - 0.50
                      - 0.75
                      - 0.95
              configuration_builders:
                - expectation: expect_column_min_to_be_between
                  min_value: $my_parameter_false_positive_threshold_min.min_value
                  max_value: $my_parameter_false_positive_threshold_min.max_value
                - expectation: expect_column_quantile_values_to_be_between
                  quantiles:
                    - 0.05
                    - 0.25
                    - 0.50
                    - 0.75
                    - 0.95
                  value_ranges: $my_parameter_quantile_ranges
                - expectation: expect_column_max_to_be_between
                  min_value: 0.9 * $my_parameter_max
                  max_value: 1.1 * $my_parameter_max
                - expectation: expect_column_median_to_be_between
                  min_value:  # in this example, the resulting configuration is not a value, but an evaluation parameter referring to a metric that must be calculated for each validation to validation of the configuration (QUESTION: can this be performed by a parameter definition relative to the current batch instead of Evaluation Parameter?)
                    "$PARAMETER": 0.9 *
                       metric_name: column.median
                       metric_batch_kwargs:
                         batch_request:
                           partition_request:
                             partition_index: -1
                         column: $domain.domain_kwargs.column # QUESTION: Should this be optional and inferred?
                  max_value:  # in this example, the resulting configuration is not a value, but an evaluation parameter refering to a metric that must be calculated for each validation to validation of the configuration
                    "$PARAMETER": 1.1 *
                       metric_name: column.median
                       metric_batch_kwargs:
                         batch_request:
                           partition_request:
                             partition_index: -1
                         column: $domain.domain_kwargs.column # QUESTION: Should this be optional and inferred?

# NOTE: PLEASE SEE ABOVE FOR UPDATED CODE AS OF 20210423 ------------------------------------------------------

        Here are a few definitions of concepts that may not yet be familiar:

          - Semantic Type - you can use these to describe information that the data represents. For example an ``user_id`` column may have an ``integer`` type but you may also associate it with an ``id`` semantic type which conveys additional meaning e.g. must be unique, increasing, non-null, no gaps. In our Profiler concepts, this can be described using a ProfilerRule using a Domain that captures the columns of interest and ExpectationConfigurationBuilders that describe the semantic type (e.g. unique, non-null, etc).



.. discourse::
    :topic_identifier: 634
