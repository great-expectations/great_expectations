.. _migrating_versions:

###################################
Migrating between versions
###################################

While we are committed to keeping Great Expectations as stable as possible,
sometimes breaking changes are necessary to maintain our trajectory. This is
especially true as the library has evolved from just a data quality tool to a
more capable framework including data docs and profiling in addition to validation.

Great Expectations provides a warning when the currently-installed version is
different from the version stored in the expectation suite.

Since expectation semantics are usually consistent across versions, there is
little change required when upgrading great expectations, with some exceptions
noted here.

***********************************************
How to use the project ``check-config`` command
***********************************************

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

            To facilitate this substantial config format change, starting with version 0.8.0
            we introduced ``project check-config`` to sanity check your config files. From your
            project directory, run:

            .. code-block:: bash

                great_expectations project check-config

            This can be used at any time and will grow more robust and helpful as our
            internal config typing system improves.

            You will most likely be prompted to install a new template. Rest assured that
            your original yaml file will be archived automatically for you. Even so, it's
            in your source control system already, right? ;-)

            .. _v3_vs_v2_api_v2:

            **V3 (Batch Request) API vs The V2 (Batch Kwargs) API**

            The 0.13 major release of Great Expectations introduced a group of new features based on “new style” Datasources and Modular Expectations that we call the V3 (Batch Request) API. The V2 (Batch Kwargs) API will be deprecated in the future.

            0.13.x releases are compatible with both versions of the API. V3 API is currently marked as experimental.

            We are actively working on incorporating user feedback, documenting the new V3 API, and making the CLI work with it.

            Here are our current recommendations for choosing between V2 and V3 APIs:

            * Always install the latest 0.13.x release in order to keep up to date with various enhancements and bug fixes.

            * If you start a new project, use V3 API.

            * Keep using V2 API for your existing projects.


            We will announce when we have documentation/procedure for migrating existing projects from using V2 API to using V3 API.

            .. _upgrading_to_0.13:

            *************************
            Upgrading to 0.13.x
            *************************

            The 0.13.8 release introduces a formal `CheckpointStore`, which is a type of a `ConfigurationStore` that allows any of the supported `StoreBackend` alternatives to be specified for the various configurable components of Great Expectations.  With `CheckpointStore`, developers can save their `Checkpoint` configuration on the local filesystem or in various cloud storage services.

            The migration of Great Expectations from 0.12.x to 0.13.8 is seamless.  Simply execute:

            .. code-block:: bash

                great_expectations project upgrade


            on the command line, and if you created any checkpoints in the previous versions, they will become managed under the auspices of the `CheckpointStore` with its `StoreBackend` pointing to the same `checkpoints` directory in your Great Expectations installation directory as was configured prior to the upgrade.


            .. _upgrading_to_0.12:

            *************************
            Upgrading to 0.12.x
            *************************

            The 0.12.0 release makes a small but breaking change to the ``add_expectation``, ``remove_expectation``, and ``find_expectations`` methods. To update your code, replace the ``expectation_type``, ``column``, or ``kwargs`` arguments with an Expectation Configuration object. For more information on the ``match_type`` parameter, see :ref:`expectation_suite_operations`.

            For example, using the old API:

            .. code-block:: python

                remove_expectation(expectation_type="expect_column_values_to_be_in_set", column="city", expectation_kwargs={"value_set": ["New York","London","Tokyo"]})


            Using the new API:

            .. code-block:: python

                remove_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_be_in_set", column="city", expectation_kwargs={"column": "city", "value_set": ["New York","London","Tokyo"]}), match_type="success")


            .. _upgrading_to_0.11:

            *************************
            Upgrading to 0.11.x
            *************************

            The 0.11.0 release has several breaking changes related to ``run_id`` and ``ValidationMetric`` objects.
            Existing projects that have Expectation Suite Validation Results or configured evaluation parameter stores with
            DatabaseStoreBackend backends must be migrated.

            In addition, ``ValidationOperator.run`` now returns an instance of new type, ``ValidationOperatorResult`` (instead of a
            dictionary). If your code uses output from Validation Operators, it must be updated.

            run_id and ValidationMetric Changes
            ===================================

            ``run_id`` is now typed using the new ``RunIdentifier`` class, with optional ``run_name`` and ``run_time`` instantiation
            arguments. The ``run_name`` can be any string (this could come from your pipeline runner, e.g. Airflow run id). The ``run_time``
            can be either a dateutil parsable string or a datetime object. Note - any provided datetime will be assumed to be a UTC time.
            If no instantiation arguments are provided, ``run_name`` will be ``None`` (and appear as "__none__" in stores) and ``run_time``
            will default to the current UTC datetime. This change affects all Great Expectations classes that have a ``run_id`` attribute
            as well as any functions or methods that accept a ``run_id`` argument.

            ``data_asset_name`` (if available) is now added to ``batch_kwargs`` by ``batch_kwargs_generators``.
            Because of this newly exposed key in ``batch_kwargs``, ``ValidationMetric`` and associated ``ValidationMetricIdentifier``
            objects now have a ``data_asset_name`` attribute.

            The affected classes that are relevant to existing projects are ``ValidationResultIdentifier`` and
            ``ValidationMetricIdentifier``, as well as any configured stores that rely on these classes for keys, namely
            stores of type ``ValidationsStore`` (and subclasses) or ``EvaluationParameterStore`` (and other subclasses of
            ``MetricStore``). In addition, because Expectation Suite Validation Result json objects have a ``run_id`` key,
            existing validation result json files must be updated with a new typed ``run_id``.

            Migrating Your 0.10.x Project
            ==============================

            Before performing any of the following migration steps, please make sure you have appropriate backups of your project.

            Great Expectations has a CLI Upgrade Helper that helps automate all or most of the migration process (affected
            stores with database backends will still have to be migrated manually). The CLI tool makes use of a new class called
            UpgradeHelperV11. For reference, the UpgradeHelperV11 class is located at ``great_expectations.cli.upgrade_helpers.upgrade_helper_v11``.

            To use the CLI Upgrade Helper, enter the following command: ``great_expectations project upgrade``

            The Upgrade Helper will check your project and guide you through the upgrade process.

            .. note:: The following instructions detail the steps required to upgrade your project manually. The migration steps
              are written in the order they should be completed. They are also provided in the event that the Upgrade Helper is unable
              to complete a fully automated upgrade and some user intervention is required.

            0. Code That Uses Great Expectations
            -------------------------------------

            If you are using any Great Expectations methods that accept a ``run_id`` argument, you should update your code to pass in
            the new ``RunIdentifier`` type (or a dictionary with ``run_name`` and ``run_time`` keys). For now, methods with a
            ``run_id`` parameter will continue to accept strings. In this case, the provided ``run_id`` string will be converted to
            a ``RunIdentifier`` object, acting as the ``run_name``. If the ``run_id`` string can also be parsed as a datetime, it
            will also be used for the ``run_time`` attribute, otherwise, the current UTC time is used. All times are assumed to be
            UTC times.

            If your code uses output from Validation Operators, it must be updated to handle the new ValidationOperatorResult
            type.

            1. Expectation Suite Validation Result JSONs
            --------------------------------------------

            Each existing Expectation Suite Validation Result JSON in your project should be updated with a typed ``run_id``. The ``run_id``
            key is found under the top-level ``meta`` key. You can use the current ``run_id`` string as the new ``run_name``
            (or select a different one). If the current ``run_id`` is already a datetime string, you can also use it for the ``run_time``
            as well, otherwise, we suggest using the last modified datetime of the validation result.

            .. note:: Subsequent migration steps will make use of this new ``run_time`` when generating new paths/keys for validation
              result jsons and their Data Docs html pages. Please ensure the ``run_time`` in these paths/keys match the ``run_time``
              in the corresponding validation result. Similarly, if you decide to use a different value for ``run_name`` instead of
              reusing an existing ``run_id`` string, make sure this is reflected in the new paths/keys.

            For example, an existing validation result json with ``run_id="my_run"`` should be updated to look like the following::

              {
              "meta": {
                "great_expectations_version": "0.10.8",
                "expectation_suite_name": "diabetic_data.warning",
                "run_id": {
                  "run_name": "my_run",
                  "run_time": "20200507T065044.404158Z"
                },
                ...
              },
              ...
              }

            2. Stores and their Backends
            ------------------------------

            Stores rely on special identifier classes to serve as keys when getting or setting values. When the signature of an
            identifier class changes, any existing stores that rely on that identifier must be updated. Specifically, the structure
            of that store's backend must be modified to conform to the new identifier signature.

            For example, in a v0.10.x project, you might have an Expectation Suite Validation Result with the following
            ``ValidationResultIdentifier``::

              v10_identifier = ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
                run_id="my_string_run_id",
                batch_identifier="some_batch_identifier"
              )

            A configured ``ValidationsStore`` with a ``TupleFilesystemStoreBackend`` (and default config) would use this identifier
            to generate the following filepath for writing the validation result to a file (and retrieving it at a later time)::

              v10_filepath = "great_expectations/uncommitted/validations/my_suite_name/my_string_run_id/some_batch_identifier.json"

            In a v0.11.x project, the ``ValidationResultIdentifier`` and corresponding filepath would look like the following::

              v11_identifier = ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
                run_id=RunIdentifier(run_name="my_string_run_name", run_time="2020-05-08T20:51:18.077262"),
                batch_identifier="some_batch_identifier"
              )
              v11_filepath = "great_expectations/uncommitted/validations/my_suite_name/my_string_run_name/2020-05-08T20:51:18.077262/some_batch_identifier.json"

            When migrating to v0.11.x, you would have to move all existing validation results to new filepaths. For a particular
            validation result, you might move the file like this::

              os.makedirs(v11_filepath, exist_ok=True)  # create missing directories from v11 filepath
              shutil.move(v10_filepath, v11_filepath)  # move validation result json file

            The following sections detail the changes you must make to existing store backends.

            **2a. Validations Store Backends**

            For validations stores with backends of type ``TupleFilesystemStoreBackend``, ``TupleS3StoreBackend``, or ``TupleGCSStoreBackend``,
            rename paths (or object keys) of all existing Expectation Suite Validation Result json files:

            Before::

              great_expectations/uncommitted/validations/my_suite_name/my_run_id/some_batch_identifier.json

            After::

              great_expectations/uncommitted/validations/my_suite_name/my_run_id/my_run_time/batch_identifier.json

            For validations stores with backends of type ``DatabaseStoreBackend``, perform the following database migration:

            * add string column with name ``run_name``; copy values from ``run_id`` column
            * add string column with name ``run_time``; fill with appropriate dateutil parsable values
            * delete ``run_id`` column

            **2b. Evaluation Parameter Store Backends**

            If you have any configured evaluation parameter stores that use a ``DatabaseStoreBackend`` backend, you must perform the
            following migration for each database backend:

            * add string column with name ``data_asset_name``; fill with appropriate values or use "__none__"
            * add string column with name ``run_name``; copy values from ``run_id`` column
            * add string column with name ``run_time``; fill with appropriate dateutil parsable values
            * delete ``run_id`` column

            **2c. Data Docs Validations Store Backends**

            .. note:: If you are okay with rebuilding your Data Docs sites, you can skip the migration steps in this section. Instead,
              you should should run the following CLI command, but **only after** you have completed the above migration steps:
              ``great_expectations docs clean --all && great_expectations docs build``.

            For Data Docs sites with store backends of type ``TupleFilesystemStoreBackend``, ``TupleS3StoreBackend``, or ``TupleGCSStoreBackend``, rename
            paths (or object keys) of all existing Expectation Suite Validation Result html files:

            Before::

              great_expectations/uncommitted/data_docs/my_site_name/validations/my_suite_name/my_run_id/some_batch_identifier.html

            After::

              great_expectations/uncommitted/data_docs/my_site_name/validations/my_suite_name/my_run_id/my_run_time/batch_identifier.html

            .. _upgrading_to_0.10.x:

            ************************
            How to upgrade to 0.10.x
            ************************

            In the 0.10.0 release, there are several breaking changes to the DataContext API.

            Most are related to the clarified naming ``BatchKwargsGenerators``.

            So, if you are using methods on the data context that used to have an argument named ``generators``,
            you will need to update that code to use the more precise name ``batch_kwargs_generators``.

            For example, in the method ``DataContext.get_available_data_asset_names`` the parameter ``generator_names`` is now ``batch_kwargs_generator_names``.

            If you are using ``BatchKwargsGenerators`` in your project config, follow these steps to upgrade your existing Great Expectations project:
            * Edit your ``great_expectations.yml`` file and change the key ``generators`` to ``batch_kwargs_generators``.

            * Run a simple command such as: ``great_expectations datasource list`` and ensure you see a list of datasources.


            ***********************
            How to upgrade to 0.9.x
            ***********************

            In the 0.9.0 release, there are several changes to the DataContext API.


            Follow these steps to upgrade your existing Great Expectations project:

            * In the terminal navigate to the parent of the ``great_expectations`` directory of your project.

            * Run this command:

            .. code-block:: bash

                great_expectations project check-config

            * For every item that needs to be renamed the command will display a message that looks like this: ``The class name 'X' has changed to 'Y'``. Replace all occurrences of X with Y in your project's ``great_expectations.yml`` config file.

            * After saving the config file, rerun the check-config command.

            * Depending on your configuration, you will see 3-6 of these messages.

            * The command will display this message when done: ``Your config file appears valid!``.

            * Rename your Expectation Suites to make them compatible with the new naming. Save this Python code snippet in a file called ``update_project.py``, then run it using the command: ``python update_project.py PATH_TO_GE_CONFIG_DIRECTORY``:

            .. code-block:: python

                #!/usr/bin/env python3
                import sys
                import os
                import json
                import uuid
                import shutil
                def update_validation_result_name(validation_result):
                    data_asset_name = validation_result["meta"].get("data_asset_name")
                    if data_asset_name is None:
                        print("    No data_asset_name in this validation result. Unable to update it.")
                        return
                    data_asset_name_parts = data_asset_name.split("/")
                    if len(data_asset_name_parts) != 3:
                        print("    data_asset_name in this validation result does not appear to be normalized. Unable to update it.")
                        return
                    expectation_suite_suffix = validation_result["meta"].get("expectation_suite_name")
                    if expectation_suite_suffix is None:
                        print("    No expectation_suite_name found in this validation result. Unable to update it.")
                        return
                    expectation_suite_name = ".".join(
                        data_asset_name_parts +
                        [expectation_suite_suffix]
                    )
                    validation_result["meta"]["expectation_suite_name"] = expectation_suite_name
                    try:
                        del validation_result["meta"]["data_asset_name"]
                    except KeyError:
                        pass
                def update_expectation_suite_name(expectation_suite):
                    data_asset_name = expectation_suite.get("data_asset_name")
                    if data_asset_name is None:
                        print("    No data_asset_name in this expectation suite. Unable to update it.")
                        return
                    data_asset_name_parts = data_asset_name.split("/")
                    if len(data_asset_name_parts) != 3:
                        print("    data_asset_name in this expectation suite does not appear to be normalized. Unable to update it.")
                        return
                    expectation_suite_suffix = expectation_suite.get("expectation_suite_name")
                    if expectation_suite_suffix is None:
                        print("    No expectation_suite_name found in this expectation suite. Unable to update it.")
                        return
                    expectation_suite_name = ".".join(
                        data_asset_name_parts +
                        [expectation_suite_suffix]
                    )
                    expectation_suite["expectation_suite_name"] = expectation_suite_name
                    try:
                        del expectation_suite["data_asset_name"]
                    except KeyError:
                        pass
                def update_context_dir(context_root_dir):
                    # Update expectation suite names in expectation suites
                    expectations_dir = os.path.join(context_root_dir, "expectations")
                    for subdir, dirs, files in os.walk(expectations_dir):
                        for file in files:
                            if file.endswith(".json"):
                                print("Migrating suite located at: " + str(os.path.join(subdir, file)))
                                with open(os.path.join(subdir, file), 'r') as suite_fp:
                                    suite = json.load(suite_fp)
                                update_expectation_suite_name(suite)
                                with open(os.path.join(subdir, file), 'w') as suite_fp:
                                    json.dump(suite, suite_fp)
                    # Update expectation suite names in validation results
                    validations_dir = os.path.join(context_root_dir, "uncommitted", "validations")
                    for subdir, dirs, files in os.walk(validations_dir):
                        for file in files:
                            if file.endswith(".json"):
                                print("Migrating validation_result located at: " + str(os.path.join(subdir, file)))
                                try:
                                    with open(os.path.join(subdir, file), 'r') as suite_fp:
                                        suite = json.load(suite_fp)
                                    update_validation_result_name(suite)
                                    with open(os.path.join(subdir, file), 'w') as suite_fp:
                                        json.dump(suite, suite_fp)
                                    try:
                                        run_id = suite["meta"].get("run_id")
                                        es_name = suite["meta"].get("expectation_suite_name").split(".")
                                        filename = "converted__" + str(uuid.uuid1()) + ".json"
                                        os.makedirs(os.path.join(
                                            context_root_dir, "uncommitted", "validations",
                                            *es_name, run_id
                                        ), exist_ok=True)
                                        shutil.move(os.path.join(subdir, file),
                                                    os.path.join(
                                                        context_root_dir, "uncommitted", "validations",
                                                        *es_name, run_id, filename
                                                    )
                                        )
                                    except OSError as e:
                                        print("    Unable to move validation result; file has been updated to new "
                                              "format but not moved to new store location.")
                                    except KeyError:
                                        pass  # error will have been generated above
                                except json.decoder.JSONDecodeError:
                                    print("    Unable to process file: error reading JSON.")
                if __name__ == "__main__":
                    if len(sys.argv) < 2:
                        print("Please provide a path to update.")
                        sys.exit(-1)
                    path = str(os.path.abspath(sys.argv[1]))
                    print("About to update context dir for path: " + path)
                    update_context_dir(path)

            * Rebuild Data Docs:

            .. code-block:: bash

                great_expectations docs build

            * This project has now been migrated to 0.9.0. Please see the list of changes below for more detailed information.


            CONFIGURATION CHANGES:

            - FixedLengthTupleXXXX stores are renamed to TupleXXXX stores; they no
              longer allow or require (or allow) a key_length to be specified, but they
              do allow `filepath_prefix` and/or `filepath_suffix` to be configured as an
              alternative to an the `filepath_template`.
            - ExtractAndStoreEvaluationParamsAction is renamed to
              StoreEvaluationParametersAction; a new StoreMetricsAction is available as
              well to allow DataContext-configured metrics to be saved.
            - The InMemoryEvaluationParameterStore is replaced with the
              EvaluationParameterStore; EvaluationParameterStore and MetricsStore can
              both be configured to use DatabaseStoreBackend instead of the
              InMemoryStoreBackend.
            - The `type` key can no longer be used in place of class_name in
              configuration. Use `class_name` instead.
            - BatchKwargsGenerators are more explicitly named; we avoid use of the term
              "Generator" because it is ambiguous. All existing BatchKwargsGenerators have
              been renamed by substituting "BatchKwargsGenerator" for "Generator"; for
              example GlobReaderGenerator is now GlobReaderBatchKwargsGenerator.
            - ReaderMethod is no longer an enum; it is a string of the actual method to
              be invoked (e.g. `read_csv` for pandas). That change makes it easy to
              specify arbitrary reader_methods via batch_kwargs (including read_pickle),
              BUT existing configurations using enum-based reader_method in batch_kwargs
              will need to update their code. For example, a pandas datasource would use
              `reader_method: read_csv`` instead of `reader_method: csv`

            CODE CHANGES:

            - DataAssetName and name normalization have been completely eliminated, which
              causes several related changes to code using the DataContext.

              - data_asset_name is **no longer** a parameter in the
                create_expectation_suite, get_expectation_suite, or get_batch commands;
                expectation suite names exist in an independent namespace.
              - batch_kwargs alone now define the batch to be received, and the
                datasource name **must** be included in batch_kwargs as the "datasource"
                key.
              - **A generator name is therefore no longer required to get data or define
                an expectation suite.**
              - The BatchKwargsGenerators API has been simplified; `build_batch_kwargs`
                should be the entrypoint for all cases of using a generator to get
                batch_kwargs, including when explicitly specifying a partition, limiting
                the number of returned rows, accessing saved kwargs, or using any other
                BatchKwargsGenerator feature. BatchKwargsGenerators *must* be attached to
                a specific datasource to be instantiated.
              - The API for validating data has changed.

            - **Database store tables are not compatible** between versions and require a
              manual migration; the new default table names are: `ge_validations_store`,
              `ge_expectations_store`, `ge_metrics`, and `ge_evaluation_parameters`. The
              Validations Store uses a three-part compound primary key consisting of
              run_id, expectation_suite_name, and batch_identifier; Expectations Store
              uses the expectation_suite_name as its only key. Both Metrics and
              Evaluation Parameters stores use `run_id`, `expectation_suite_name`,
              `metric_id`, and `metric_kwargs_id` to form a compound primary key.
            - The term "batch_fingerprint" is no longer used, and has been replaced with
              "batch_markers". It is a dictionary that, like batch_kwargs, can be used to
              construct an ID.
            - `get_data_asset_name` and `save_data_asset_name` are removed.
            - There are numerous under-the-scenes changes to the internal types used in
              GreatExpectations. These should be transparent to users.


            ***********************
            How to upgrade to 0.8.x
            ***********************

            In the 0.8.0 release, our DataContext config format has changed dramatically to
            enable new features including extensibility.

            Some specific changes:

            - New top-level keys:

              - `expectations_store_name`
              - `evaluation_parameter_store_name`
              - `validations_store_name`

            - Deprecation of the `type` key for configuring objects (replaced by
              `class_name` (and `module_name` as well when ambiguous).
            - Completely new `SiteBuilder` configuration.

            BREAKING:
             - **top-level `validate` has a new signature**, that offers a variety of different options for specifying the DataAsset
               class to use during validation, including `data_asset_class_name` / `data_asset_module_name` or `data_asset_class`
             - Internal class name changes between alpha versions:
               - InMemoryEvaluationParameterStore
               - ValidationsStore
               - ExpectationsStore
               - ActionListValidationOperator
             - Several modules are now refactored into different names including all datasources
             - InMemoryBatchKwargs use the key dataset instead of df to be more explicit


            Pre-0.8.x configuration files ``great_expectations.yml`` are not compatible with 0.8.x. Run ``great_expectations project check-config`` - it will offer to create a new config file. The new config file will not have any customizations you made, so you will have to copy these from the old file.

            If you run into any issues, please ask for help on `Slack <https://greatexpectations.io/slack>`__.

            ***********************
            How to upgrade to 0.7.x
            ***********************

            In version 0.7, GE introduced several new features, and significantly changed the way DataContext objects work:

             - A :ref:`data_context` object manages access to expectation suites and other configuration in addition to data assets.
               It provides a flexible but opinionated structure for creating and storing configuration and expectations in version
               control.

             - When upgrading from prior versions, the new :ref:`datasource` objects provide the same functionality that compute-
               environment-specific data context objects provided before, but with significantly more flexibility.

             - The term "autoinspect" is no longer used directly, having been replaced by a much more flexible :ref:`profiling`
               feature.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

            To facilitate this substantial config format change, starting with version 0.8.0
            we introduced ``project check-config`` to sanity check your config files. From your
            project directory, run:

            .. code-block:: bash

                great_expectations --v3-api project check-config

            This can be used at any time and will grow more robust and helpful as our
            internal config typing system improves.

            You will most likely be prompted to install a new template. Rest assured that
            your original yaml file will be archived automatically for you. Even so, it's
            in your source control system already, right? ;-)

            .. _v3_vs_v2_api_v3:

            **V3 (Batch Request) API vs The V2 (Batch Kwargs) API**

            The 0.13 major release of Great Expectations introduced a group of new features based on “new style” Datasources and Modular Expectations that we call the V3 (Batch Request) API. The V2 (Batch Kwargs) API will be deprecated in the future.

            0.13.x releases are compatible with both versions of the API. V3 API is currently marked as experimental.

            We are actively working on incorporating user feedback, documenting the new V3 API, and making the CLI work with it.

            Here are our current recommendations for choosing between V2 and V3 APIs:

            * Always install the latest 0.13.x release in order to keep up to date with various enhancements and bug fixes.

            * If you start a new project, use V3 API.

            * Keep using V2 API for your existing projects.


            We will announce when we have documentation/procedure for migrating existing projects from using V2 API to using V3 API.

            .. _upgrading_to_0.13:

            *************************
            Upgrading to 0.13.x
            *************************

            The 0.13.8 release introduces a formal `CheckpointStore`, which is a type of a `ConfigurationStore` that allows any of the supported `StoreBackend` alternatives to be specified for the various configurable components of Great Expectations.  With `CheckpointStore`, developers can save their `Checkpoint` configuration on the local filesystem or in various cloud storage services.

            The migration of Great Expectations from 0.12.x to 0.13.8 is seamless.  Simply execute:

            .. code-block:: bash

                great_expectations --v3-api project upgrade


            on the command line, and if you created any checkpoints in the previous versions, they will become managed under the auspices of the `CheckpointStore` with its `StoreBackend` pointing to the same `checkpoints` directory in your Great Expectations installation directory as was configured prior to the upgrade.


            .. _upgrading_to_0.12:

            *************************
            Upgrading to 0.12.x
            *************************

            The 0.12.0 release makes a small but breaking change to the ``add_expectation``, ``remove_expectation``, and ``find_expectations`` methods. To update your code, replace the ``expectation_type``, ``column``, or ``kwargs`` arguments with an Expectation Configuration object. For more information on the ``match_type`` parameter, see :ref:`expectation_suite_operations`.

            For example, using the old API:

            .. code-block:: python

                remove_expectation(expectation_type="expect_column_values_to_be_in_set", column="city", expectation_kwargs={"value_set": ["New York","London","Tokyo"]})


            Using the new API:

            .. code-block:: python

                remove_expectation(ExpectationConfiguration(expectation_type="expect_column_values_to_be_in_set", column="city", expectation_kwargs={"column": "city", "value_set": ["New York","London","Tokyo"]}), match_type="success")


            .. _upgrading_to_0.11:

            *************************
            Upgrading to 0.11.x
            *************************

            The 0.11.0 release has several breaking changes related to ``run_id`` and ``ValidationMetric`` objects.
            Existing projects that have Expectation Suite Validation Results or configured evaluation parameter stores with
            DatabaseStoreBackend backends must be migrated.

            In addition, ``ValidationOperator.run`` now returns an instance of new type, ``ValidationOperatorResult`` (instead of a
            dictionary). If your code uses output from Validation Operators, it must be updated.

            run_id and ValidationMetric Changes
            ===================================

            ``run_id`` is now typed using the new ``RunIdentifier`` class, with optional ``run_name`` and ``run_time`` instantiation
            arguments. The ``run_name`` can be any string (this could come from your pipeline runner, e.g. Airflow run id). The ``run_time``
            can be either a dateutil parsable string or a datetime object. Note - any provided datetime will be assumed to be a UTC time.
            If no instantiation arguments are provided, ``run_name`` will be ``None`` (and appear as "__none__" in stores) and ``run_time``
            will default to the current UTC datetime. This change affects all Great Expectations classes that have a ``run_id`` attribute
            as well as any functions or methods that accept a ``run_id`` argument.

            ``data_asset_name`` (if available) is now added to ``batch_kwargs`` by ``batch_kwargs_generators``.
            Because of this newly exposed key in ``batch_kwargs``, ``ValidationMetric`` and associated ``ValidationMetricIdentifier``
            objects now have a ``data_asset_name`` attribute.

            The affected classes that are relevant to existing projects are ``ValidationResultIdentifier`` and
            ``ValidationMetricIdentifier``, as well as any configured stores that rely on these classes for keys, namely
            stores of type ``ValidationsStore`` (and subclasses) or ``EvaluationParameterStore`` (and other subclasses of
            ``MetricStore``). In addition, because Expectation Suite Validation Result json objects have a ``run_id`` key,
            existing validation result json files must be updated with a new typed ``run_id``.

            Migrating Your 0.10.x Project
            ==============================

            Before performing any of the following migration steps, please make sure you have appropriate backups of your project.

            Great Expectations has a CLI Upgrade Helper that helps automate all or most of the migration process (affected
            stores with database backends will still have to be migrated manually). The CLI tool makes use of a new class called
            UpgradeHelperV11. For reference, the UpgradeHelperV11 class is located at ``great_expectations.cli.upgrade_helpers.upgrade_helper_v11``.

            To use the CLI Upgrade Helper, enter the following command: ``great_expectations --v3-api project upgrade``

            The Upgrade Helper will check your project and guide you through the upgrade process.

            .. note:: The following instructions detail the steps required to upgrade your project manually. The migration steps
              are written in the order they should be completed. They are also provided in the event that the Upgrade Helper is unable
              to complete a fully automated upgrade and some user intervention is required.

            0. Code That Uses Great Expectations
            -------------------------------------

            If you are using any Great Expectations methods that accept a ``run_id`` argument, you should update your code to pass in
            the new ``RunIdentifier`` type (or a dictionary with ``run_name`` and ``run_time`` keys). For now, methods with a
            ``run_id`` parameter will continue to accept strings. In this case, the provided ``run_id`` string will be converted to
            a ``RunIdentifier`` object, acting as the ``run_name``. If the ``run_id`` string can also be parsed as a datetime, it
            will also be used for the ``run_time`` attribute, otherwise, the current UTC time is used. All times are assumed to be
            UTC times.

            If your code uses output from Validation Operators, it must be updated to handle the new ValidationOperatorResult
            type.

            1. Expectation Suite Validation Result JSONs
            --------------------------------------------

            Each existing Expectation Suite Validation Result JSON in your project should be updated with a typed ``run_id``. The ``run_id``
            key is found under the top-level ``meta`` key. You can use the current ``run_id`` string as the new ``run_name``
            (or select a different one). If the current ``run_id`` is already a datetime string, you can also use it for the ``run_time``
            as well, otherwise, we suggest using the last modified datetime of the validation result.

            .. note:: Subsequent migration steps will make use of this new ``run_time`` when generating new paths/keys for validation
              result jsons and their Data Docs html pages. Please ensure the ``run_time`` in these paths/keys match the ``run_time``
              in the corresponding validation result. Similarly, if you decide to use a different value for ``run_name`` instead of
              reusing an existing ``run_id`` string, make sure this is reflected in the new paths/keys.

            For example, an existing validation result json with ``run_id="my_run"`` should be updated to look like the following::

              {
              "meta": {
                "great_expectations_version": "0.10.8",
                "expectation_suite_name": "diabetic_data.warning",
                "run_id": {
                  "run_name": "my_run",
                  "run_time": "20200507T065044.404158Z"
                },
                ...
              },
              ...
              }

            2. Stores and their Backends
            ------------------------------

            Stores rely on special identifier classes to serve as keys when getting or setting values. When the signature of an
            identifier class changes, any existing stores that rely on that identifier must be updated. Specifically, the structure
            of that store's backend must be modified to conform to the new identifier signature.

            For example, in a v0.10.x project, you might have an Expectation Suite Validation Result with the following
            ``ValidationResultIdentifier``::

              v10_identifier = ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
                run_id="my_string_run_id",
                batch_identifier="some_batch_identifier"
              )

            A configured ``ValidationsStore`` with a ``TupleFilesystemStoreBackend`` (and default config) would use this identifier
            to generate the following filepath for writing the validation result to a file (and retrieving it at a later time)::

              v10_filepath = "great_expectations/uncommitted/validations/my_suite_name/my_string_run_id/some_batch_identifier.json"

            In a v0.11.x project, the ``ValidationResultIdentifier`` and corresponding filepath would look like the following::

              v11_identifier = ValidationResultIdentifier(
                expectation_suite_identifier=ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
                run_id=RunIdentifier(run_name="my_string_run_name", run_time="2020-05-08T20:51:18.077262"),
                batch_identifier="some_batch_identifier"
              )
              v11_filepath = "great_expectations/uncommitted/validations/my_suite_name/my_string_run_name/2020-05-08T20:51:18.077262/some_batch_identifier.json"

            When migrating to v0.11.x, you would have to move all existing validation results to new filepaths. For a particular
            validation result, you might move the file like this::

              os.makedirs(v11_filepath, exist_ok=True)  # create missing directories from v11 filepath
              shutil.move(v10_filepath, v11_filepath)  # move validation result json file

            The following sections detail the changes you must make to existing store backends.

            **2a. Validations Store Backends**

            For validations stores with backends of type ``TupleFilesystemStoreBackend``, ``TupleS3StoreBackend``, or ``TupleGCSStoreBackend``,
            rename paths (or object keys) of all existing Expectation Suite Validation Result json files:

            Before::

              great_expectations/uncommitted/validations/my_suite_name/my_run_id/some_batch_identifier.json

            After::

              great_expectations/uncommitted/validations/my_suite_name/my_run_id/my_run_time/batch_identifier.json

            For validations stores with backends of type ``DatabaseStoreBackend``, perform the following database migration:

            * add string column with name ``run_name``; copy values from ``run_id`` column
            * add string column with name ``run_time``; fill with appropriate dateutil parsable values
            * delete ``run_id`` column

            **2b. Evaluation Parameter Store Backends**

            If you have any configured evaluation parameter stores that use a ``DatabaseStoreBackend`` backend, you must perform the
            following migration for each database backend:

            * add string column with name ``data_asset_name``; fill with appropriate values or use "__none__"
            * add string column with name ``run_name``; copy values from ``run_id`` column
            * add string column with name ``run_time``; fill with appropriate dateutil parsable values
            * delete ``run_id`` column

            **2c. Data Docs Validations Store Backends**

            .. note:: If you are okay with rebuilding your Data Docs sites, you can skip the migration steps in this section. Instead,
              you should should run the following CLI command, but **only after** you have completed the above migration steps:
              ``great_expectations docs clean --all && great_expectations docs build``.

            For Data Docs sites with store backends of type ``TupleFilesystemStoreBackend``, ``TupleS3StoreBackend``, or ``TupleGCSStoreBackend``, rename
            paths (or object keys) of all existing Expectation Suite Validation Result html files:

            Before::

              great_expectations/uncommitted/data_docs/my_site_name/validations/my_suite_name/my_run_id/some_batch_identifier.html

            After::

              great_expectations/uncommitted/data_docs/my_site_name/validations/my_suite_name/my_run_id/my_run_time/batch_identifier.html

            .. _upgrading_to_0.10.x:

            ************************
            How to upgrade to 0.10.x
            ************************

            In the 0.10.0 release, there are several breaking changes to the DataContext API.

            Most are related to the clarified naming ``BatchKwargsGenerators``.

            So, if you are using methods on the data context that used to have an argument named ``generators``,
            you will need to update that code to use the more precise name ``batch_kwargs_generators``.

            For example, in the method ``DataContext.get_available_data_asset_names`` the parameter ``generator_names`` is now ``batch_kwargs_generator_names``.

            If you are using ``BatchKwargsGenerators`` in your project config, follow these steps to upgrade your existing Great Expectations project:
            * Edit your ``great_expectations.yml`` file and change the key ``generators`` to ``batch_kwargs_generators``.

            * Run a simple command such as: ``great_expectations datasource list`` and ensure you see a list of datasources.


            ***********************
            How to upgrade to 0.9.x
            ***********************

            In the 0.9.0 release, there are several changes to the DataContext API.


            Follow these steps to upgrade your existing Great Expectations project:

            * In the terminal navigate to the parent of the ``great_expectations`` directory of your project.

            * Run this command:

            .. code-block:: bash

                great_expectations --v3-api project check-config

            * For every item that needs to be renamed the command will display a message that looks like this: ``The class name 'X' has changed to 'Y'``. Replace all occurrences of X with Y in your project's ``great_expectations.yml`` config file.

            * After saving the config file, rerun the check-config command.

            * Depending on your configuration, you will see 3-6 of these messages.

            * The command will display this message when done: ``Your config file appears valid!``.

            * Rename your Expectation Suites to make them compatible with the new naming. Save this Python code snippet in a file called ``update_project.py``, then run it using the command: ``python update_project.py PATH_TO_GE_CONFIG_DIRECTORY``:

            .. code-block:: python

                #!/usr/bin/env python3
                import sys
                import os
                import json
                import uuid
                import shutil
                def update_validation_result_name(validation_result):
                    data_asset_name = validation_result["meta"].get("data_asset_name")
                    if data_asset_name is None:
                        print("    No data_asset_name in this validation result. Unable to update it.")
                        return
                    data_asset_name_parts = data_asset_name.split("/")
                    if len(data_asset_name_parts) != 3:
                        print("    data_asset_name in this validation result does not appear to be normalized. Unable to update it.")
                        return
                    expectation_suite_suffix = validation_result["meta"].get("expectation_suite_name")
                    if expectation_suite_suffix is None:
                        print("    No expectation_suite_name found in this validation result. Unable to update it.")
                        return
                    expectation_suite_name = ".".join(
                        data_asset_name_parts +
                        [expectation_suite_suffix]
                    )
                    validation_result["meta"]["expectation_suite_name"] = expectation_suite_name
                    try:
                        del validation_result["meta"]["data_asset_name"]
                    except KeyError:
                        pass
                def update_expectation_suite_name(expectation_suite):
                    data_asset_name = expectation_suite.get("data_asset_name")
                    if data_asset_name is None:
                        print("    No data_asset_name in this expectation suite. Unable to update it.")
                        return
                    data_asset_name_parts = data_asset_name.split("/")
                    if len(data_asset_name_parts) != 3:
                        print("    data_asset_name in this expectation suite does not appear to be normalized. Unable to update it.")
                        return
                    expectation_suite_suffix = expectation_suite.get("expectation_suite_name")
                    if expectation_suite_suffix is None:
                        print("    No expectation_suite_name found in this expectation suite. Unable to update it.")
                        return
                    expectation_suite_name = ".".join(
                        data_asset_name_parts +
                        [expectation_suite_suffix]
                    )
                    expectation_suite["expectation_suite_name"] = expectation_suite_name
                    try:
                        del expectation_suite["data_asset_name"]
                    except KeyError:
                        pass
                def update_context_dir(context_root_dir):
                    # Update expectation suite names in expectation suites
                    expectations_dir = os.path.join(context_root_dir, "expectations")
                    for subdir, dirs, files in os.walk(expectations_dir):
                        for file in files:
                            if file.endswith(".json"):
                                print("Migrating suite located at: " + str(os.path.join(subdir, file)))
                                with open(os.path.join(subdir, file), 'r') as suite_fp:
                                    suite = json.load(suite_fp)
                                update_expectation_suite_name(suite)
                                with open(os.path.join(subdir, file), 'w') as suite_fp:
                                    json.dump(suite, suite_fp)
                    # Update expectation suite names in validation results
                    validations_dir = os.path.join(context_root_dir, "uncommitted", "validations")
                    for subdir, dirs, files in os.walk(validations_dir):
                        for file in files:
                            if file.endswith(".json"):
                                print("Migrating validation_result located at: " + str(os.path.join(subdir, file)))
                                try:
                                    with open(os.path.join(subdir, file), 'r') as suite_fp:
                                        suite = json.load(suite_fp)
                                    update_validation_result_name(suite)
                                    with open(os.path.join(subdir, file), 'w') as suite_fp:
                                        json.dump(suite, suite_fp)
                                    try:
                                        run_id = suite["meta"].get("run_id")
                                        es_name = suite["meta"].get("expectation_suite_name").split(".")
                                        filename = "converted__" + str(uuid.uuid1()) + ".json"
                                        os.makedirs(os.path.join(
                                            context_root_dir, "uncommitted", "validations",
                                            *es_name, run_id
                                        ), exist_ok=True)
                                        shutil.move(os.path.join(subdir, file),
                                                    os.path.join(
                                                        context_root_dir, "uncommitted", "validations",
                                                        *es_name, run_id, filename
                                                    )
                                        )
                                    except OSError as e:
                                        print("    Unable to move validation result; file has been updated to new "
                                              "format but not moved to new store location.")
                                    except KeyError:
                                        pass  # error will have been generated above
                                except json.decoder.JSONDecodeError:
                                    print("    Unable to process file: error reading JSON.")
                if __name__ == "__main__":
                    if len(sys.argv) < 2:
                        print("Please provide a path to update.")
                        sys.exit(-1)
                    path = str(os.path.abspath(sys.argv[1]))
                    print("About to update context dir for path: " + path)
                    update_context_dir(path)

            * Rebuild Data Docs:

            .. code-block:: bash

                great_expectations docs build

            * This project has now been migrated to 0.9.0. Please see the list of changes below for more detailed information.


            CONFIGURATION CHANGES:

            - FixedLengthTupleXXXX stores are renamed to TupleXXXX stores; they no
              longer allow or require (or allow) a key_length to be specified, but they
              do allow `filepath_prefix` and/or `filepath_suffix` to be configured as an
              alternative to an the `filepath_template`.
            - ExtractAndStoreEvaluationParamsAction is renamed to
              StoreEvaluationParametersAction; a new StoreMetricsAction is available as
              well to allow DataContext-configured metrics to be saved.
            - The InMemoryEvaluationParameterStore is replaced with the
              EvaluationParameterStore; EvaluationParameterStore and MetricsStore can
              both be configured to use DatabaseStoreBackend instead of the
              InMemoryStoreBackend.
            - The `type` key can no longer be used in place of class_name in
              configuration. Use `class_name` instead.
            - BatchKwargsGenerators are more explicitly named; we avoid use of the term
              "Generator" because it is ambiguous. All existing BatchKwargsGenerators have
              been renamed by substituting "BatchKwargsGenerator" for "Generator"; for
              example GlobReaderGenerator is now GlobReaderBatchKwargsGenerator.
            - ReaderMethod is no longer an enum; it is a string of the actual method to
              be invoked (e.g. `read_csv` for pandas). That change makes it easy to
              specify arbitrary reader_methods via batch_kwargs (including read_pickle),
              BUT existing configurations using enum-based reader_method in batch_kwargs
              will need to update their code. For example, a pandas datasource would use
              `reader_method: read_csv`` instead of `reader_method: csv`

            CODE CHANGES:

            - DataAssetName and name normalization have been completely eliminated, which
              causes several related changes to code using the DataContext.

              - data_asset_name is **no longer** a parameter in the
                create_expectation_suite, get_expectation_suite, or get_batch commands;
                expectation suite names exist in an independent namespace.
              - batch_kwargs alone now define the batch to be received, and the
                datasource name **must** be included in batch_kwargs as the "datasource"
                key.
              - **A generator name is therefore no longer required to get data or define
                an expectation suite.**
              - The BatchKwargsGenerators API has been simplified; `build_batch_kwargs`
                should be the entrypoint for all cases of using a generator to get
                batch_kwargs, including when explicitly specifying a partition, limiting
                the number of returned rows, accessing saved kwargs, or using any other
                BatchKwargsGenerator feature. BatchKwargsGenerators *must* be attached to
                a specific datasource to be instantiated.
              - The API for validating data has changed.

            - **Database store tables are not compatible** between versions and require a
              manual migration; the new default table names are: `ge_validations_store`,
              `ge_expectations_store`, `ge_metrics`, and `ge_evaluation_parameters`. The
              Validations Store uses a three-part compound primary key consisting of
              run_id, expectation_suite_name, and batch_identifier; Expectations Store
              uses the expectation_suite_name as its only key. Both Metrics and
              Evaluation Parameters stores use `run_id`, `expectation_suite_name`,
              `metric_id`, and `metric_kwargs_id` to form a compound primary key.
            - The term "batch_fingerprint" is no longer used, and has been replaced with
              "batch_markers". It is a dictionary that, like batch_kwargs, can be used to
              construct an ID.
            - `get_data_asset_name` and `save_data_asset_name` are removed.
            - There are numerous under-the-scenes changes to the internal types used in
              GreatExpectations. These should be transparent to users.


            ***********************
            How to upgrade to 0.8.x
            ***********************

            In the 0.8.0 release, our DataContext config format has changed dramatically to
            enable new features including extensibility.

            Some specific changes:

            - New top-level keys:

              - `expectations_store_name`
              - `evaluation_parameter_store_name`
              - `validations_store_name`

            - Deprecation of the `type` key for configuring objects (replaced by
              `class_name` (and `module_name` as well when ambiguous).
            - Completely new `SiteBuilder` configuration.

            BREAKING:
             - **top-level `validate` has a new signature**, that offers a variety of different options for specifying the DataAsset
               class to use during validation, including `data_asset_class_name` / `data_asset_module_name` or `data_asset_class`
             - Internal class name changes between alpha versions:
               - InMemoryEvaluationParameterStore
               - ValidationsStore
               - ExpectationsStore
               - ActionListValidationOperator
             - Several modules are now refactored into different names including all datasources
             - InMemoryBatchKwargs use the key dataset instead of df to be more explicit


            Pre-0.8.x configuration files ``great_expectations.yml`` are not compatible with 0.8.x. Run ``great_expectations --v3-api project check-config`` - it will offer to create a new config file. The new config file will not have any customizations you made, so you will have to copy these from the old file.

            If you run into any issues, please ask for help on `Slack <https://greatexpectations.io/slack>`__.

            ***********************
            How to upgrade to 0.7.x
            ***********************

            In version 0.7, GE introduced several new features, and significantly changed the way DataContext objects work:

             - A :ref:`data_context` object manages access to expectation suites and other configuration in addition to data assets.
               It provides a flexible but opinionated structure for creating and storing configuration and expectations in version
               control.

             - When upgrading from prior versions, the new :ref:`datasource` objects provide the same functionality that compute-
               environment-specific data context objects provided before, but with significantly more flexibility.

             - The term "autoinspect" is no longer used directly, having been replaced by a much more flexible :ref:`profiling`
               feature.


   .. discourse::
      :topic_identifier: 235
