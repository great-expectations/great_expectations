import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Migration Guide

While we are committed to keeping Great Expectations as stable as possible, sometimes breaking changes are necessary to maintain our trajectory. This is especially true as the library has evolved from just a data quality tool to a more capable framework including [data docs](/docs/reference/data_docs) and [profilers](/docs/reference/profilers) as well as [validation](/docs/reference/validation).

The Batch Request (V3) API was introduced as part of the 0.13 major release of Great Expectations, with an improved Checkpoints feature introduced as part of the 0.13.7 release. The Batch Request (V3) API includes a group of new features based on "new style" Datasources and Modular Expectations, as well as a deprecation of Validation Operators.  These offer a number of advantages including an improved experience around deploying and maintaining Great Expectations in production.

:::note Note on V3 Expectations

  A small number of Expectations have not been fully migrated to V3, and will be very soon. These currently include:
  - `expect_column_bootstrapped_ks_test_p_value_to_be_greater_than`
  - `expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than`
  - `expect_column_chisquare_test_p_value_to_be_greater_than`
  - `expect_column_kl_divergence_to_be_less_than`
  - `expect_column_pair_cramers_phi_value_to_be_less_than`

:::

## Migrating to the Batch Request (V3) API

As of version 0.14.0, the V3 API is the preferred method of interacting with GE. We highly recommend that you migrate to working with the V3 API as soon as possible. Please make sure you're using the latest version of GE before beginning your migration!

The migration involves two parts: first, using an automated CLI tool to upgrade the config file and Data Stores, and second, manually upgrading Datasources and Checkpoints. To begin the migration from the V2 to the V3 API, please do the following:

### Check configuration using Great Expectations CLI

The Great Expectations CLI contains a tool that will check your configuration and determine if it needs to be migrated. To perform this check, run the `project check-config` command in your project folder:

```bash
great_expectations project check-config
```

If your configuration is up-to-date and does not need to be upgraded, you will see the following message:

```bash
Using v3 (Batch Request) API
Checking your config files for validity...

Your config file appears valid!

```

If your configuration needs to be upgraded, you will see a message like this:

```bash
Using v3 (Batch Request) API
Checking your config files for validity...

Unfortunately, your config appears to be invalid:

The config_version of your great_expectations.yml -- 2.0 -- is outdated.
Please consult the V3 API migration guide https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and
upgrade your Great Expectations configuration to version 3.0 in order to take advantage of the latest capabilities.
```

If the `check-config` method has recommended that you upgrade your configuration, you can run the following `project upgrade` command in your project directory.

```bash
great_expectations project upgrade
```

Then you will see the following prompt:

```bash
Using v3 (Batch Request) API

Checking project...

================================================================================

You appear to be using a legacy capability with the latest config version (3.0).
    Your data context with this configuration version uses validation_operators, which are being deprecated.  Please consult the V3 API migration guide https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and update your configuration to be compatible with the version number 3.
    (This message will appear repeatedly until your configuration is updated.)

Checkpoint store named "checkpoint_store" is not a configured store, so will try to use default Checkpoint store.
  Please update your configuration to the new version number 3.0 in order to use the new "Checkpoint Store" feature.
  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process.
++====================================++
|| UpgradeHelperV13: Upgrade Overview ||
++====================================++

UpgradeHelperV13 will upgrade your project to be compatible with Great Expectations V3 API.

**WARNING**: Before proceeding, please make sure you have appropriate backups of your project.

Automated Steps
================

The following Stores and/or Store Names will be upgraded:

    - Stores: checkpoint_store
    - Store Names: checkpoint_store_name

Manual Steps
=============

The following Checkpoints must be upgraded manually, due to using the old Checkpoint format, which is being deprecated:

    - Checkpoints: test_v2_checkpoint

The following Data Sources must be upgraded manually, due to using the old Datasource format, which is being deprecated:

    - Data Sources: my_datasource

Your configuration uses validation_operators, which are being deprecated.  Please, manually convert validation_operators to use the new Checkpoint validation unit, since validation_operators will be deleted.


Upgrade Confirmation
=====================

Please consult the V3 API migration guide for instructions on how to complete any required manual steps or to learn more about the automated upgrade process:

    https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api

Would you like to proceed with the project upgrade? [Y/n]:

```

If you select `Y`, then the following upgrade will automatically occur:

```bash
Upgrading project...

================================================================================

You appear to be using a legacy capability with the latest config version (3.0).
    Your data context with this configuration version uses validation_operators, which are being deprecated.  Please consult the V3 API migration guide https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and update your configuration to be compatible with the version number 3.
    (This message will appear repeatedly until your configuration is updated.)

++================++
|| Upgrade Report ||
++================++

The Upgrade Helper has performed the automated upgrade steps as part of upgrading your project to be compatible with Great Expectations V3 API, and the config_version of your great_expectations.yml has been automatically incremented to 3.0.  However, manual steps are required in order for the upgrade process to be completed successfully.

A log detailing the upgrade can be found here:

    - /Users/work/Development/great_expectations/tests/test_fixtures/configuration_for_testing_v2_v3_migration/v2/great_expectations/uncommitted/logs/project_upgrades/UpgradeHelperV13_20211026T203114.461738Z.json
You appear to be using a legacy capability with the latest config version (3.0).
    Your data context with this configuration version uses validation_operators, which are being deprecated.  Please consult the V3 API migration guide https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and update your configuration to be compatible with the version number 3.
    (This message will appear repeatedly until your configuration is updated.)

Your project requires manual upgrade steps in order to be up-to-date.

You appear to be using a legacy capability with the latest config version (3.0).
    Your data context with this configuration version uses validation_operators, which are being deprecated.  Please consult the V3 API migration guide https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and update your configuration to be compatible with the version number 3.
    (This message will appear repeatedly until your configuration is updated.)

```

Now you are ready to manually migrate Datasources and Checkpoints to be compatible with the V3 API.

### Manually migrate Datasources from V2 to V3

The first manual step needed is to convert the V2-style Datasource to a V3-style one. The following documentation 
contains examples for data read-in using `pandas`, `spark`, and a database, using `postgresql` as an example. 

:::tip

The configurations for `pandas`, `spark` and `postgresql` shown in this guide are available as part of the `great-expectations` repository. 
Please feel free to use the complete-and-working configurations found [here](https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration)
to help with your migration. 

:::

<Tabs
  groupId="configurations-pandas-spark-postgres"
  defaultValue='pandas'
  values={[
  {label: 'Pandas', value:'pandas'},
  {label: 'Spark', value:'spark'},
  {label: 'Database', value:'db'},
  ]}>

<TabItem value="pandas">

The V2-style Datasource has:
  - Data-type specific Datasource, like the `PandasDatasource` in our example below.
  - Data-type specific Datasets, like the `PandasDataset` in our example below.
  - batch_kwargs_generators like the `SubdirReaderBatchKwargsGenerator` in our example below.

The V3-style Datasource has:
  - Datasource that is agnostic to datatype.
  - Datatype-specific ExecutionEngine, like the `PandasExecutionEngine` in our example below.
  - Data-specific DataConnectors, like the `InferredAssetFilesystemDataConnector` in our example below.


#### V2-Style Datasource
```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/pandas/v2/great_expectations/great_expectations.yml#L16-L26
```

#### V3-Style Datasource
```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/pandas/v3/great_expectations/great_expectations.yml#L16-L31
```
 
<details>
  <summary> More details on <code>base_directory</code></summary>
    The <code>base_directory</code> is set to <code>../../../data/</code> according to the example Pandas configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.

</details>

Migrating Datasource configurations that contain connections to the cloud or databases involve additional parameters like credentials that are specific to each configuration. The how-to-guides for Great Expectations contain numerous examples of V3 configurations that can be used for these various situations. Please check out our documentation on [Connecting to your Data](/docs/guides/connecting_to_your_data/index) for examples on V3-style Datasource configurations that will suit your needs.

</TabItem>
<TabItem value="spark">


The V2-style Datasource has:
  - Data-type specific Datasource, like the `SparkDFDatasource` in our example below.
  - Data-type specific Datasets, like the `SparkDFDataset` in our example below.
  - batch_kwargs_generators like the `SubdirReaderBatchKwargsGenerator` in our example below.

The V3-style Datasource has:
  - Datasource that is agnostic to datatype.
  - Datatype-specific ExecutionEngine, like the `SparkDFExecutionEngine` in our example below.
  - Data-specific DataConnectors, like the `InferredAssetFilesystemDataConnector` in our example below.


#### V2-Style Datasource
```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v2/great_expectations/great_expectations.yml#L16-L26
```

#### V3-Style Datasource
```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v3/great_expectations/great_expectations.yml#L16-L34
```

<details>
  <summary> More details on <code>base_directory</code></summary>
    The <code>base_directory</code> is set to <code>../../../data/</code> according to the example Spark configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.

</details>

Migrating Datasource configurations that contain connections to the cloud or databases involve additional parameters like credentials that are specific to each configuration. The how-to-guides for Great Expectations contain numerous examples of V3 configurations that can be used for these various situations. Please check out our documentation on [Connecting to your Data](/docs/guides/connecting_to_your_data/index) for examples on V3-style Datasource configurations that will suit your needs.

</TabItem>

<TabItem value="db">


The V2-style Datasource has:
  - Data-type specific Datasource, like the `SqlAlchemyDatasource` in our example below.
  - Data-type specific Datasets, like the `SqlAlchemyDataset` in our example below.

The V3-style Datasource has:
  - Datasource that is agnostic to datatype.
  - Datatype-specific ExecutionEngine, like the `SqlAlchemyExecutionEngine` in our example below.
  - Data-specific DataConnectors, like the `InferredAssetSqlDataConnector` in our example below.

:::note Note on Datasource in V3

One exception to the datatype-agnostic Datasource in the V3 API is the <code>SimpleSqlalchemyDatasource</code>, which combines functionality of the <code>Datasource</code> and <code>ExecutionEngine</code> to enable [database introspection and partitioning](/docs/guides/connecting_to_your_data/how_to_configure_a_dataconnector_to_introspect_and_partition_tables_in_sql). More examples on using the <code>SimpleSqlalchemyDatasource</code> can be found [here](/docs/guides/connecting_to_your_data/how_to_configure_a_dataconnector_to_introspect_and_partition_tables_in_sql).

:::

#### V2-Style Datasource
```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v2/great_expectations/great_expectations.yml#L16-L23
```

#### V3-Style Datasource
```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v3/great_expectations/great_expectations.yml#L16-L32
```

<details>
  <summary> More details on <code>connection_string</code></summary>
    The <code>connection_string</code> is set according to <code>postgresql+psycopg2://postgres:@localhost/test_ci</code> according to the example Postgres configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>,
    which loads an example dataset (<code>Titanic.csv</code>) into a local postgres database (<code>test_ci</code>).

</details>

Migrating Datasource configurations that contain connections to databases involve additional parameters like credentials that are specific to each configuration. The how-to-guides for Great Expectations contain numerous examples of V3 configurations that can be used for these various situations. 

Please check out the following docs for examples of V3-style Datasource configurations that will suit your needs: 

- [How to connect to a Athena database](/docs/guides/connecting_to_your_data/database/athena)
- [How to connect to a BigQuery database](/docs/guides/connecting_to_your_data/database/bigquery)
- [How to connect to a MySQL database](/docs/guides/connecting_to_your_data/database/mysql)
- [How to connect to a Redshift database](/docs/guides/connecting_to_your_data/database/redshift)
- [How to connect to a Snowflake database](/docs/guides/connecting_to_your_data/database/snowflake)
- [How to connect to a SQLite database](/docs/guides/connecting_to_your_data/database/sqlite)

</TabItem>
</Tabs>

### Manually Migrate V2 Checkpoints to V3 Checkpoints

:::tip

Before doing the migration, we recommend that you create a backup of your V2 Checkpoints. Checkpoints are typically stored as `.yml` configuration files in the `checkpoints/` directory of your `great_expectations/` folder.  We recommend that you make a backup copy of these files or the directory.  

:::

In Great Expectations version 0.13.7, we introduced an improved Checkpoints feature, which allowed Checkpoints to utilize features the V3 API. As a result, Checkpoints are now able to [filter and sort batches from configured datasources](/docs/guides/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource), [introspect and partition tables as batches](/docs/guides/connecting_to_your_data/how_to_configure_a_dataconnector_to_introspect_and_partition_tables_in_sql), with multi-batch Expectations soon to come.  As part of these design improvements, Validation Operators (originally located in the `great_expectations.yml` file) were combined into Checkpoint configurations. 

This means that, although Validation Operators were run directly from the DataContext in V2, they are now run by Checkpoints in V3 as part of `action_list` items. This change offers a convenient abstraction for running Validations and ensures that all actions associated with running validations are included in one place, rather than split up between the `great_expectations.yml` file and Checkpoint configuration.

The example below demonstrates how a V2 to V3 migration can be performed for an existing V2  Checkpoint.


<Tabs
  groupId="configurations-pandas-spark-postgres"
  defaultValue='pandas'
  values={[
  {label: 'Pandas', value:'pandas'},
  {label: 'Spark', value:'spark'},
  {label: 'Database', value:'db'},
  ]}>


<TabItem value="pandas">

The example V2-style Checkpoint contains:
  - A `LegacyCheckpoint`, with no `config_version` (versions were introduced as part of V3-style Checkpoints).
  - A `validation_operator_name` that contains a reference to Validation Operators that are configured in the `great_expectations.yml` file, like `action_list_operator` in our example below.
  - Reference to `batch_kwargs`, like in our example below.

The example V3-style Checkpoint contains:
  - A `Checkpoint` class with `config_version` populated (`1.0` in our example below).
  - A list of `validations`, which contain [BatchRequests](/docs/reference/datasources#batches) that will be used to run the Checkpoint.
  - A `action_list`, which contain a list of actions associated with the Validation Results (e.g., saving them for a later review, sending notifications in case of failures, etc.). These were known as Validation Operators in V2-style Checkpoints.

:::note Migrating ExpectationSuites
  
  `ExpectationSuites` that were created in the V2-API will work in the V3-API **without** needing to be modified. However, `ExpectationSuites` also contain `metadata` describing the `batch` that was used to create the original `ExpectationSuite` object (under the `citations` field). For a suite that was created in V2, this metadata will contain `batch_kwargs`, and V3 suites will contain a `batch_request`. 
  
  If you choose to do so, the `citation` metadata can be migrated using the same pattern for migrating `batch_kwargs` to `batch_request` described below. 

:::

#### V2-Style Checkpoint

```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/pandas/v2/great_expectations/checkpoints/test_v2_checkpoint.yml#L1-L13
```

<details>
  <summary> More details on <code>path</code></summary>
    The <code>path</code> for <code>batch_kwargs</code> is set to <code>../../data/Titanic.csv</code> according to the example Pandas configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.
</details>

The Validation Operator named `action_list_operator` would be part of the `great_expectations.yml` file.

```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/pandas/v2/great_expectations/great_expectations.yml#L56-L68
```

#### V3-Style Checkpoint

Here is the equivalent configuration in V3-style. Notice that the Validation Operators have been migrated into the `action_list` field in the Checkpoint configuration. In addition, you will also need to remove the Validation Operations from `great_expectations.yml` as a manual step.  Also, notice the `batch_request` that refers to the data asset rather than `batch_kwargs`.

For additional examples on how to configure V3-style checkpoints, including how to use `test_yaml_config` to build advanced configurations, please refer to our documentation here:

- [How to add validations data or suites to a Checkpoint](/docs/guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint)
- [How to configure a new Checkpoint using test_yaml_config](/docs/guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config)


```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/pandas/v3/great_expectations/checkpoints/test_v3_checkpoint.yml#L1-L33
```

<details>
  <summary> More details on <code>data_asset_name</code></summary>
    The <code>data_asset_name</code> for <code>batch_request</code> is set to <code>Titanic.csv</code> according to the example Pandas configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.
</details>

If the update was successful, then you should be able to see the updated Checkpoint `test_v3_checkpoint` by running `great_expectations checkpoint list`.

```bash
Using v3 (Batch Request) API
Found 1 Checkpoint.
 - test_v3_checkpoint
 ```

Finally, you can check if your migration has worked by running your new V3-style Checkpoint.

```bash
great_expectations checkpoint run test_v3_checkpoint
```

If everything is successful, then you should see output similar to below.:

```bash
Using v3 (Batch Request) API
Calculating Metrics: 100%|████████████████████████████████████████████████████████████████████████████████████| 3/3 [00:00<00:00, 604.83it/s]
Validation succeeded!

Suite Name                                   Status     Expectations met
- Titanic.profiled                           ✔ Passed   2 of 2 (100.0 %)
```

</TabItem>
<TabItem value="spark">


The example V2-style Checkpoint contains:
  - A `LegacyCheckpoint`, with no `config_version` (versions were introduced as part of V3-style Checkpoints).
  - A `validation_operator_name` that contains a reference to Validation Operators that are configured in the `great_expectations.yml` file, like `action_list_operator` in our example below.
  - Reference to `batch_kwargs`, like in our example below.

The example V3-style Checkpoint contains:
  - A `Checkpoint` class with `config_version` populated (`1.0` in our example below).
  - A list of `validations`, which contain [BatchRequests](/docs/reference/datasources#batches) that will be used to run the Checkpoint.
  - A `action_list`, which contain a list of actions associated with the Validation Results (e.g., saving them for a later review, sending notifications in case of failures, etc.). These were known as Validation Operators in V2-style Checkpoints.

:::note Migrating ExpectationSuites
  
  `ExpectationSuites` that were created in the V2-API will work in the V3-API **without** needing to be modified. However, `ExpectationSuites` also contain `metadata` describing the `batch` that was used to create the original `ExpectationSuite` object (under the `citations` field). For a suite that was created in V2, this metadata will contain `batch_kwargs`, and V3 suites will contain a `batch_request`. 
  
  If you choose to do so, the `citation` metadata can be migrated using the same pattern for migrating `batch_kwargs` to `batch_request` described below. 

:::

#### V2-Style Checkpoint

```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v2/great_expectations/checkpoints/test_v2_checkpoint.yml#L1-L15
```

<details>
  <summary> More details on <code>path</code></summary>
    The <code>path</code> for <code>batch_kwargs</code> is set to <code>../../data/Titanic.csv</code> according to the example Spark configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.
</details>

The Validation Operator named `action_list_operator` would be part of the `great_expectations.yml` file.

```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v2/great_expectations/great_expectations.yml#L57-L69
```

#### V3-Style Checkpoint

Here is the equivalent configuration in V3-style. Notice that the Validation Operators have been migrated into the `action_list` field in the Checkpoint configuration. In addition, you will also need to remove the Validation Operations from `great_expectations.yml` as a manual step.  Also, notice the `batch_request` that refers to the data asset rather than `batch_kwargs`.  In our example, there are also some additional parameters like `ge_cloud_id` and `expectation_suite_ge_cloud_id` that are added automatically and do not need to be modified as part of the migration.

For additional examples on how to configure V3-style checkpoints, including how to use `test_yaml_config` to build advanced configurations, please refer to our documentation here:

- [How to add validations data or suites to a Checkpoint](/docs/guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint)
- [How to configure a new Checkpoint using test_yaml_config](/docs/guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config)


```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/spark/v3/great_expectations/checkpoints/test_v3_checkpoint.yml#L1-L33
```

<details>
  <summary> More details on <code>data_asset_name</code></summary>
    The <code>data_asset_name</code> for <code>batch_request</code> is set to <code>Titanic.csv</code> according to the example Spark configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.
</details>

If the update was successful, then you should be able to see the updated Checkpoint `test_v3_checkpoint` by running `great_expectations checkpoint list`.

```bash
Using v3 (Batch Request) API
Found 1 Checkpoint.
 - test_v3_checkpoint
 ```

Finally, you can check if your migration has worked by running your new V3-style Checkpoint.

```bash
great_expectations checkpoint run test_v3_checkpoint
```

If everything is successful, then you should see output similar to below.:

```bash
Using v3 (Batch Request) API
Calculating Metrics: 100%|█████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00,  6.67it/s]
Validation succeeded!

Suite Name                                   Status     Expectations met
- Titanic.profiled                           ✔ Passed   2 of 2 (100.0 %)
```

</TabItem>
<TabItem value="db">

The example V2-style Checkpoint contains:
  - A `LegacyCheckpoint`, with no `config_version` (versions were introduced as part of V3-style Checkpoints).
  - A `validation_operator_name` that contains a reference to Validation Operators that are configured in the `great_expectations.yml` file, like `action_list_operator` in our example below.
  - Reference to `batch_kwargs`, like in our example below.

The example V3-style Checkpoint contains:
  - A `Checkpoint` class with `config_version` populated (`1.0` in our example below).
  - A list of `validations`, which contain [BatchRequests](/docs/reference/datasources#batches) that will be used to run the Checkpoint.
  - A `action_list`, which contain a list of actions associated with the Validation Results (e.g., saving them for a later review, sending notifications in case of failures, etc.). These were known as Validation Operators in V2-style Checkpoints.

:::note Migrating ExpectationSuites
  
  `ExpectationSuites` that were created in the V2-API will work in the V3-API **without** needing to be modified. However, `ExpectationSuites` also contain `metadata` describing the `batch` that was used to create the original `ExpectationSuite` object (under the `citations` field). For a suite that was created in V2, this metadata will contain `batch_kwargs`, and V3 suites will contain a `batch_request`. 
  
  If you choose to do so, the `citation` metadata can be migrated using the same pattern for migrating `batch_kwargs` to `batch_request` described below. 

:::

#### V2-Style Checkpoint

```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v2/great_expectations/checkpoints/test_v2_checkpoint.yml#L1-L13
```

<details>
  <summary> More details on <code>query</code></summary>
    In the example above, a <code>query</code> is passed in as part of <code>batch_kwargs</code>. In our example we are selecting all rows from <code>public.titanic</code> according to the example Postgres configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.
</details>

The Validation Operator named `action_list_operator` would be part of the `great_expectations.yml` file.

```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v2/great_expectations/great_expectations.yml#L56-L66
```

#### V3-Style Checkpoint

Here is the equivalent configuration in V3-style. Notice that the Validation Operators have been migrated into the `action_list` field in the Checkpoint configuration. In addition, you will also need to remove the Validation Operations from `great_expectations.yml` as a manual step.  Also, notice the `batch_request` that refers to the data asset rather than `batch_kwargs`.

For additional examples on how to configure V3-style checkpoints, including how to use `test_yaml_config` to build advanced configurations, please refer to our documentation here:

- [How to add validations data or suites to a Checkpoint](/docs/guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint)
- [How to configure a new Checkpoint using test_yaml_config](/docs/guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config)


```yaml file=../../../tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v3/great_expectations/checkpoints/test_v3_checkpoint.yml#L1-L35
```

<details>
  <summary> More details on <code>query</code></summary>
    The <code>query</code> for <code>batch_request</code> is passed in as <code>runtime_parameters</code>. In our example we are selecting all rows from <code>public.titanic</code> according to the example Postgres configuration which can be found <a href="https://github.com/great-expectations/great_expectations/tree/develop/tests/test_fixtures/configuration_for_testing_v2_v3_migration"> in the great_expectations repository</a>.
</details>

If the update was successful, then you should be able to see the updated Checkpoint `test_v3_checkpoint` by running `great_expectations checkpoint list`.

```bash
Using v3 (Batch Request) API
Found 1 Checkpoint.
 - test_v3_checkpoint
 ```

Finally, you can check if your migration has worked by running your new V3-style Checkpoint.

```bash
great_expectations checkpoint run test_v3_checkpoint
```

If everything is successful, then you should see output similar to below.:

```bash
Using v3 (Batch Request) API
Calculating Metrics: 100%|████████████████████████████████████████████████████████████████████████████████████| 3/3 [00:00<00:00, 604.83it/s]
Validation succeeded!

Suite Name                                   Status     Expectations met
- Titanic.profiled                           ✔ Passed   2 of 2 (100.0 %)
```
</TabItem>

</Tabs>

Congratulations! You have successfully migrated your configuration of Great Expectations to be compatible with the V3-API.

## Upgrading from previous versions of Great Expectations

Since each major version introduces breaking changes that can have unintended interactions when combined with other changes, we recommend that you only upgrade 1 major version at a time. Notes from previous migration guides are included for reference.

### Upgrading to 0.12.x

The 0.12.0 release makes a small but breaking change to the `add_expectation`, `remove_expectation`, and `find_expectations` methods. To update your code, replace the `expectation_type`, `column`, or `kwargs` arguments with an Expectation Configuration object. For more information on the `match_type` parameter, see [Expectation Suite Operations](/docs/reference/expectation_suite_operations)

For example, using the old API:

```python
    remove_expectation(
      expectation_type="expect_column_values_to_be_in_set",
      column="city",
      expectation_kwargs={"value_set": ["New York","London","Tokyo"]}
    )
```

Using the new API:

```python
    remove_expectation(
      ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        column="city",
        expectation_kwargs={
          "column": "city",
           "value_set": ["New York","London","Tokyo"]
           }
      ),
      match_type="success"
    )    
```


### Upgrading to 0.11.x

The 0.11.0 release has several breaking changes related to `run_id` and `ValidationMetric` objects. Existing projects that have Expectation Suite Validation Results or configured evaluation parameter stores with DatabaseStoreBackend backends must be migrated.

In addition, `ValidationOperator.run` now returns an instance of new type, `ValidationOperatorResult` (instead of a dictionary). If your code uses output from Validation Operators, it must be updated.

#### `run_id` and `ValidationMetric` Changes

`run_id` is now typed using the new `RunIdentifier` class, with optional `run_name` and `run_time` instantiation
arguments. The `run_name` can be any string (this could come from your pipeline runner, e.g. Airflow run id). The `run_time`
can be either a dateutil parsable string or a datetime object. Note - any provided datetime will be assumed to be a UTC time.
If no instantiation arguments are provided, `run_name` will be `None` (and appear as `__none__` in stores) and `run_time`
will default to the current UTC datetime. This change affects all Great Expectations classes that have a `run_id` attribute
as well as any functions or methods that accept a `run_id` argument.

`data_asset_name` (if available) is now added to `batch_kwargs` by `batch_kwargs_generators`.
Because of this newly exposed key in `batch_kwargs`, `ValidationMetric` and associated `ValidationMetricIdentifier`
objects now have a `data_asset_name` attribute.

The affected classes that are relevant to existing projects are `ValidationResultIdentifier` and
`ValidationMetricIdentifier`, as well as any configured stores that rely on these classes for keys, namely
stores of type `ValidationsStore` (and subclasses) or `EvaluationParameterStore` (and other subclasses of
`MetricStore`). In addition, because Expectation Suite Validation Result json objects have a `run_id` key,
existing validation result json files must be updated with a new typed `run_id`.

#### Migrating Your 0.10.x Project

Before performing any of the following migration steps, please make sure you have appropriate backups of your project.

Great Expectations has a CLI Upgrade Helper that helps automate all or most of the migration process (affected
stores with database backends will still have to be migrated manually). The CLI tool makes use of a new class called
UpgradeHelperV11. For reference, the UpgradeHelperV11 class is located at `great_expectations.cli.upgrade_helpers.upgrade_helper_v11`.

To use the CLI Upgrade Helper, enter the following command: `great_expectations project upgrade`

The Upgrade Helper will check your project and guide you through the upgrade process.

:::info
  The following instructions detail the steps required to upgrade your project manually. The migration steps are written in the order they should be completed. They are also provided in the event that the Upgrade Helper is unable to complete a fully automated upgrade and some user intervention is required.
:::

#### 0. Code That Uses Great Expectations

If you are using any Great Expectations methods that accept a `run_id` argument, you should update your code to pass in
the new `RunIdentifier` type (or a dictionary with `run_name` and `run_time` keys). For now, methods with a
`run_id` parameter will continue to accept strings. In this case, the provided `run_id` string will be converted to
a `RunIdentifier` object, acting as the `run_name`. If the `run_id` string can also be parsed as a datetime, it
will also be used for the `run_time` attribute, otherwise, the current UTC time is used. All times are assumed to be
UTC times.

If your code uses output from Validation Operators, it must be updated to handle the new ValidationOperatorResult
type.

#### 1. Expectation Suite Validation Result JSONs

Each existing Expectation Suite Validation Result JSON in your project should be updated with a typed `run_id`. The `run_id`
key is found under the top-level `meta` key. You can use the current `run_id` string as the new `run_name`
(or select a different one). If the current `run_id` is already a datetime string, you can also use it for the `run_time`
as well, otherwise, we suggest using the last modified datetime of the validation result.

:::info
  Subsequent migration steps will make use of this new `run_time` when generating new paths/keys for validation
  result jsons and their Data Docs html pages. Please ensure the `run_time` in these paths/keys match the `run_time`
  in the corresponding validation result. Similarly, if you decide to use a different value for `run_name` instead of
  reusing an existing `run_id` string, make sure this is reflected in the new paths/keys.
:::

For example, an existing validation result json with `run_id="my_run"` should be updated to look like the following::

```json
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
```

#### 2. Stores and their Backends

Stores rely on special identifier classes to serve as keys when getting or setting values. When the signature of an
identifier class changes, any existing stores that rely on that identifier must be updated. Specifically, the structure
of that store's backend must be modified to conform to the new identifier signature.

For example, in a v0.10.x project, you might have an Expectation Suite Validation Result with the following
`ValidationResultIdentifier`::

```json
  v10_identifier = ValidationResultIdentifier(
    expectation_suite_identifier=ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
    run_id="my_string_run_id",
    batch_identifier="some_batch_identifier"
  )
```

A configured `ValidationsStore` with a `TupleFilesystemStoreBackend` (and default config) would use this identifier
to generate the following filepath for writing the validation result to a file (and retrieving it at a later time)::

```python
  v10_filepath = "great_expectations/uncommitted/validations/my_suite_name/my_string_run_id/some_batch_identifier.json"
```

In a v0.11.x project, the `ValidationResultIdentifier` and corresponding filepath would look like the following::

```python
  v11_identifier = ValidationResultIdentifier(
    expectation_suite_identifier=ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
    run_id=RunIdentifier(run_name="my_string_run_name", run_time="2020-05-08T20:51:18.077262"),
    batch_identifier="some_batch_identifier"
  )
  v11_filepath = "great_expectations/uncommitted/validations/my_suite_name/my_string_run_name/2020-05-08T20:51:18.077262/some_batch_identifier.json"
```

When migrating to v0.11.x, you would have to move all existing validation results to new filepaths. For a particular
validation result, you might move the file like this::

  os.makedirs(v11_filepath, exist_ok=True)  # create missing directories from v11 filepath
  shutil.move(v10_filepath, v11_filepath)  # move validation result json file

The following sections detail the changes you must make to existing store backends.

#### 2a. Validations Store Backends

For validations stores with backends of type `TupleFilesystemStoreBackend`, `TupleS3StoreBackend`, or `TupleGCSStoreBackend`,
rename paths (or object keys) of all existing Expectation Suite Validation Result json files:

**Before**

```
great_expectations/uncommitted/validations/my_suite_name/my_run_id/some_batch_identifier.json
```

**After**

```
great_expectations/uncommitted/validations/my_suite_name/my_run_id/my_run_time/batch_identifier.json
```

For validations stores with backends of type `DatabaseStoreBackend`, perform the following database migration:

- add string column with name `run_name`; copy values from `run_id` column
- add string column with name `run_time`; fill with appropriate dateutil parsable values
- delete `run_id` column

#### 2b. Evaluation Parameter Store Backends

If you have any configured evaluation parameter stores that use a `DatabaseStoreBackend` backend, you must perform the
following migration for each database backend:

- add string column with name `data_asset_name`; fill with appropriate values or use "__none__"
- add string column with name `run_name`; copy values from `run_id` column
- add string column with name `run_time`; fill with appropriate dateutil parsable values
- delete `run_id` column

#### 2c. Data Docs Validations Store Backends

:::info
  If you are okay with rebuilding your Data Docs sites, you can skip the migration steps in this section. Instead, you should should run the following CLI command, but **only after** you have completed the above migration steps: `great_expectations docs clean --all && great_expectations docs build`.
:::

For Data Docs sites with store backends of type `TupleFilesystemStoreBackend`, `TupleS3StoreBackend`, or `TupleGCSStoreBackend`, rename
paths (or object keys) of all existing Expectation Suite Validation Result html files:

**Before**

```
great_expectations/uncommitted/data_docs/my_site_name/validations/my_suite_name/my_run_id/some_batch_identifier.html
```

**After**

```
great_expectations/uncommitted/data_docs/my_site_name/validations/my_suite_name/my_run_id/my_run_time/batch_identifier.html
```

### Upgrading to 0.10.x

In the 0.10.0 release, there are several breaking changes to the DataContext API.

Most are related to the clarified naming `BatchKwargsGenerators`.

So, if you are using methods on the data context that used to have an argument named `generators`, you will need to update that code to use the more precise name `batch_kwargs_generators`.

For example, in the method `DataContext.get_available_data_asset_names` the parameter `generator_names` is now `batch_kwargs_generator_names`.

If you are using `BatchKwargsGenerators` in your project config, follow these steps to upgrade your existing Great Expectations project:

- Edit your `great_expectations.yml` file and change the key `generators` to `batch_kwargs_generators`.
- Run a simple command such as: `great_expectations datasource list` and ensure you see a list of datasources.


### Upgrading to 0.9.x

In the 0.9.0 release, there are several changes to the DataContext API.

Follow these steps to upgrade your existing Great Expectations project:


- In the terminal navigate to the parent of the `great_expectations` directory of your project.

- Run this command:

```bash
    great_expectations project check-config
```


- For every item that needs to be renamed the command will display a message that looks like this: `The class name 'X' has changed to 'Y'`. Replace all occurrences of X with Y in your project's `great_expectations.yml` config file.

- After saving the config file, rerun the check-config command.

- Depending on your configuration, you will see 3-6 of these messages.

- The command will display this message when done: `Your config file appears valid!`.

- Rename your Expectation Suites to make them compatible with the new naming. Save this Python code snippet in a file called `update_project.py`, then run it using the command: `python update_project.py PATH_TO_GE_CONFIG_DIRECTORY`:

```python
    # !/usr/bin/env python3
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
    expectations_dir = os.path.join(context_root_dir, "../../reference/expectations")
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
    validations_dir = os.path.join(context_root_dir, "../../../uncommitted", "validations")
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
                            context_root_dir, "../../../uncommitted", "validations",
                            *es_name, run_id
                        ), exist_ok=True)
                        shutil.move(os.path.join(subdir, file),
                                    os.path.join(
                                        context_root_dir, "../../../uncommitted", "validations",
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
```

- Rebuild Data Docs:

```bash
    great_expectations docs build
```

- This project has now been migrated to 0.9.0. Please see the list of changes below for more detailed information.


#### CONFIGURATION CHANGES

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
  specify arbitrary reader_methods via batch_kwargs (including `read_pickle`),
  BUT existing configurations using enum-based reader_method in batch_kwargs
  will need to update their code. For example, a pandas datasource would use
  `reader_method: read_csv` instead of `reader_method: csv`

#### CODE CHANGES

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

### Upgrading to 0.8.x

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

#### Breaking Changes

 - **top-level `validate` has a new signature**, that offers a variety of different options for specifying the DataAsset
   class to use during validation, including `data_asset_class_name` / `data_asset_module_name` or `data_asset_class`
 - Internal class name changes between alpha versions:
   - `InMemoryEvaluationParameterStore`
   - `ValidationsStore`
   - `ExpectationsStore`
   - `ActionListValidationOperator`
 - Several modules are now refactored into different names including all datasources
 - `InMemoryBatchKwargs` use the key dataset instead of df to be more explicit

Pre-0.8.x configuration files `great_expectations.yml` are not compatible with 0.8.x. Run `great_expectations project check-config` - it will offer to create a new config file. The new config file will not have any customizations you made, so you will have to copy these from the old file.

If you run into any issues, please ask for help on [Slack](https://greatexpectations.io/slack).

### Upgrading to 0.7.x

In version 0.7, GE introduced several new features, and significantly changed the way DataContext objects work:

 - A [DataContext](/docs/reference/data_context) object manages access to expectation suites and other configuration in addition to data assets.
   - It provides a flexible but opinionated structure for creating and storing configuration and expectations in version control.

 - When upgrading from prior versions, the new [datasource](/docs/reference/datasources) objects provide the same functionality that compute-environment-specific data context objects provided before, but with significantly more flexibility.

 - The term "autoinspect" is no longer used directly, having been replaced by a much more flexible [profiler](/docs/reference/profilers) feature.
