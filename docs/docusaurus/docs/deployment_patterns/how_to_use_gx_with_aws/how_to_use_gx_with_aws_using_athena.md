---
title: Use Great Expectations with Amazon Web Services using Athena
sidebar_label: "AWS S3 and Athena"
---
import Prerequisites from '@site/docs/components/_prerequisites.jsx'
import PrereqPython from '@site/docs/components/prerequisites/_python_version.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from './components/_congratulations_aws_athena.md'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

import InProgress from '/docs/components/warnings/_in_progress.md'

<InProgress />

<!-- Part 1: Setup -->

<!-- 1.1 Ensure that the AWS CLI is ready for use -->

<!-- 1.1.1 Verify that the AWS CLI is installed -->
import VerifyAwsInstalled from './components/_aws_cli_verify_installation.md'

<!-- 1.1.2 Verify that your AWS credentials are properly configured -->
import VerifyAwsCredentials from '@site/docs/guides/setup/configuring_metadata_stores/components/_verify_aws_credentials_are_configured_properly.mdx'

<!-- 1.2 Prepare a local installation of Great Expectations -->

<!-- 1.2.1 Verify that your Python version meets requirements -->

import VerifyPythonVersion from '@site/docs/guides/setup/installation/components_local/_check_python_version.mdx'
import WhereToGetPython from './components/_python_where_to_get.md'

<!-- 1.2.2 Create a virtual environment for your Great Expectations project -->

import CreateVirtualEnvironment from '@site/docs/guides/setup/installation/components_local/_create_an_venv_with_pip.mdx'

<!-- 1.2.3 Ensure you have the latest version of pip -->

import GetLatestPip from '@site/docs/guides/setup/installation/components_local/_ensure_latest_pip.mdx'

<!-- 1.2.4 Install boto3 -->

import InstallBoto3WithPip from '@site/docs/guides/setup/configuring_metadata_stores/components/_install_boto3_with_pip.mdx'

<!-- 1.2.5 Install Great Expectations -->

import InstallGxWithPip from '@site/docs/guides/setup/installation/components_local/_install_ge_with_pip.mdx'

<!-- 1.2.6 Verify that Great Expectations installed successfully -->

import VerifySuccessfulGxInstallation from '@site/docs/guides/setup/installation/components_local/_verify_ge_install_succeeded.mdx'

<!-- 1.3 Create your Data Context -->

<!-- 1.4 Configure your Expectations Store on Amazon S3 -->

<!-- 1.4.1 Identify your Data Context Expectations Store -->

import IdentifyDataContextExpectationsStore from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_an_expectation_store_in_amazon_s3/_identify_your_data_context_expectations_store.mdx'

<!-- 1.4.2 Update your configuration file to include a new Store for Expectations on Amazon S3 -->

import AddS3ExpectationsStoreConfiguration from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_an_expectation_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_expectations_on_s.mdx'

<!-- 1.4.3 Verify that the new Amazon S3 Expectations Store has been added successfully -->

import VerifyS3ExpectationsStoreExists from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_an_expectation_store_in_amazon_s3/_confirm_that_the_new_expectations_store_has_been_added_by_running_great_expectations_store_list.mdx'

<!-- 1.4.4 (Optional) Copy existing Expectation JSON files to the Amazon S3 bucket -->

import OptionalCopyExistingExpectationsToS3 from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_an_expectation_store_in_amazon_s3/_copy_existing_expectation_json_files_to_the_s_bucket_this_step_is_optional.mdx'

<!-- 1.4.5 (Optional) Verify that copied Expectations can be accessed from Amazon S3 -->

import OptionalVerifyCopiedExpectationsAreAccessible from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_an_expectation_store_in_amazon_s3/_confirm_list.mdx'

<!-- 1.5 Configure your Validation Results Store on Amazon S3 -->

<!-- 1.5.1 Identify your Data Context's Validation Results Store -->

import IdentifyDataContextValidationResultsStore from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_a_validation_result_store_in_amazon_s3/_identify_your_data_context_validation_results_store.mdx'

<!-- 1.5.2 Update your configuration file to include a new Store for Validation Results on Amazon S3 -->

import AddS3ValidationResultsStoreConfiguration from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_a_validation_result_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_validation_results_on_s.mdx'

<!-- 1.5.3 Verify that the new Amazon S3 Validation Results Store has been added successfully -->

import VerifyS3ValidationResultsStoreExists from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_a_validation_result_store_in_amazon_s3/_update_store_reference.mdx'

<!-- 1.5.4 (Optional) Copy existing Validation results to the Amazon S3 bucket -->

import OptionalCopyExistingValidationResultsToS3 from '@site/docs/guides/setup/configuring_metadata_stores/components_how_to_configure_a_validation_result_store_in_amazon_s3/_copy_existing_validation_results_to_the_s_bucket_this_step_is_optional.mdx'

<!-- 1.6 Configure Data Docs for hosting and sharing from Amazon S3 -->

<!-- 1.6.1 Create an Amazon S3 bucket for your Data Docs -->
import CreateAnS3BucketForDataDocs from '@site/docs/guides/setup/configuring_data_docs/components_how_to_host_and_share_data_docs_on_amazon_s3/_create_an_s3_bucket.mdx'

<!-- 1.6.2 Configure your bucket policy to enable appropriate access -->
import ConfigureYourBucketPolicyToEnableAppropriateAccess from '@site/docs/guides/setup/configuring_data_docs/components_how_to_host_and_share_data_docs_on_amazon_s3/_configure_your_bucket_policy_to_enable_appropriate_access.mdx'

<!-- 1.6.3 Apply the access policy to your Data Docs' Amazon S3 bucket -->
import ApplyTheDataDocsAccessPolicy from '@site/docs/guides/setup/configuring_data_docs/components_how_to_host_and_share_data_docs_on_amazon_s3/_apply_the_policy.mdx'

<!-- 1.6.4 Add a new Amazon S3 site to the `data_docs_sites` section of your `great_expectations.yml` -->
import AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml from '@site/docs/guides/setup/configuring_data_docs/components_how_to_host_and_share_data_docs_on_amazon_s3/_add_a_new_s3_site_to_the_data_docs_sites_section_of_your_great_expectationsyml.mdx'

<!-- 1.6.5 Test that your Data Docs configuration is correct by building the site -->
import TestThatYourConfigurationIsCorrectByBuildingTheSite from '@site/docs/guides/setup/configuring_data_docs/components_how_to_host_and_share_data_docs_on_amazon_s3/_test_that_your_configuration_is_correct_by_building_the_site.mdx'

<!-- Additional notes on hosting Data Docs from an Amazon S3 bucket -->
import AdditionalDataDocsNotes from '@site/docs/guides/setup/configuring_data_docs/components_how_to_host_and_share_data_docs_on_amazon_s3/_additional_notes.mdx'

<!-- Part 2: Connect to data -->

<!-- 2.1 Choose how to run the code for configuring a new Datasource -->

import HowToRunDatasourceCode from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_datasource_code_environment.md'

<!-- 2.2 Instantiate your project's DataContext -->

import InstantiateDataContext from '@site/docs/guides/connecting_to_your_data/cloud/s3/components_pandas/_instantiate_your_projects_datacontext.mdx'

<!-- 2.3 Determine your connection string -->

import ConnectionStringAthena from '@site/docs/guides/connecting_to_your_data/database/components/_connection_string_athena.md'

<!-- 2.4 Configure your Datasource -->

import ConfigureYourDatasource from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_datasource_sql_runtime_configuration.md'

<!-- 2.6 Test your new Datasource -->

import TestAthenaDatasource from '@site/docs/guides/connecting_to_your_data/database/components/_datasource_athena_test.md'

<!-- Part 3: Create Expectations -->

<!-- 3.2: Use a Validator to add Expectations to the Expectation Suite -->

import CreateExpectationsInteractively from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_expectation_suite_add_expectations_with_validator.md'

<!-- 3.3 Save the Expectation Suite -->

import SaveTheExpectationSuite from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_expectation_suite_save.md'

<!-- Part 4: Validate Data -->

<!-- 4.1 Create and run a Checkpoint -->

import CheckpointCreateAndRun from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_create_and_run.md'

<!-- 4.1.1 Create a Checkpoint -->

import CreateCheckpoint from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_create_tabs.md'

<!-- 4.1.2 Save the Checkpoint -->

import SaveCheckpoint from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_save.md'

<!-- 4.1.3 Run the Checkpoint -->

import RunCheckpoint from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_run.md'

<!-- 4.2 Build and view Data Docs -->
import BuildAndViewDataDocs from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_data_docs_build_and_view.md'

Use the information provided here to learn how to use Great Expectations (GX) with AWS and cloud storage. You'll configure a local GX project to store Expectations, Validation Results, and Data Docs in Amazon S3 buckets. You'll also configure Great Expectations to access data stored in an Athena database.

## Prerequisites

<Prerequisites>

- <PrereqPython />
- The AWS CLI. To download and install the AWS CLI, see [Installing or updating the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
- AWS credentials. See [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
- Permissions to install the Python packages ([`boto3`](https://github.com/boto/boto3) and `great_expectations`) with pip.
- An S3 bucket and prefix to store Expectations and Validation Results.

</Prerequisites>

## Ensure that the AWS CLI is ready for use

<VerifyAwsInstalled />

<VerifyAwsCredentials />

## Prepare a local installation of Great Expectations

<VerifyPythonVersion />

<WhereToGetPython />

<CreateVirtualEnvironment />

<GetLatestPip />

<InstallBoto3WithPip />

<InstallGxWithPip />

<VerifySuccessfulGxInstallation />

## Create your Data Context

It is assumed that there is an empty folder to initialize the Filesystem Data Context. For example:

```python title="Python code"
path_to_empty_folder = '/my_gx_project/'
```

You provide the path for the empty folder in the GX library `FileDataContext.create(...)` method as a `project_root_dir` parameter. When you provide the path to the empty folder, the Filesystem Data Context is initialized in that location.

For convenience, the `FileDataContext.create(...)` method instantiates and returns the initialized Data Context, which you can keep in a Python variable. For example:

```python title="Python code"
from great_expectations.data_context import FileDataContext

context = FileDataContext.create(project_root_dir=path_to_empty_folder)
```

## Configure your Expectations Store on Amazon S3

<IdentifyDataContextExpectationsStore />

<AddS3ExpectationsStoreConfiguration />

<VerifyS3ExpectationsStoreExists />

<OptionalCopyExistingExpectationsToS3 />

<OptionalVerifyCopiedExpectationsAreAccessible />

## Configure your Validation Results Store on Amazon S3

<IdentifyDataContextValidationResultsStore />

<AddS3ValidationResultsStoreConfiguration />

<VerifyS3ValidationResultsStoreExists />

<OptionalCopyExistingValidationResultsToS3 />

## Configure Data Docs for hosting and sharing from Amazon S3

<CreateAnS3BucketForDataDocs />

<ConfigureYourBucketPolicyToEnableAppropriateAccess />

<ApplyTheDataDocsAccessPolicy />

<AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml />

<TestThatYourConfigurationIsCorrectByBuildingTheSite />

### Optional settings
<AdditionalDataDocsNotes />

## Connect to data

<HowToRunDatasourceCode />

<InstantiateDataContext />

<ConnectionStringAthena />

<ConfigureYourDatasource />

To configure a SQL Datasource, see [Connect to SQL database source data](/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data.md).

<SaveDatasourceConfigurationToDataContext />

<TestAthenaDatasource />

## Create Expectations

<PrepareABatchRequestAndValidatorForCreatingExpectations />

<CreateExpectationsInteractively />

<SaveTheExpectationSuite />

## Validate Data

<CheckpointCreateAndRun />

<CreateCheckpoint />

<SaveCheckpoint />

<RunCheckpoint />

<BuildAndViewDataDocs />