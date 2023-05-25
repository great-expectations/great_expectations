---
title: How to use Great Expectations with Amazon Web Services using S3 and Spark
---
import Prerequisites from '@site/docs/components/_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from './components/_congratulations_aws_s3_spark.md'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

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

<!-- 1.2.5 Install Spark dependencies for S3 -->
import InstallSparkS3Dependencies from './components/_spark_s3_dependencies.md'

<!-- 1.2.6 Install Great Expectations -->

import InstallGxWithPip from '@site/docs/guides/setup/installation/components_local/_install_ge_with_pip.mdx'

<!-- 1.2.7 Verify that Great Expectations installed successfully -->

import VerifySuccessfulGxInstallation from '@site/docs/guides/setup/installation/components_local/_verify_ge_install_succeeded.mdx'

<!-- 1.3 Create your Data Context -->

import CreateDataContextWithCreate from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_initialize_data_context_with_create.mdx'

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

<!-- 2.1 Instantiate your project's DataContext -->

import CreateDataContextWithCreateAgain from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_initialize_data_context_with_create.mdx'

<!-- 2.2 Add Datasource to your DataContext  -->

import ConfigureYourDatasource from '@site/docs/guides/connecting_to_your_data/cloud/s3/components_spark/_configure_your_datasource.md'

<!-- 2.3 Add CSV Asset to your Datasource -->

import AddCSVAssetToS3Datasource from '@site/docs/guides/connecting_to_your_data/cloud/s3/components_spark/_add_csv_asset_to_spark_s3_datasource.md'

<!-- 2.4 Test your new Datasource Asset -->

import TestS3Datasource from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_test_your_new_datasource.mdx'

<!-- Part 3: Create Expectations -->

<!-- 3.1 Prepare a Batch Request, Empty Expectation Suite, and Validator -->

import PrepareABatchRequestAndValidatorForCreatingExpectations from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_add_expectation_suite_and_validator_for_fluent_datasource.mdx'

<!-- 3.2: Use a Validator to add Expectations to the Expectation Suite -->

import CreateExpectationsInteractively from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_expectation_suite_add_expectations_with_validator.md'

<!-- 3.3 Save the Expectation Suite -->

import SaveTheExpectationSuite from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_expectation_suite_save.md'

<!-- Part 4: Validate Data -->

<!-- 4.1 Create and run a Checkpoint -->

import CheckpointCreateAndRun from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_create_and_run.md'

<!-- 4.1.1 Create a Checkpoint -->

import CreateCheckpoint from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_create.md'

<!-- 4.1.2 Run the Checkpoint -->

import RunCheckpoint from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_run.md'

<!-- 4.2 Build and view Data Docs -->
import BuildAndViewDataDocs from '@site/docs/deployment_patterns/how_to_use_gx_with_aws/components/_data_docs_build_and_view.md'

Great Expectations can work within many frameworks.  In this guide you will be shown a workflow for using Great Expectations with AWS and cloud storage.  You will configure a local Great Expectations project to store Expectations, Validation Results, and Data Docs in Amazon S3 buckets.  You will further configure Great Expectations to use Spark and access data stored in another Amazon S3 bucket.

This guide will demonstrate each of the steps necessary to go from installing a new instance of Great Expectations to Validating your data for the first time and viewing your Validation Results as Data Docs.

## Prerequisites

<Prerequisites>

- Python 3. To download and install Python, see [Python downloads](https://www.python.org/downloads/).
- The AWS CLI. To download and install the AWS CLI, see [Installing or updating the latest version of the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
- AWS credentials. See [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
- Permissions to install the Python packages ([`boto3`](https://github.com/boto/boto3) and `great_expectations`) with pip.
- An S3 bucket and prefix to store Expectations and Validation Results.

</Prerequisites>

## Steps

## Part 1: Setup

### 1.1 Ensure that the AWS CLI is ready for use

#### 1.1.1 Verify that the AWS CLI is installed
<VerifyAwsInstalled />

#### 1.1.2 Verify that your AWS credentials are properly configured
<VerifyAwsCredentials />

### 1.2 Prepare a local installation of Great Expectations and necessary dependencies

#### 1.2.1 Verify that your Python version meets requirements
<VerifyPythonVersion />

<WhereToGetPython />

#### 1.2.2 Create a virtual environment for your Great Expectations project
<CreateVirtualEnvironment />

#### 1.2.3 Ensure you have the latest version of pip
<GetLatestPip />

#### 1.2.4 Install boto3
<InstallBoto3WithPip />

#### 1.2.5 Install Spark dependencies for S3
<InstallSparkS3Dependencies />

#### 1.2.6 Install Great Expectations
<InstallGxWithPip />

#### 1.2.7 Verify that Great Expectations installed successfully
<VerifySuccessfulGxInstallation />

### 1.3 Create your Data Context
<CreateDataContextWithCreate />

### 1.4 Configure your Expectations Store on Amazon S3

#### 1.4.1 Identify your Data Context Expectations Store
<IdentifyDataContextExpectationsStore />

#### 1.4.2 Update your configuration file to include a new Store for Expectations on Amazon S3
<AddS3ExpectationsStoreConfiguration />

#### 1.4.3 Verify that the new Amazon S3 Expectations Store has been added successfully
<VerifyS3ExpectationsStoreExists />

#### 1.4.4 (Optional) Copy existing Expectation JSON files to the Amazon S3 bucket
<OptionalCopyExistingExpectationsToS3 />

#### 1.4.5 (Optional) Verify that copied Expectations can be accessed from Amazon S3
<OptionalVerifyCopiedExpectationsAreAccessible />

### 1.5 Configure your Validation Results Store on Amazon S3

#### 1.5.1 Identify your Data Context's Validation Results Store
<IdentifyDataContextValidationResultsStore />

#### 1.5.2 Update your configuration file to include a new Store for Validation Results on Amazon S3
<AddS3ValidationResultsStoreConfiguration />

#### 1.5.3 Verify that the new Amazon S3 Validation Results Store has been added successfully
<VerifyS3ValidationResultsStoreExists />

#### 1.5.4 (Optional) Copy existing Validation results to the Amazon S3 bucket
<OptionalCopyExistingValidationResultsToS3 />

### 1.6 Configure Data Docs for hosting and sharing from Amazon S3

#### 1.6.1 Create an Amazon S3 bucket for your Data Docs
<CreateAnS3BucketForDataDocs />

#### 1.6.2 Configure your bucket policy to enable appropriate access
<ConfigureYourBucketPolicyToEnableAppropriateAccess />

#### 1.6.3 Apply the access policy to your Data Docs' Amazon S3 bucket
<ApplyTheDataDocsAccessPolicy />

#### 1.6.4 Add a new Amazon S3 site to the `data_docs_sites` section of your `great_expectations.yml`
<AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml />

#### 1.6.5 Test that your Data Docs configuration is correct by building the site
<TestThatYourConfigurationIsCorrectByBuildingTheSite />

#### Additional notes on hosting Data Docs from an Amazon S3 bucket
<AdditionalDataDocsNotes />

## Part 2: Connect to data

### 2.1 Instantiate your project's DataContext
<CreateDataContextWithCreateAgain />

If you have already instantiated your `DataContext` in a previous step, this step can be skipped.

### 2.2 Add Datasource to your DataContext 
<ConfigureYourDatasource />

### 2.3 Add CSV Asset to your Datasource 
<AddCSVAssetToS3Datasource />

### 2.3 Test your new Datasource
<TestS3Datasource />

## Part 3: Create Expectations

### 3.1: Prepare a Batch Request, empty Expectation Suite, and Validator

<PrepareABatchRequestAndValidatorForCreatingExpectations />

### 3.2: Use a Validator to add Expectations to the Expectation Suite

<CreateExpectationsInteractively />

### 3.3: Save the Expectation Suite

<SaveTheExpectationSuite />

## Part 4: Validate Data

### 4.1: Create and run a Checkpoint

<CheckpointCreateAndRun />

#### 4.1.1 Create a Checkpoint

<CreateCheckpoint />

#### 4.1.2 Run the Checkpoint

<RunCheckpoint />

### 4.2: Build and view Data Docs

<BuildAndViewDataDocs />

## Congratulations!

<Congratulations />