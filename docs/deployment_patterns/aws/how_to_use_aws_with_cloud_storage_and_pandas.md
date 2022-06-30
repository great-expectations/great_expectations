---
title: "How to use Great Expectations with Amazon Web Services using cloud storage and Pandas"
---
<!--- Part 1: Setup --->
<!--- Prepare a local installation of Great Expectations -->
import CheckPythonVersion from './components/setup/install/check_python_version.mdx'
import CreateAVirtualEnvironment from './components/setup/install/create_a_virtual_environment.mdx';
import EnsureLatestPipVersion from './components/setup/install/_ensure_latest_pip.mdx'
import ConfigureBoto3 from './components/setup/configure_stores/_configure_boto3_to_amazon_s3_bucket.mdx'
import InstallGeWithPip from './components/setup/install/local/with_pip/install_ge_with_pip.mdx';
import VerifyGeInstallSucceeded from './components/setup/install/verify_ge_install_succeeded.mdx';

<!-- Configure your Data Context -->
import ConfigureANewDataContext from './components/setup/configure_data_context/configure_a_new_data_context_with_the_cli.mdx'

<!-- Configure your credentials -->

<!-- Configure your Stores -->

<!-- Expectation Store-->
import IdentifyYourDataContextExpectationsStore from './components/setup/configure_stores/how_to_configure_an_expectation_store_in_amazon_s3/_identify_your_data_context_expectations_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS from './components/setup/configure_stores/how_to_configure_an_expectation_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_expectations_on_s.mdx'
import CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional from './components/setup/configure_stores/how_to_configure_an_expectation_store_in_amazon_s3/_copy_existing_expectation_json_files_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheNewExpectationsStoreHasBeenAddedByRunningGreatExpectationsStoreList from './components/setup/configure_stores/how_to_configure_an_expectation_store_in_amazon_s3/_confirm_that_the_new_expectations_store_has_been_added_by_running_great_expectations_store_list.mdx'
import ConfirmThatExpectationsCanBeAccessedFromAmazonSByRunningGreatExpectationsSuiteList from './components/setup/configure_stores/how_to_configure_an_expectation_store_in_amazon_s3/_confirm_that_expectations_can_be_accessed_from_amazon_s_by_running_great_expectations_suite_list.mdx'

<!-- Validation Result Store-->
import IdentifyYourDataContextValidationResultsStore from './components/setup/configure_stores/how_to_configure_a_validation_result_store_in_amazon_s3/_identify_your_data_context_validation_results_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS from './components/setup/configure_stores/how_to_configure_a_validation_result_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_validation_results_on_s.mdx'
import CopyExistingValidationResultsToTheSBucketThisStepIsOptional from './components/setup/configure_stores/how_to_configure_a_validation_result_store_in_amazon_s3/_copy_existing_validation_results_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheNewValidationResultsStoreHasBeenAddedByRunningGreatExpectationsStoreList from './components/setup/configure_stores/how_to_configure_a_validation_result_store_in_amazon_s3/_confirm_that_the_new_validation_results_store_has_been_added_by_running_great_expectations_store_list.mdx'
import ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured from './components/setup/configure_stores/how_to_configure_a_validation_result_store_in_amazon_s3/_confirm_that_the_validations_results_store_has_been_correctly_configured.mdx'

<!--Metrics Store
import AddingAMetricstore from './components/setup/configure_stores/how_to_configure_a_metricsstore/_adding_a_s3_metricstore.mdx'
import ConfiguringAValidationAction from './components/setup/configure_stores/how_to_configure_a_metricsstore/_configuring_a_validation_action.mdx'
import TestYourMetricstoreAndStoremetricsaction from './components/setup/configure_stores/how_to_configure_a_metricsstore/_test_your_metricstore_and_storemetricsaction.mdx'
import MetricStoreSummary from './components/setup/configure_stores/how_to_configure_a_metricsstore/_summary.mdx'
-->

<!--Part 2: Connect to Data-->
import WhereToRunCode from '@site/docs/guides/connecting_to_your_data/components/where_to_run_code.md'
import InstantiateYourProjectSDatacontext from './components/connect_to_data/pandas/_instantiate_your_projects_datacontext.mdx'
import ConfigureYourDatasource from './components/connect_to_data/pandas/_configure_your_datasource.mdx'
import SaveTheDatasourceConfigurationToYourDatacontext from './components/connect_to_data/pandas/_save_the_datasource_configuration_to_your_datacontext.mdx'
import TestYourNewDatasource from './components/connect_to_data/pandas/_test_your_new_datasource.mdx'
import AdditionalConnectToDataNotes from './components/connect_to_data/pandas/_additional_notes.mdx'

import Prerequisites from '@site/docs/guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

:::caution Prerequisites

This guide assumes you have:

- Installed Python 3. (Great Expectations requires Python 3. For details on how to download and install Python on your platform, see [python.org](https://www.python.org/downloads/)).
- Installed the AWS CLI. (For guidance on how install this, please see [Amazon's documentation on how to install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html))
- Configured your AWS credentials.  (For guidance in doing this, please see [Amazon's documentation on configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
- the ability to install Python packages ([`boto3`](https://github.com/boto/boto3) and `great_expectations`) with pip.
- Identified the S3 bucket and prefix where Expectations and Validation Results will be stored.
- Familiarity with [configuring a Data Context](@site/docs/tutorials/getting_started/tutorial_setup.md).
- Familiarity with [configuring an Expectations Suite](@site/docs/tutorials/getting_started/tutorial_create_expectations.md).
- Familiarity with [configuring a Checkpoint](@site/docs/tutorials/getting_started/tutorial_validate_data.md).

:::

## **Part 1: Setup**

### **1.1 -** Ensure that the AWS CLI is ready for use

#### **1.1.1 -** Check that the AWS CLI is installed

You can verify that the AWS CLI has been installed by running the command:

```bash title="Terminal command"
aws --version
```

If this command does not respond by informing you of the version information of the AWS CLI, you may need to install the AWS CLI or otherwise troubleshoot your current installation.  For detailed guidance on how to do this, please refer to [Amazon's documentation on how to install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html))

#### **1.1.2 -** Check that your AWS credentials are properly configured

You can verify that your AWS credentials are properly configured by running the command:

```bash title="Terminal command"
aws sts get-caller-identity
```

If your credentials are properly configured, this will output your `UserId`, `Account` and `Arn`.  If your credentials are not configured correctly, this will throw an error.

If an error is thrown, you can find additional guidance on configuring your AWS credentials by referencing [Amazon's documentation on configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).


### **1.2 -** Prepare a local installation of Great Expectations

#### **1.2.1 -** Check your Python Version

<CheckPythonVersion />

If you do not have Python 3 installed, please refer to [python.org](https://www.python.org/downloads/) for the necessary downloads and guidance to perform the installation.

#### **1.2.2 -** Create a virtual environment

<CreateAVirtualEnvironment />

#### **1.2.3 -** Ensure you have the latest version of pip

<EnsureLatestPipVersion />

You will be using pip to install `boto3` and Great Expectations.

### **1.2.4 -** Install boto3

<ConfigureBoto3 />

#### **1.2.5 -** Install Great Expectations

<InstallGeWithPip />

#### **1.2.6 -** Verify that Great Expectations installed successfully


<VerifyGeInstallSucceeded />

### **1.3 -** Create your Data Context

<ConfigureANewDataContext />

### **1.4 -** Configure your Expectations Store on S3

By default, newly <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  This section of the guide will help you configure Great Expectations to store them in an Amazon S3 bucket.

#### **1.4.1 -** Identify your Data Context Expectations Store
<IdentifyYourDataContextExpectationsStore />

#### **1.4.2 -** Update your configuration file to include a new Store for Expectations on S3
<UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS />

#### **1.4.4 -** Confirm that the new Expectations Store has been properly added
<ConfirmThatTheNewExpectationsStoreHasBeenAddedByRunningGreatExpectationsStoreList />

#### **1.4.3 -** Copy existing Expectation JSON files to the S3 bucket (This step is optional)
<CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional />

#### **1.4.5 -** Confirm that copied (optional) Expectations can be accessed from Amazon S3
<ConfirmThatExpectationsCanBeAccessedFromAmazonSByRunningGreatExpectationsSuiteList />

### **1.5 -** Configure your Validation Results Store on S3

By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  This section of the guide will help you configure a new storage location for Validation Results in Amazon S3.

:::caution 

Since Validation Results may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.

:::

#### **1.5.1 -** Identify your Data Context Validation Results Store
<IdentifyYourDataContextValidationResultsStore />

#### **1.5.2 -** Update your configuration file to include a new Store for Validation Results on S3
<UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS />

#### **1.5.3 -** Confirm that the new Validation Results Store has been properly added
<ConfirmThatTheNewValidationResultsStoreHasBeenAddedByRunningGreatExpectationsStoreList />

#### **1.5.4 -** Copy existing Validation results to the S3 bucket (This step is optional)
<CopyExistingValidationResultsToTheSBucketThisStepIsOptional />


### **1.6 -** Host and share Data Docs on S3

## **Part 2: Connect to Data**

### **2.1 -** Choose how to run the code in this guide

<WhereToRunCode />

### **2.2 -** Instantiate your project's DataContext

<InstantiateYourProjectSDatacontext />

### **2.3 -** Configure your Datasource

<ConfigureYourDatasource />

### **2.4 -** Save the Datasource configuration to your DataContext

<SaveTheDatasourceConfigurationToYourDatacontext />

### **2.5 -** Test your new Datasource

<TestYourNewDatasource />

### **2.6 - Example code in GitHub**

:::info 

<AdditionalConnectToDataNotes />

:::

## **Step 3: Create Expectations**

## **Step 4: Validate Data**
