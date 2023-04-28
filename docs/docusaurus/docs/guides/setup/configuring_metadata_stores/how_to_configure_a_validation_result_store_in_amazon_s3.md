---
title: How to configure a Validation Result Store in Amazon S3
---

import Preface from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_preface.mdx'
import ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored from './components/_install_boto3_with_pip.mdx'
import VerifyYourAwsCredentials from './components/_verify_aws_credentials_are_configured_properly.mdx'
import IdentifyYourDataContextValidationResultsStore from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_identify_your_data_context_validation_results_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_validation_results_on_s.mdx'
import CopyExistingValidationResultsToTheSBucketThisStepIsOptional from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_copy_existing_validation_results_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheNewValidationResultsStoreHasBeenAddedByRunningGreatExpectationsStoreList from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_confirm_that_the_new_validation_results_store_has_been_added_by_running_great_expectations_store_list.mdx'
import ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_confirm_that_the_validations_results_store_has_been_correctly_configured.mdx'
import Congrats from '../components/_congrats.mdx'
import CLIRemoval from '/docs/components/warnings/_cli_removal.md'

<Preface />

## Steps

### 1. Install boto3 to your local environment
<ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored />

### 2. Verify that your AWS credentials are properly configured
<VerifyYourAwsCredentials />

### 3. Identify your Data Context Validation Results Store
<IdentifyYourDataContextValidationResultsStore />

### 4. Update your configuration file to include a new Store for Validation Results on S3
<UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS />

### 5. Confirm that the new Validation Results Store has been properly added
<ConfirmThatTheNewValidationResultsStoreHasBeenAddedByRunningGreatExpectationsStoreList />

### 6. Copy existing Validation results to the S3 bucket (This step is optional)
<CopyExistingValidationResultsToTheSBucketThisStepIsOptional />

### 7. Confirm that the Validations Results Store has been correctly configured
<ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured />

<Congrats/>

You have configured your Validation Results Store to exist in your S3 bucket!