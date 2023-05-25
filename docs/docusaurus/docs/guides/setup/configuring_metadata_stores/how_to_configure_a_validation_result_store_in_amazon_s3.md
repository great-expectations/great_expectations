---
title: How to configure a Validation Result Store in Amazon S3
---

import Preface from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_preface.mdx'
import ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored from './components/_install_boto3_with_pip.mdx'
import VerifyYourAwsCredentials from './components/_verify_aws_credentials_are_configured_properly.mdx'
import IdentifyYourDataContextValidationResultsStore from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_identify_your_data_context_validation_results_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_validation_results_on_s.mdx'
import CopyExistingValidationResultsToTheSBucketThisStepIsOptional from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_copy_existing_validation_results_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured from './components_how_to_configure_a_validation_result_store_in_amazon_s3/_confirm_that_the_validations_results_store_has_been_correctly_configured.mdx'

<Preface />

## 1. Install boto3 in your local environment
<ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored />

## 2. Verify your AWS credentials are properly configured
<VerifyYourAwsCredentials />

## 3. Identify your Data Context Validation Results Store
<IdentifyYourDataContextValidationResultsStore />

## 4. Update your configuration file to include a new Store for Validation Results
<UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS />

## 5. Copy existing Validation results to the S3 bucket (Optional)
<CopyExistingValidationResultsToTheSBucketThisStepIsOptional />

## 6. Confirm the Validations Results Store configuration
<ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured />
