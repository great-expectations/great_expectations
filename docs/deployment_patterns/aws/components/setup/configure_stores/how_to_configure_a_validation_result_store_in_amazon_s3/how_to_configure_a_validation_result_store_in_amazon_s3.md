---
title: How to configure a Validation Result Store in Amazon S3
---
import ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored from '../_configure_boto3_to_amazon_s3_bucket.mdx'
import IdentifyYourDataContextValidationResultsStore from './_identify_your_data_context_validation_results_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS from './_update_your_configuration_file_to_include_a_new_store_for_validation_results_on_s.mdx'
import CopyExistingValidationResultsToTheSBucketThisStepIsOptional from './_copy_existing_validation_results_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheNewValidationResultsStoreHasBeenAddedByRunningGreatExpectationsStoreList from './_confirm_that_the_new_validation_results_store_has_been_added_by_running_great_expectations_store_list.mdx'
import ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured from './_confirm_that_the_validations_results_store_has_been_correctly_configured.mdx'

import Prerequisites from '../../../../../../guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';



By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  Since Validation Results may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system. This guide will help you configure a new storage location for Validation Results in Amazon S3.

<Prerequisites>

- [Configured a Data Context](../../../../../../tutorials/getting_started/tutorial_setup.md).
- [Configured an Expectations Suite](../../../../../../tutorials/getting_started/tutorial_create_expectations.md).
- [Configured a Checkpoint](../../../../../../tutorials/getting_started/tutorial_validate_data.md).
- [Installed boto3](https://github.com/boto/boto3) in your local environment.
- Identified the S3 bucket and prefix where Validation Results will be stored.

</Prerequisites>


## Steps

### 1. Configure boto3 to connect to the Amazon S3 bucket where Validation results will be stored
<ConfigureBotoToConnectToTheAmazonSBucketWhereValidationResultsWillBeStored />

### 2. Identify your Data Context Validation Results Store
<IdentifyYourDataContextValidationResultsStore />

### 3. Update your configuration file to include a new Store for Validation Results on S3
<UpdateYourConfigurationFileToIncludeANewStoreForValidationResultsOnS />

### 4. Copy existing Validation results to the S3 bucket (This step is optional)
<CopyExistingValidationResultsToTheSBucketThisStepIsOptional />

### 5. Confirm that the new Validation Results Store has been added by running ``great_expectations store list``
<ConfirmThatTheNewValidationResultsStoreHasBeenAddedByRunningGreatExpectationsStoreList />

### 6. Confirm that the Validations Results Store has been correctly configured
<ConfirmThatTheValidationsResultsStoreHasBeenCorrectlyConfigured />
