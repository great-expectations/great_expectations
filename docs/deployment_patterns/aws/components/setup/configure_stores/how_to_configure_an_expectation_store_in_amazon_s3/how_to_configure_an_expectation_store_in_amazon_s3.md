---
title: How to configure an Expectation Store to use Amazon S3
---
import ConfigureBotoToConnectToTheAmazonSBucketWhereExpectationsWillBeStored from '../_configure_boto3_to_amazon_s3_bucket.mdx'
import IdentifyYourDataContextExpectationsStore from './_identify_your_data_context_expectations_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS from './_update_your_configuration_file_to_include_a_new_store_for_expectations_on_s.mdx'
import CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional from './_copy_existing_expectation_json_files_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmThatTheNewExpectationsStoreHasBeenAddedByRunningGreatExpectationsStoreList from './_confirm_that_the_new_expectations_store_has_been_added_by_running_great_expectations_store_list.mdx'
import ConfirmThatExpectationsCanBeAccessedFromAmazonSByRunningGreatExpectationsSuiteList from './_confirm_that_expectations_can_be_accessed_from_amazon_s_by_running_great_expectations_suite_list.mdx'

import Prerequisites from '../../../../../../guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';


By default, newly <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  This guide will help you configure Great Expectations to store them in an Amazon S3 bucket.

<Prerequisites>

- [Configured a Data Context](../../../../../../tutorials/getting_started/tutorial_setup.md).
- [Configured an Expectations Suite](../../../../../../tutorials/getting_started/tutorial_create_expectations.md).
- Installed [boto3](https://github.com/boto/boto3) in your local environment.
- Identified the S3 bucket and prefix where Expectations will be stored.

</Prerequisites>

## Steps

### 1. Configure boto3 to connect to the Amazon S3 bucket where Expectations will be stored
<ConfigureBotoToConnectToTheAmazonSBucketWhereExpectationsWillBeStored />

### 2. Identify your Data Context Expectations Store
<IdentifyYourDataContextExpectationsStore />

### 3. Update your configuration file to include a new Store for Expectations on S3
<UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS />

### 4. Copy existing Expectation JSON files to the S3 bucket (This step is optional)
<CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional />

### 5. Confirm that the new Expectations Store has been added by running ``great_expectations store list``
<ConfirmThatTheNewExpectationsStoreHasBeenAddedByRunningGreatExpectationsStoreList />

### 6. Confirm that Expectations can be accessed from Amazon S3 by running ``great_expectations suite list``
<ConfirmThatExpectationsCanBeAccessedFromAmazonSByRunningGreatExpectationsSuiteList />
