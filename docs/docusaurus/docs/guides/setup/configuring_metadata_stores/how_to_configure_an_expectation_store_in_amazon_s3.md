---
title: How to configure an Expectation Store to use Amazon S3
---

import Preface from './components_how_to_configure_an_expectation_store_in_amazon_s3/_preface.mdx'
import InstallBoto3 from './components/_install_boto3_with_pip.mdx'
import VerifyAwsCredentials from './components/_verify_aws_credentials_are_configured_properly.mdx'
import IdentifyYourDataContextExpectationsStore from './components_how_to_configure_an_expectation_store_in_amazon_s3/_identify_your_data_context_expectations_store.mdx'
import UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS from './components_how_to_configure_an_expectation_store_in_amazon_s3/_update_your_configuration_file_to_include_a_new_store_for_expectations_on_s.mdx'
import CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional from './components_how_to_configure_an_expectation_store_in_amazon_s3/_copy_existing_expectation_json_files_to_the_s_bucket_this_step_is_optional.mdx'
import ConfirmList from './components_how_to_configure_an_expectation_store_in_amazon_s3/_confirm_list.mdx'

<Preface />

## 1. Install boto3 with pip
<InstallBoto3 />

## 2. Verify your AWS credentials are properly configured
<VerifyAwsCredentials />

## 3. Identify your Data Context Expectations Store
<IdentifyYourDataContextExpectationsStore />

## 4. Update your configuration file to include a new Store for Expectations
<UpdateYourConfigurationFileToIncludeANewStoreForExpectationsOnS />

## 5. Copy existing Expectation JSON files to the S3 bucket (Optional)
<CopyExistingExpectationJsonFilesToTheSBucketThisStepIsOptional />

## 6. Confirm Expectation Suite availability
<ConfirmList />