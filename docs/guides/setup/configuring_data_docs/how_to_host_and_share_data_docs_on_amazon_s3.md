---
title: How to host and share Data Docs on Amazon S3
---

import Preface from './components_how_to_host_and_share_data_docs_on_amazon_s3/_preface.mdx'
import CreateAnS3Bucket from './components_how_to_host_and_share_data_docs_on_amazon_s3/_create_an_s3_bucket.mdx'
import ConfigureYourBucketPolicyToEnableAppropriateAccess from './components_how_to_host_and_share_data_docs_on_amazon_s3/_configure_your_bucket_policy_to_enable_appropriate_access.mdx'
import ApplyThePolicy from './components_how_to_host_and_share_data_docs_on_amazon_s3/_apply_the_policy.mdx'
import AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml from './components_how_to_host_and_share_data_docs_on_amazon_s3/_add_a_new_s3_site_to_the_data_docs_sites_section_of_your_great_expectationsyml.mdx'
import TestThatYourConfigurationIsCorrectByBuildingTheSite from './components_how_to_host_and_share_data_docs_on_amazon_s3/_test_that_your_configuration_is_correct_by_building_the_site.mdx'
import AdditionalNotes from './components_how_to_host_and_share_data_docs_on_amazon_s3/_additional_notes.mdx'

<Preface />

## Steps

### 1. Create an S3 bucket
<CreateAnS3Bucket />

### 2. Configure your bucket policy to enable appropriate access
<ConfigureYourBucketPolicyToEnableAppropriateAccess />

### 3. Apply the policy
<ApplyThePolicy />

### 4. Add a new S3 site to the `data_docs_sites` section of your `great_expectations.yml`
<AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml />

### 5. Test that your configuration is correct by building the site
<TestThatYourConfigurationIsCorrectByBuildingTheSite />

## Additional notes
<AdditionalNotes />
