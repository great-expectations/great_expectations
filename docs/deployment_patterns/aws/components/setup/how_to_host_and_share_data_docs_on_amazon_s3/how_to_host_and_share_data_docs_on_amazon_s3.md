---
title: How to host and share Data Docs on Amazon S3
---
import ConfigureAnS3Bucket from './_configure_an_s_bucket.mdx'
import ConfigureYourBucketPolicyToEnableAppropriateAccess from './_configure_your_bucket_policy_to_enable_appropriate_access.mdx'
import ApplyThePolicy from './_apply_the_policy.mdx'
import AddANewS3SiteToTheDataDocsSitesSectionOfYourGreat_ExpectationsYml from './_add_a_new_s_site_to_the_data_docs_sites_section_of_your_great_expectationsyml.mdx'
import TestThatYourConfigurationIsCorrectByBuildingTheSite from './_test_that_your_configuration_is_correct_by_building_the_site.mdx'
import AdditionalNotes from './_additional_notes.mdx'
import AdditionalResources from './_additional_resource.mdx'

import Prerequisites from '../../../../../guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '/docs/term_tags/_tag.mdx';


This guide will explain how to host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on AWS S3.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/tutorial_overview.md)
- [Set up the AWS Command Line Interface](https://aws.amazon.com/cli/)

</Prerequisites>

## Steps

### 1. Configure an S3 bucket
<ConfigureAnS3Bucket />

### 2. Configure your bucket policy to enable appropriate access
<ConfigureYourBucketPolicyToEnableAppropriateAccess />

### 3. Apply the policy
<ApplyThePolicy />

### 4. Add a new S3 site to the data_docs_sites section of your great_expectations.yml
<AddANewS3SiteToTheDataDocsSitesSectionOfYourGreatExpectationsYml />

### 5. Test that your configuration is correct by building the site
<TestThatYourConfigurationIsCorrectByBuildingTheSite />

## Additional notes
<AdditionalNotes />

## Additional resources
<AdditionalResources />