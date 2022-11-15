
# Load expectations and packages

Scripts to download format and then upload expectations and packages data from S3 to algolia which will be consumed in greatexpectations.io expectations and packages gallery.





## Installation

Install my-project with npm

```bash
  cd AlgoliaScripts
  npm install
  
  // Upload expectations to algolia
  node upload-expec

  // Upload packages data to algolia
  node upload-packages
```

## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`ALGOLIA_ACCOUNT`

`ALGOLIA_WRITE_KEY`

`ALGOLIA_EXPECTATION_INDEX` : prod_expectations

`ALGOLIA_PACKAGE_INDEX` : prod_packages

`ALGOLIA_PACKAGE_EXPEC_INDEX` : prod_packages_expectations

`ALGOLIA_EXPEC_REPLICA_ALPHA_ASC_INDEX` : prod_expectations_alpha_asc

`ALGOLIA_EXPEC_REPLICA_ALPHA_DSC_INDEX` : prod_expectations_alpha_dsc

`ALGOLIA_EXPEC_REPLICA_COVERAGE_ASC_INDEX` : prod_expectations_coverage_asc

`ALGOLIA_EXPEC_REPLICA_COVERAGE_DSC_INDEX` : prod_expectations_coverage_dsc

`ALGOLIA_EXPEC_REPLICA_CREATED_ASC_INDEX` : prod_expectations_created_asc

`ALGOLIA_EXPEC_REPLICA_CREATED_DSC_INDEX` : prod_expectations_created_dsc

`ALGOLIA_EXPEC_REPLICA_UPDATED_ASC_INDEX` : prod_expectations_updated_asc

`ALGOLIA_EXPEC_REPLICA_UPDATED_DSC_INDEX` : prod_expectations_updated_dsc

`ALGOLIA_PACK_EXPEC_REPLICA_ALPHA_ASC_INDEX` : prod_pack_expectations_alpha_asc

`ALGOLIA_PACK_EXPEC_REPLICA_ALPHA_DSC_INDEX` : prod_pack_expectations_alpha_dsc

`ALGOLIA_PACK_EXPEC_REPLICA_COVERAGE_ASC_INDEX` : prod_pack_expectations_coverage_asc

`ALGOLIA_PACK_EXPEC_REPLICA_COVERAGE_DSC_INDEX` : prod_pack_expectations_coverage_dsc
