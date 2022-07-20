
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

`EXPECTATION_INDEX`

`PACKAGE_INDEX`

`PACKAGE_EXPEC_INDEX`
