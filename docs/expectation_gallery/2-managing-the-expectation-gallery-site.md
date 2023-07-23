Managing the Expectation Gallery Site
=====================================

## Re-deploy the staging site

This should only need to be done if there are new Expectations merged in or if the content in the Expectation details pages changes (i.e. reformatting Expectation docstrings like [PR 6340](https://github.com/great-expectations/great_expectations/pull/6340), [PR 6423](https://github.com/great-expectations/great_expectations/pull/6423), [PR 6577](https://github.com/great-expectations/great_expectations/pull/6577), and [PR 8353](https://github.com/great-expectations/great_expectations/pull/8353)).

- Visit <https://app.netlify.com/sites/staging-great-expectations/deploys> while logged into Netlify
    - Click the "Trigger deploy" button

## Manually promote staging to prod

**Staging changes will never go to prod without doing this!**

The staging S3 file will get copied to prod at <https://superconductive-public.s3.us-east-2.amazonaws.com/static/gallery/expectation_library_v2.json> and the Algolia indicies for the prod site are updated by the `manual_staging_json_to_prod` CI pipeline, which is **only manually triggered**. You can re-deploy the prod site through Netlify.

- Make sure everything looks OK in staging <https://staging-great-expectations.netlify.app/expectations>
    - Check total number of Expectations
    - Look at the "Completeness" view and "Datasource" view
- Visit <https://dev.azure.com/great-expectations/great_expectations/_build?definitionId=18> while logged into Azure Pipelines
    - Click the "Run pipeline" button
    - Click the "Run" button
- Visit <https://app.netlify.com/sites/dazzling-wilson-9787fb/deploys> while logged into Netlify
    - Click the "Trigger deploy" button
- Make sure everything looks OK in prod <https://greatexpectations.io/expectations>

## Misc things you should rarely need to touch

### Netlify environment variables

[Staging site vars](https://app.netlify.com/sites/staging-great-expectations/configuration/env) and [Prod site vars](https://app.netlify.com/sites/dazzling-wilson-9787fb/configuration/env)

- `ALGOLIA_API_KEY`
- `ALGOLIA_APPLICATION_KEY`
- `S3_EXPECTATIONS_URL`
- `S3_PACKAGES_URL`

The Netlify vars are read by [gatsby-node.js in the website repo](https://github.com/greatexpectationslabs/great-expectations-io/blob/main/gatsby-node.js)

### Algolia index names

On the <https://dashboard.algolia.com/apps/2S9KBQSQ3L/indices> page, the names that match these patterns are used by the gallery

- `prod_expectations_*`
- `prod_expectations`
- `prod_pack_expectations_*`
- `prod_packages`
- `staging_expectations_*`
- `staging_expectations`
- `staging_pack_expectations_*`
- `staging_packages`

These are the values of the Azure Pipelines environment variables in [the pipeline yaml files](https://github.com/great-expectations/great_expectations/tree/develop/docs/expectation_gallery) which invoke the [nodejs algolia scripts](https://github.com/great-expectations/great_expectations/tree/develop/assets/scripts/AlgoliaScripts)
