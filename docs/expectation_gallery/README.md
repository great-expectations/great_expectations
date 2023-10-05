README
======

## Accepting pull requests

Before ANY pull request introducing a new Expectation is accepted, be sure to checkout the contributor's branch locally (using the [GitHub CLI tool](https://cli.github.com) with `gh pr checkout {PR-NUMBER}`) and run the `build_gallery.py` script against that new Expectation!

```
source venv/bin/activate

cd assets/scripts

python ./build_gallery.py --backends "pandas, spark, sqlite, postgresql" expect_whatever_new_snake_case_name
```

> See: [The build_gallery.py script in your local environment](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/1-the-build_gallery.py-script.md#the-build_gallerypy-script-in-your-local-environment)

## Troubleshooting

### ExpectationNotFoundError raised in `get_expectation_impl`

This will happen if the `CamelCaseName` (Expectation class name) and `snake_case_name` (file name that has the definition of the Expectation class) do not match. **Look very closely** for the typo and either rename the class or the file name.

### Newly merged Expectation is missing from the staging Expectation Gallery

> <https://staging-great-expectations.netlify.app/expectations>

If you have [manually triggered the expectation_gallery pipeline](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/1-the-build_gallery.py-script.md#manually-triggered-pipeline) or the [morning cron job against develop](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/1-the-build_gallery.py-script.md#the-build_gallerypy-script-in-ci) has completed, visit <https://dev.azure.com/great-expectations/great_expectations/_build?definitionId=14>, click the "run" in question, click the `build_gallery_staging` stage, then click `Show gallery tracebacks`. If there is no traceback for your Expectation, [re-deploy the staging site on Netlify](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/2-managing-the-expectation-gallery-site.md#re-deploy-the-staging-site).

If there is a traceback for your Expectation, try to resolve it in your current branch or in a new branch. See [info about gallery-tracebacks.txt](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/1-the-build_gallery.py-script.md#gallery-tracebackstxt).

### The production Expectation Gallery is out of date

> <https://greatexpectations.io/expectations>

You must [manually promote staging to prod](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/2-managing-the-expectation-gallery-site.md#manually-promote-staging-to-prod).
