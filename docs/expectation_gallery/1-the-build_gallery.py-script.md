Build Gallery Script
====================

## The `build_gallery.py` script in your local environment

The script is a powerful local testing tool!

```
Usage: build_gallery.py [OPTIONS] [ARGS]...

  Find Expectations, run their diagnostics methods, and generate JSON files
  with test result summaries for each backend

  - args: snake_name of specific Expectations to include (useful for testing)

  By default, all core and contrib Expectations are found and tested against
  every backend that can be connected to. If any specific Expectation names
  are passed in, only those Expectations will be tested.

  If all Expectations are included and there are no test running modifiers
  specified, the JSON files with tests result summaries will have the "full"
  suffix. If test running modifiers are specified (--ignore-suppress or
  --ignore-only-for), the JSON files will have the "nonstandard" suffix. If
  any Expectations are excluded, the JSON files will have the "partial"
  suffix.

  If all {backend}_full.json files are present and the --only-combine option
  is used, then the complete JSON file for the expectation gallery (including
  a lot of metadata for each Expectation) will be written to outfile_name
  (default: expectation_library_v2--staging.json).

  If running locally (i.e. not in CI), you can run docker containers for
  mssql, mysql, postgresql, and trino. Simply navigate to
  assets/docker/{backend} and run `docker-compose up -d`

Options:
  --only-combine           Generate sqlite_full.json and combine data from
                           other *_full.json files to outfile_name
  -C, --no-core            Do not include core Expectations
  -c, --no-contrib         Do not include contrib/package Expectations
  -S, --ignore-suppress    Ignore the suppress_test_for list on Expectation
                           sample tests
  -O, --ignore-only-for    Ignore the only_for list on Expectation sample
                           tests
  -o, --outfile-name TEXT  Name for the generated JSON file assembled from
                           full backend files (no partials)
  -b, --backends TEXT      Comma-separated names of backends (in a single
                           string) to consider running tests against
                           (bigquery, mssql, mysql, pandas, postgresql,
                           redshift, snowflake, spark, sqlite, trino)
  --help                   Show this message and exit.
```

### Activate your venv, go to the `assets/scripts/` directory, and run the script

```
source venv/bin/activate

cd assets/scripts

python ./build_gallery.py [OPTIONS] [ARGS]
```

Typically you will want to pass in a small subset of backends, especially if you're doing a "local speed run", i.e. `--backends "pandas, sqlite"`. It's also useful to pass in the `snake_case` name of a particular Expectation for fast testing.

### Extra text files

After running the `build_gallery.py` script, there are some plain text files that are written as well.

#### checklists.txt

This file contains the output of each Expectation instance's call to the `print_diagnostic_checklist()` method, which actually calls the `generate_checklist()` method on an ExpectationDiagnostics object.

Lines that begin with a checkmark ✔ are all Expectation checks considered to be successful.

To see all high-level issues with Expectations at a glance, the following shell statement is useful:

```
grep -E "(^expect|Completeness checklist|^ *\"|^ *[A-z]|^ *-|-----)" checklists.txt | less
```

#### docstrings.txt

This file contains the raw docstring of each Expectation and it's markdown rendering, which is the majority of the details page in the gallery site for a particular Expectation.

The `format_docstring_to_markdown` function in the `build_gallery.py` script takes the raw docstring for an Expectation and converts it to markdown.

You can copy the markdown to a file and render with a tool like [GitHub README instant preview (grip)](https://github.com/joeyespo/grip) if you are ever making changes to `format_docstring_to_markdown` for some reason.

> TODO: add link to docstring formatting doc

#### gallery-tracebacks.txt

This file is only created if any exceptions occured while:

- trying to load an Expectation
- trying to get an Expectation's implementation from the registry
- trying to run an Expectation's `run_diagnostics` method
- trying convert the resulting diagnostics object to JSON

These are all scenarios that will lead to the Expectation(s) mentioned in the file to not be included in the final expectation gallery JSON file.

## Full logging/debug output of the `build_gallery.py` script

This is not done automatically, but leverages error redirection and the `tee` command when invoking the script.

```
python ./build_gallery.py ... 2>&1 | tee output--build_gallery.txt
```

There was a lot of effort spent to log precicse details in the Expectation testing process across backends, including durations for loading test data, showing when tests are skipped, what the results are when there are failures, what utility functions are doing what, and more. This was essential to make performance improvements and fix issues with individual Expectations.

> See PRs [4548](https://github.com/great-expectations/great_expectations/pull/4548), [4816](https://github.com/great-expectations/great_expectations/pull/4816), [5239](https://github.com/great-expectations/great_expectations/pull/5239), [5616](https://github.com/great-expectations/great_expectations/pull/5616), [5881](https://github.com/great-expectations/great_expectations/pull/5881), [8019](https://github.com/great-expectations/great_expectations/pull/8019)

Since the logging/debug output follows consistent patterns, it is possible to generate other text files from the captured output by using some standard shell utilities like `grep`, `cut` and `sort`.

## The `build_gallery.py` script in CI

The script is run in Azure Pipelines against every core and contributed Expectation, in every backend that we officially test in (pandas, spark, sqlite, postgresql, mysql, mssql, trino, redshift, bigquery, snowflake).

The resulting JSON file is pushed up to S3 at <https://superconductive-public.s3.us-east-2.amazonaws.com/static/gallery/expectation_library_v2--staging.json> and the Algolia indicies for the staging site are updated.

In the `expectation_gallery` pipeline, there are additional stages after the invocation(s) of `build_gallery.py` to show useful summary output, leveraging the trick mentioned in the [previous section](#full-loggingdebug-output-of-the-build_gallerypy-script).

Currently, there are separate parallelized jobs in the `exp_tests_on_all_backends` stage that each invoke `build_gallery.py` for a single backend and show any testing errors or tracebacks in separate sections before uploading intermediate results to S3.

```
python ./build_gallery.py --backends "SOME-BACKEND" 2>&1 | tee output--build_gallery.txt
grep -o "ERROR - (.*" output--build_gallery.txt | sort > testing-error-messages.txt
touch gallery-tracebacks.txt

# Show testing errors
cat testing-error-messages.txt

# Show gallery tracebacks
cat gallery-tracebacks.txt
```

Then there is a final stage after the parallelized stages complete to make the complete expectation gallery JSON file.

Prior to [PR 7572](https://github.com/great-expectations/great_expectations/pull/7572) that refactored the script to allow parallelization, the `build_gallery.py` script was invoked a single time for all backends and a lot more useful summary info was shown.

```
python ./build_gallery.py --outfile-name "expectation_library_v2--staging.json" 2>&1 | tee output--build_gallery.txt
grep -o "Took .* seconds to .*" output--build_gallery.txt | sort -k2,2nr > testing-times.txt
grep -o "ERROR - (.*" output--build_gallery.txt | sort > testing-error-messages.txt
touch gallery-tracebacks.txt
grep -o "Expectation type.*" output--build_gallery.txt | sort > gallery-exp-types.txt

# Show coverage scores
grep -o "coverage_score:.*" output--build_gallery.txt | sort -k2,2nr

# Show Expectation types and counts
cut -d " " -f 3,4 gallery-exp-types.txt | uniq -c | sort -nr; echo; cut -d " " -f 3,4,6 gallery-exp-types.txt | sort

# Show implemented engines
grep -o "Implemented engines.*" output--build_gallery.txt

# Show full checklist summary
cat checklists.txt

# Show checklist issues
grep -E "(^expect|Completeness checklist|^ *[A-z]|^ *-|-----)" checklists.txt | grep -vE '(No validate_configuration|Using default validate_configuration|Has a full suite|Has passed a manual)'

# Show testing errors
cat testing-error-messages.txt

# Show testing warnings
grep --color -n -i warning -B 2 output--build_gallery.txt || echo "No warnings found"

# Show gallery tracebacks
cat gallery-tracebacks.txt

# Show DataFrame to SQL times
grep "to df.to_sql" testing-times.txt || echo "No df.to_sql calls were made"

# Show testing times grouped by backend and Expectation
grep "to run" testing-times.txt

# Show testing times for individual tests
grep "to evaluate_json" testing-times.txt
```

### Manually triggered pipeline

You can manually trigger the `expectation_gallery` pipeline in Azure Pipeline against a branch you are working on.

- Visit <https://dev.azure.com/great-expectations/great_expectations/_build?definitionId=14> while logged into Azure Pipelines
    - Click the "Run pipeline" button
    - In the "Branch/tag" dropdown, select your branch
    - Click the "Run" button

### Daily cron schedule to run `build_gallery.py`

The script is run automatically once a day against against the develop branch.

See: <https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/azure-pipelines-expectation-gallery.yml#L10-L16>
