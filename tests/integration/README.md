# Integration Tests

The purpose of tests in this directory are as follows:

1. Ideally, all non-unit tests will live here.
For example, tests that require services.
This allows for better developer experience and also has CI benefits.
2. The `docusaurus` directory contains all code surfaced to users in documentation.
This ensures code snippets in docs are robust and under test.
3. Ideally tests here are executed against an isolated installation of Great Expectations to mitigate packaging risks.

## How these are run

The script `test_script_runner.py` does the heavy lifting of setting up environments and fixtures.

For example, run `pytest -m docs tests/integration/test_script_runner.py` to execute all docs tests.
