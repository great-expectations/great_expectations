When the `result_format` key is set to `"COMPLETE"` the Validation Results of each Expectation includes a `result` dictionary with all available information to explain why it failed or succeeded.  This format is intended for debugging pipelines or developing detailed regression tests and includes additional information beyond what is provided by `"SUMMARY"`.

You can check the [Validation Results reference tables](#validation-results-reference-tables) to see what information is provided in the `result` dictionary.

To create a `"COMPLETE"` result format configuration use the following code:

```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - complete Result Format"
```