When the `result_format` key is set to `"SUMMARY"` the Validation Results of each Expectation includes a `result` dictionary with information that summarizes values to show why it failed or succeeded.  This format is intended for more detailed exploratory work and includes additional information beyond what is included by `BASIC`.

You can check the [Validation Results reference tables](#validation-results-reference-tables) to see what information is provided in the `result` dictionary.
To create a `"SUMMARY"` result format configuration use the following code:

```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - summary Result Format"
```