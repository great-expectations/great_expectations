When the `result_format` is `"BOOLEAN_ONLY"` Validation Results do not include additional information in a `result` dictionary.  The successful evaluation of the Expectation is exclusively returned via the `True` or `False` value of the `success` key in the returned Validation Result.

To create a `"BOOLEAN_ONLY"` result format configuration use the following code:

```python title="Python" name="docs/docusaurus/docs/core/trigger_actions_based_on_results/_examples/choose_result_format.py - boolean_only Result Format"
```