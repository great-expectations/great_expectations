import copy
from typing import Any, List, Union

from great_expectations.types import DictDot


def get_runtime_parameters_batch_data_references_from_config(
    config: Union[DictDot, dict]
) -> List[Union[DictDot, dict]]:
    if not isinstance(config, (DictDot, dict)):
        raise TypeError(
            f"""The Checkpoint configuration argument must have the type "dict" or "DictDot" (the type given \
is "{str(type(config))}", which is illegal).
"""
        )

    results = []

    def _walk(node: Any) -> None:
        if isinstance(node, (dict, DictDot)):
            for k, v in node.items():
                if k == "batch_data":
                    results.append(node)
                _walk(v)
        elif isinstance(node, list):
            for n in node:
                _walk(n)

    _walk(config)
    return results


foo = {
    "batch_data": {"a": ["a", "b", {"foo": {"batch_data": "hey"}}]},
    "validations": [{"batch_data": "foo"}, {"a": {"batch_data": "a"}}],
}

# Get a list of dictionaries that contain `batch_data`
results = get_runtime_parameters_batch_data_references_from_config(foo)


# Copy and delete
copied_results = copy.deepcopy(results)
for res in copied_results:
    if hasattr(res, "pop"):
        res.pop("batch_data")
    else:
        delattr(res, "batch_data")

print(results)
print(copied_results)

# Restore deleted values?


"""
Standard Workflow:

    1. get_runtime_paramters_batch_data_references_from_config
    2. delete_runtime_parameters_batch_data_references_from_config
    3. Some work
    4. restore_runtime_parameters_batch_data_references_from_config (self)
    5. restore_runtime_parameters_batch_data_references_from_config (config)

"""
