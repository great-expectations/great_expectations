import copy
from typing import Any, List, Union

from great_expectations.types import DictDot

foo = {
    "batch_data": {"a": ["a", "b", {"foo": {"batch_data": "hey"}}]},
    "validations": [{"batch_data": "foo"}, {"a": {"batch_data": "a"}}],
}


def get_runtime_parameters_batch_data_references_from_config(
    config: Union[DictDot, dict]
) -> List[Union[DictDot, dict]]:
    if not isinstance(config, (DictDot, dict)):
        raise TypeError(
            f"""The Checkpoint configuraiton argument must have the type "dict" or "DictDot" (the type given \
is "{str(type(config))}", which is illegal).
"""
        )

    results = []

    def _walk(node: Any) -> List[Union[DictDot, dict]]:
        if isinstance(node, (dict, DictDot)):
            for k, v in node.items():
                if k == "batch_data":
                    results.append(node)
                _walk(v)
        elif isinstance(node, list):
            for n in node:
                _walk(n)

        return results

    _walk(config)
    return results


results = get_runtime_parameters_batch_data_references_from_config(foo)

for res in copy.deepcopy(results):
    if hasattr(res, "pop"):
        res.pop("batch_data")
    else:
        delattr(res, "batch_data")


__import__("pprint").pprint(foo)
