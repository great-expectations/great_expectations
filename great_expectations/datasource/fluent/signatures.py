from __future__ import annotations

import inspect
from inspect import Parameter, Signature
from typing import Callable, Type


def _merge_signatures(
    target: Callable,
    source: Callable,
    exclude: set[str] | None = None,
    return_type: Type | None = None,
) -> Signature:
    """
    Merge 2 method or function signatures.

    The returned signature will retain all positional and keyword arguments from
    `target` as well the return type.

    The `source` signature cannot contain any positional arguments, and will be
    'appended' to the `target` signatures arguments.

    Note: Signatures are immutable, a new signature is returned by this function.
    """
    target_sig: Signature = inspect.signature(target)
    source_sig: Signature = inspect.signature(source)

    final_exclude: set[str] = set()

    final_params: list[Parameter] = []
    for name, param in target_sig.parameters.items():
        if param.kind == Parameter.VAR_KEYWORD:
            continue
        final_exclude.add(name)
        final_params.append(param)

    if exclude:
        final_exclude.update(exclude)

    final_params.extend(
        [
            p
            for (name, p) in source_sig.parameters.items()
            if p.kind != Parameter.VAR_POSITIONAL and name not in final_exclude
        ]
    )

    return Signature(
        parameters=final_params,
        return_annotation=return_type if return_type else target_sig.return_annotation,
    )
