"""Resolve the Data Context an object belongs to, falling back to the current one.

An object resolves through the Data Context that owns it. Only an object that belongs to no
Data Context resolves through the current one, the context most recently returned by
``get_context()`` or passed to ``set_context()``. Resolving through the owner keeps an object
working after another context becomes current (GX issue #12209).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from great_expectations.data_context.data_context.context_factory import project_manager

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


@dataclass(frozen=True)
class ResolvedContext:
    """The Data Context an object will use, and whether it is the object's own."""

    context: AbstractDataContext
    bound: bool  # True: reached through the owner chain; False: the current context was used


def owner_from_batch_definition(batch_definition: Any) -> AbstractDataContext | None:
    """Return batch_definition.data_asset.datasource.data_context, or None.

    None when any link raises AttributeError (a bare BatchDefinition, a Mock with a spec that
    lacks the attribute), when the chain ends in None (a hand-assembled datasource), or when
    the result is not an AbstractDataContext (an unspecced Mock). Never raises.
    """
    # Imported here because abstract_data_context imports core, so a module-level import
    # would be a cycle.
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )

    try:
        context = batch_definition.data_asset.datasource.data_context
    except AttributeError:
        return None
    return context if isinstance(context, AbstractDataContext) else None


def resolve_context(owner: AbstractDataContext | None) -> ResolvedContext:
    """Return the owner as bound, or else the current context as not bound.

    Raises DataContextRequiredError when there is no owner and no current context.
    """
    if owner is not None:
        return ResolvedContext(context=owner, bound=True)
    return ResolvedContext(context=project_manager.get_current_project(), bound=False)


def _describe_current(context: AbstractDataContext) -> str:
    consulted = f"the current {context.mode} Data Context"
    if context.root_directory:
        consulted += f" at '{context.root_directory}'"
    if context.data_context_id:
        consulted += f" (id {context.data_context_id})"
    return consulted


def unbound_resolution_note(context: AbstractDataContext) -> str:
    """One sentence, with a leading space, saying the object is not bound to a Data Context
    and naming the context consulted: its mode always, its root directory and id when set.
    """
    return (
        " This object is not bound to a Data Context, so the lookup went through"
        f" {_describe_current(context)}."
    )


def consulted_context_note(context: AbstractDataContext) -> str:
    """One sentence, with a leading space, naming the context a lookup went through.

    For a lookup that cannot tell whether the object it serves is bound: a serialized record
    being parsed may have been read from any context's store.
    """
    return f" The lookup went through {_describe_current(context)}."
