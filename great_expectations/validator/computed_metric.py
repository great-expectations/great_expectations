from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

# A resolved metric value is backend-determined and dynamically typed: it may be a
# scalar (int, str, float, bool), a container (list, set, tuple, dict), a pandas
# DataFrame/Series, a numpy ndarray, or a deferred SQL/Spark expression object that
# only resolves once its execution engine runs the query or job it represents.
#
# This alias previously spelled that set out as a Union whose first member was
# already `Any` (the carrier for the deferred-expression case). A union containing
# `Any` accepts every value on assignment -- `Any` always matches -- so the union
# added no checking power over assignments the plain alias below also accepts.
# What it did add was cost: every *read* of a `MetricValue`-typed attribute was
# checked against each of the union's other members in turn, so one read produced
# one diagnostic per non-`Any` member whenever the underlying static type couldn't
# be proven to satisfy all of them -- multiplying a single real signal into many
# duplicate, non-actionable ones. Spelling the alias as `Any` keeps every existing
# annotation and every existing assignment valid, in both directions, while
# collapsing that multiplication at its source instead of leaving each caller to
# suppress it individually.
#
# The former member inventory, preserved for readers: Any, List[Any], Set[Any],
# Tuple[Any, ...], pandas.DataFrame, pandas.Series, numpy.ndarray, int, str, float,
# bool, Dict[str, Any].
#
# The name and import path are unchanged, and the alias stays re-exported from the
# validation-graph module, but the object bound to the name is no longer a Union:
# introspecting it at runtime (e.g. via `typing.get_args`) now
# returns no members, where it previously returned twelve, and using it as a
# validated field annotation on a data model would no longer validate anything.
# Neither case occurs anywhere in this codebase today.
MetricValue: TypeAlias = Any
