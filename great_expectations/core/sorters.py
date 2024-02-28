from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

from great_expectations.compatibility.pydantic import dataclasses as pydantic_dc

if TYPE_CHECKING:
    from typing_extensions import TypeAlias, TypeGuard


@pydantic_dc.dataclass(frozen=True)
class Sorter:
    key: str
    reverse: bool = False

    @classmethod
    def sorter_from_list(cls, sorters: SortersDefinition) -> List[Sorter]:
        return _sorter_from_list(sorters=sorters)

    @classmethod
    def sorter_from_str(cls, sort_key: str) -> Sorter:
        return _sorter_from_str(sort_key=sort_key)


SortersDefinition: TypeAlias = List[Union[Sorter, str, dict]]


def _is_sorter_list(
    sorters: SortersDefinition,
) -> TypeGuard[list[Sorter]]:
    if len(sorters) == 0 or isinstance(sorters[0], Sorter):
        return True
    return False


def _is_str_sorter_list(sorters: SortersDefinition) -> TypeGuard[list[str]]:
    if len(sorters) > 0 and isinstance(sorters[0], str):
        return True
    return False


def _sorter_from_list(sorters: SortersDefinition) -> list[Sorter]:
    if _is_sorter_list(sorters):
        return sorters

    # mypy doesn't successfully type-narrow sorters to a list[str] here, so we use
    # another TypeGuard. We could cast instead which may be slightly faster.
    sring_valued_sorter: str
    if _is_str_sorter_list(sorters):
        return [
            _sorter_from_str(sring_valued_sorter) for sring_valued_sorter in sorters
        ]

    # This should never be reached because of static typing but is necessary because
    # mypy doesn't know of the if conditions must evaluate to True.
    raise ValueError(f"sorters is a not a SortersDefinition but is a {type(sorters)}")


def _sorter_from_str(sort_key: str) -> Sorter:
    """Convert a list of strings to Sorter objects

    Args:
        sort_key: A batch metadata key which will be used to sort batches on a data asset.
                  This can be prefixed with a + or - to indicate increasing or decreasing
                  sorting.  If not specified, defaults to increasing order.
    """
    if sort_key[0] == "-":
        return Sorter(key=sort_key[1:], reverse=True)

    if sort_key[0] == "+":
        return Sorter(key=sort_key[1:], reverse=False)

    return Sorter(key=sort_key, reverse=False)
