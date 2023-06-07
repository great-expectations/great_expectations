from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Union

from pydantic import StrictInt, StrictStr

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.id_dict import IDDict

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.core.batch import BatchDefinition

logger = logging.getLogger(__name__)


class SliceValidator:
    """
    A custom slice class which has implemented:
      - __get_validators__ for type validation
      - __modify_schemas__ to provide custom json schema
    """

    def __init__(self, slice_validator: slice):
        self._slice = slice_validator
        self.start = self._slice.start
        self.stop = self._slice.stop
        self.step = self._slice.step

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema):
        # __modify_schema__ should mutate the dict it receives in place,
        # the returned value will be ignored
        field_schema.update(
            slice={
                "description": "A slice object representing the set of indices specified by range(start, stop, step).",
                "type": "object",
                "properties": {
                    "start": {
                        "description": "The starting index of the slice.",
                        "type": "integer",
                    },
                    "stop": {
                        "description": "The stopping index of the slice.",
                        "type": "integer",
                    },
                    "step": {
                        "description": "The number of steps between indices.",
                        "type": "integer",
                    },
                },
            }
        )

    @classmethod
    def validate(cls, v):
        if not isinstance(v, slice):
            raise TypeError("slice required")
        return cls(v)


BatchSlice: TypeAlias = Union[
    Sequence[Union[StrictInt, None]], SliceValidator, StrictInt, StrictStr
]


def build_batch_filter(
    data_connector_query_dict: Optional[
        Dict[
            str,
            Optional[
                Union[
                    int,
                    list,
                    tuple,
                    Union[slice, SliceValidator],
                    str,
                    Union[Dict, IDDict],
                    Callable,
                ]
            ],
        ]
    ] = None
):
    if not data_connector_query_dict:
        return BatchFilter(
            custom_filter_function=None,
            batch_filter_parameters=None,
            index=None,
            limit=None,
        )
    data_connector_query_keys: set = set(data_connector_query_dict.keys())
    if not data_connector_query_keys <= BatchFilter.RECOGNIZED_KEYS:
        raise gx_exceptions.BatchFilterError(
            f"""Unrecognized data_connector_query key(s):
"{str(data_connector_query_keys - BatchFilter.RECOGNIZED_KEYS)}" detected.
            """
        )
    custom_filter_function: Optional[Callable] = data_connector_query_dict.get(  # type: ignore[assignment]
        "custom_filter_function"
    )
    if custom_filter_function and not isinstance(custom_filter_function, Callable):  # type: ignore[arg-type]
        raise gx_exceptions.BatchFilterError(
            f"""The type of a custom_filter must be a function (Python "Callable").  The type given is
"{str(type(custom_filter_function))}", which is illegal.
            """
        )
    batch_filter_parameters: Optional[
        Union[dict, IDDict]
    ] = data_connector_query_dict.get(  # type: ignore[assignment]
        "batch_filter_parameters"
    )
    if batch_filter_parameters:
        if not isinstance(batch_filter_parameters, dict):
            raise gx_exceptions.BatchFilterError(
                f"""The type of batch_filter_parameters must be a dictionary (Python "dict").  The type given is
"{str(type(batch_filter_parameters))}", which is illegal.
                """
            )
        if not all(isinstance(key, str) for key in batch_filter_parameters.keys()):
            raise gx_exceptions.BatchFilterError(
                'All batch_filter_parameters keys must strings (Python "str").'
            )
        batch_filter_parameters = IDDict(batch_filter_parameters)
    index: Optional[BatchSlice] = data_connector_query_dict.get(  # type: ignore[assignment]
        "index"
    )
    limit: Optional[int] = data_connector_query_dict.get("limit")  # type: ignore[assignment]
    if limit and (not isinstance(limit, int) or limit < 0):
        raise gx_exceptions.BatchFilterError(
            f"""The type of a limit must be an integer (Python "int") that is greater than or equal to 0.  The
type and value given are "{str(type(limit))}" and "{limit}", respectively, which is illegal.
            """
        )
    if index is not None and limit is not None:
        raise gx_exceptions.BatchFilterError(
            "Only one of index or limit, but not both, can be specified (specifying both is illegal)."
        )
    parsed_index: slice | None = (
        parse_batch_slice(batch_slice=index) if index is not None else None
    )
    return BatchFilter(
        custom_filter_function=custom_filter_function,
        batch_filter_parameters=batch_filter_parameters,  # type: ignore[arg-type]
        index=parsed_index,
        limit=limit,
    )


def _batch_slice_string_to_slice_params(batch_slice: str) -> list[int | None]:
    # trim whitespace
    parsed_batch_slice = batch_slice.strip()

    slice_params: list[int | None] = []
    if parsed_batch_slice:
        # determine if bracket or slice() notation and choose delimiter
        delimiter: str = ":"
        if (parsed_batch_slice[0] in "[(") and (parsed_batch_slice[-1] in ")]"):
            parsed_batch_slice = parsed_batch_slice[1:-1]
        elif parsed_batch_slice.startswith("slice(") and parsed_batch_slice.endswith(
            ")"
        ):
            parsed_batch_slice = parsed_batch_slice[6:-1]
            delimiter = ","

        # split and convert string to int
        for param in parsed_batch_slice.split(delimiter):
            param = param.strip()  # noqa: PLW2901
            if param and param != "None":
                try:
                    slice_params.append(int(param))
                except ValueError as e:
                    raise ValueError(
                        f'Attempt to convert string slice index "{param}" to integer failed with message: {e}'
                    )
            else:
                slice_params.append(None)

    return slice_params


def _batch_slice_from_string(batch_slice: str) -> slice:
    slice_params: list[int | None] = _batch_slice_string_to_slice_params(
        batch_slice=batch_slice
    )

    if len(slice_params) == 0:
        return slice(0, None, None)
    elif len(slice_params) == 1 and slice_params[0] is not None:
        return _batch_slice_from_int(batch_slice=slice_params[0])
    elif len(slice_params) == 2:  # noqa: PLR2004
        return slice(slice_params[0], slice_params[1], None)
    elif len(slice_params) == 3:  # noqa: PLR2004
        return slice(slice_params[0], slice_params[1], slice_params[2])
    else:
        raise ValueError(
            f"batch_slice string must take the form of a python slice, but {batch_slice} was provided."
        )


def _batch_slice_from_list_or_tuple(batch_slice: list[int] | tuple[int, ...]) -> slice:
    if len(batch_slice) == 0:
        return slice(0, None, None)
    elif len(batch_slice) == 1 and batch_slice[0] is not None:
        return slice(batch_slice[0] - 1, batch_slice[0])
    elif len(batch_slice) == 2:  # noqa: PLR2004
        return slice(batch_slice[0], batch_slice[1])
    elif len(batch_slice) == 3:  # noqa: PLR2004
        return slice(batch_slice[0], batch_slice[1], batch_slice[2])
    else:
        raise ValueError(
            f'batch_slice sequence must be of length 0-3, but "{batch_slice}" was provided.'
        )


def _batch_slice_from_int(batch_slice: int) -> slice:
    if batch_slice == -1:
        return slice(batch_slice, None, None)
    else:
        return slice(batch_slice, batch_slice + 1, None)


def parse_batch_slice(batch_slice: Optional[BatchSlice]) -> slice:
    return_slice: slice
    if batch_slice is None:
        return_slice = slice(0, None, None)
    elif isinstance(batch_slice, slice):
        return_slice = batch_slice
    elif isinstance(batch_slice, SliceValidator):
        return_slice = slice(batch_slice.start, batch_slice.stop, batch_slice.step)
    elif isinstance(batch_slice, int) and not isinstance(batch_slice, bool):
        return_slice = _batch_slice_from_int(batch_slice=batch_slice)
    elif isinstance(batch_slice, str):
        return_slice = _batch_slice_from_string(batch_slice=batch_slice)
    elif isinstance(batch_slice, (list, tuple)):
        return_slice = _batch_slice_from_list_or_tuple(batch_slice=batch_slice)
    else:
        raise TypeError(
            f"`batch_slice` should be of type `BatchSlice`, but type: {type(batch_slice)} was passed."
        )
    logger.info(f"batch_slice: {batch_slice} was parsed to: {return_slice}")
    return return_slice


class BatchFilter:
    RECOGNIZED_KEYS: set = {
        "custom_filter_function",
        "batch_filter_parameters",
        "index",
        "limit",
    }

    def __init__(
        self,
        custom_filter_function: Optional[Callable] = None,
        batch_filter_parameters: Optional[IDDict] = None,
        index: Optional[Union[int, slice]] = None,
        limit: Optional[int] = None,
    ) -> None:
        self._custom_filter_function = custom_filter_function
        self._batch_filter_parameters = batch_filter_parameters
        self._index = index
        self._limit = limit

    @property
    def custom_filter_function(self) -> Optional[Callable]:
        return self._custom_filter_function

    @property
    def batch_filter_parameters(self) -> Optional[IDDict]:
        return self._batch_filter_parameters

    @property
    def index(self) -> Optional[Union[int, slice]]:
        return self._index

    @property
    def limit(self) -> int:
        return self._limit  # type: ignore[return-value]

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "custom_filter_function": self._custom_filter_function,
            "batch_filter_parameters": self.batch_filter_parameters,
            "index": self.index,
            "limit": self.limit,
        }
        return str(doc_fields_dict)

    def select_from_data_connector_query(
        self, batch_definition_list: Optional[List[BatchDefinition]] = None
    ) -> List[BatchDefinition]:
        if batch_definition_list is None:
            return []
        filter_function: Callable
        if self.custom_filter_function:
            filter_function = self.custom_filter_function
        else:
            filter_function = self.best_effort_batch_definition_matcher()
        selected_batch_definitions: List[BatchDefinition]
        selected_batch_definitions = list(
            filter(
                lambda batch_definition: filter_function(
                    batch_identifiers=batch_definition.batch_identifiers,
                ),
                batch_definition_list,
            )
        )
        if len(selected_batch_definitions) == 0:
            return selected_batch_definitions

        if self.index is None:
            selected_batch_definitions = selected_batch_definitions[: self.limit]
        else:
            if isinstance(self.index, int):  # noqa: PLR5501
                selected_batch_definitions = [selected_batch_definitions[self.index]]
            else:
                selected_batch_definitions = list(
                    itertools.chain.from_iterable(
                        [selected_batch_definitions[self.index]]
                    )
                )
        return selected_batch_definitions

    def best_effort_batch_definition_matcher(self) -> Callable:
        def match_batch_identifiers_to_batch_filter_params(
            batch_identifiers: dict,
        ) -> bool:
            if self.batch_filter_parameters:
                if not batch_identifiers:
                    return False

                for batch_filter_parameter, val in self.batch_filter_parameters.items():
                    if not (
                        batch_filter_parameter in batch_identifiers
                        and batch_identifiers[batch_filter_parameter] == val
                    ):
                        return False

            return True

        return match_batch_identifiers_to_batch_filter_params
