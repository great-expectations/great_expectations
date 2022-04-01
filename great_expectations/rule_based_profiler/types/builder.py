import json
from typing import Any, List, Optional, Set, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    RuntimeBatchRequest,
    batch_request_contains_batch_data,
    get_batch_request_as_dict,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot, safe_deep_copy
from great_expectations.util import deep_filter_properties_iterable


class Builder(SerializableDictDot):
    """
    A Builder provides methods to serialize any builder object of a rule generically.
    """

    exclude_field_names: Set[str] = {
        "batch_list",
        "data_context",
    }

    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[
            Union[str, BatchRequest, RuntimeBatchRequest, dict]
        ] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Args:
            data_context: DataContext
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
        """
        self._batch_list = batch_list

        if batch_request_contains_batch_data(batch_request=batch_request):
            raise ValueError(
                f"""Error: batch_data found in batch_request -- only primitive types are allowed as part of \
{self.__class__.__name__} instance attributes.
"""
            )

        self._batch_request = batch_request

        self._data_context = data_context

    """
    Full getter/setter accessors for "batch_request" and "batch_list" are for configuring Builder dynamically.
    """

    @property
    def batch_list(self) -> Optional[List[Batch]]:
        return self._batch_list

    @batch_list.setter
    def batch_list(self, value: List[Batch]) -> None:
        self._batch_list = value

    @property
    def batch_request(self) -> Optional[Union[BatchRequest, RuntimeBatchRequest, dict]]:
        return self._batch_request

    @batch_request.setter
    def batch_request(
        self, value: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]]
    ) -> None:
        if not (value is None or isinstance(value, dict)):
            value = get_batch_request_as_dict(batch_request=value)

        self._batch_request = value

    @property
    def data_context(self) -> Optional["DataContext"]:  # noqa: F821
        return self._data_context

    def set_batch_list_or_batch_request(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        force_batch_data: bool = False,
    ) -> None:
        if force_batch_data or self.batch_request is None:
            self.set_batch_data(
                batch_list=batch_list,
                batch_request=batch_request,
            )

    def set_batch_data(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
    ) -> None:
        arg: Any
        num_supplied_batch_specification_args: int = sum(
            [
                0 if arg is None else 1
                for arg in (
                    batch_list,
                    batch_request,
                )
            ]
        )
        if num_supplied_batch_specification_args > 1:
            raise ge_exceptions.ProfilerConfigurationError(
                f'Please pass at most one of "batch_list" and "batch_request" arguments (you passed {num_supplied_batch_specification_args} arguments).'
            )

        if batch_list is None:
            self.batch_request = batch_request
        else:
            self.batch_list = batch_list

    def to_dict(self) -> dict:
        dict_obj: dict = super().to_dict()
        dict_obj["class_name"] = self.__class__.__name__
        dict_obj["module_name"] = self.__class__.__module__
        return dict_obj

    def to_json_dict(self) -> dict:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
        make this refactoring infeasible at the present time.
        """
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)

        memo[id(self)] = result

        for key, value in self.to_raw_dict().items():
            value_copy = safe_deep_copy(data=value, memo=memo)
            setattr(result, key, value_copy)

        return result

    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )
        return json.dumps(json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        return self.__repr__()
