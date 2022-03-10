import json
from typing import List, Optional, Set, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot, safe_deep_copy
from great_expectations.util import deep_filter_properties_iterable


class Builder(SerializableDictDot):
    """
    A Builder provides methods to serialize any builder object of a rule generically.
    """

    exclude_field_names: Set[str] = {
        "data_context",
        "batch_list",
    }

    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Args:
            data_context: DataContext
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
        """
        if data_context is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a data_context, but none was provided."
            )

        self._data_context = data_context
        self._batch_request = batch_request

        self._batch_list = batch_list

    """
    Full getter/setter accessors for "batch_request" and "batch_list" are for configuring Builder dynamically.
    """

    @property
    def batch_request(self) -> Optional[Union[BatchRequest, RuntimeBatchRequest, dict]]:
        return self._batch_request

    @batch_request.setter
    def batch_request(
        self, value: Union[BatchRequest, RuntimeBatchRequest, dict]
    ) -> None:
        self._batch_request = value

    @property
    def batch_list(self) -> Optional[List[Batch]]:
        return self._batch_list

    @batch_list.setter
    def batch_list(self, value: List[Batch]) -> None:
        self._batch_list = value

    @property
    def data_context(self) -> "DataContext":  # noqa: F821
        return self._data_context

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
