
import logging
from typing import Any, List
import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import Sorter
logger = logging.getLogger(__name__)

class CustomListSorter(Sorter):
    '\n    CustomListSorter\n        - The CustomListSorter is able to sort partitions values according to a user-provided custom list.\n    '

    def __init__(self, name: str, orderby: str='asc', reference_list: List[str]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(name=name, orderby=orderby)
        self._reference_list = self._validate_reference_list(reference_list=reference_list)

    @staticmethod
    def _validate_reference_list(reference_list: List[str]=None) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not (reference_list and isinstance(reference_list, list))):
            raise ge_exceptions.SorterError('CustomListSorter requires reference_list which was not provided.')
        for item in reference_list:
            if (not isinstance(item, str)):
                raise ge_exceptions.SorterError(f'Items in reference list for CustomListSorter must have string type (actual type is `{str(type(item))}`).')
        return reference_list

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.name]
        if (batch_value in self._reference_list):
            return self._reference_list.index(batch_value)
        else:
            raise ge_exceptions.SorterError(f'Source {batch_value} was not found in Reference list.  Try again...')

    def __repr__(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        doc_fields_dict: dict = {'name': self.name, 'reverse': self.reverse, 'type': 'CustomListSorter'}
        return str(doc_fields_dict)
