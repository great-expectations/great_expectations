import logging
from typing import Any

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.data_connector.sorter import Sorter
from great_expectations.util import is_int, is_numeric

logger = logging.getLogger(__name__)


class NumericSorter(Sorter):
    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batch_identifiers: dict = batch_definition.batch_identifiers
        batch_value: Any = batch_identifiers[self.name]
        if not is_numeric(value=batch_value):
            raise ge_exceptions.SorterError(
                f"""BatchDefinition with IDDict "{self.name}" with value "{batch_value}" has value
"{batch_value}" which cannot be part of numeric sort.
"""
            )
        if is_int(value=batch_value):
            return int(batch_value)
        return round(float(batch_value))

    def __repr__(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        doc_fields_dict: dict = {
            "name": self.name,
            "reverse": self.reverse,
            "type": "NumericSorter",
        }
        return str(doc_fields_dict)
