import warnings

from great_expectations.dataset.util import create_multiple_expectations
from great_expectations.profile.base import DatasetProfiler


class ColumnsExistProfiler(DatasetProfiler):
    @classmethod
    def _profile(cls, dataset, configuration=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        This function will take a dataset and add expectations that each column present exists.\n\n        Args:\n            dataset (great_expectations.dataset): The dataset to profile and to which to add expectations.\n            configuration: Configuration for select profilers.\n        "
        if not hasattr(dataset, "get_table_columns"):
            warnings.warn("No columns list found in dataset; no profiling performed.")
            raise NotImplementedError(
                "ColumnsExistProfiler._profile is not implemented for data assests without the table_columns property"
            )
        table_columns = dataset.get_table_columns()
        if table_columns is None:
            warnings.warn("No columns list found in dataset; no profiling performed.")
            raise NotImplementedError(
                "ColumnsExistProfiler._profile is not implemented for data assests without the table_columns property"
            )
        create_multiple_expectations(dataset, table_columns, "expect_column_to_exist")
        return dataset.get_expectation_suite(suppress_warnings=True)
