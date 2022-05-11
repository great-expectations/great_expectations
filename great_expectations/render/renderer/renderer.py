from functools import wraps

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)


def renderer(renderer_type, **kwargs):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")

    def wrapper(renderer_fn):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")

        @wraps(renderer_fn)
        def inner_func(*args, **kwargs):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            return renderer_fn(*args, **kwargs)

        inner_func._renderer_type = renderer_type
        inner_func._renderer_definition_kwargs = kwargs
        return inner_func

    return wrapper


class Renderer:
    def __init__(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        pass

    @classmethod
    def render(cls, ge_object):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return ge_object

    @classmethod
    def _get_expectation_type(cls, ge_object):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if isinstance(ge_object, ExpectationConfiguration):
            return ge_object.expectation_type
        elif isinstance(ge_object, ExpectationValidationResult):
            return ge_object.expectation_config.expectation_type

    @classmethod
    def _find_evr_by_type(cls, evrs, type_):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        for evr in evrs:
            if evr.expectation_config.expectation_type == type_:
                return evr

    @classmethod
    def _find_all_evrs_by_type(cls, evrs, type_, column_=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        ret = []
        for evr in evrs:
            if (evr.expectation_config.expectation_type == type_) and (
                (not column_)
                or (column_ == evr.expectation_config.kwargs.get("column"))
            ):
                ret.append(evr)
        return ret

    @classmethod
    def _get_column_list_from_evrs(cls, evrs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Get list of column names.\n\n        If expect_table_columns_to_match_ordered_list EVR is present, use it as the list, including the order.\n\n        Otherwise, get the list of all columns mentioned in the expectations and order it alphabetically.\n\n        :param evrs:\n        :return: list of columns with best effort sorting\n        "
        evrs_ = evrs if isinstance(evrs, list) else evrs.results
        expect_table_columns_to_match_ordered_list_evr = cls._find_evr_by_type(
            evrs_, "expect_table_columns_to_match_ordered_list"
        )
        sorted_columns = sorted(
            list(
                {
                    evr.expectation_config.kwargs["column"]
                    for evr in evrs_
                    if ("column" in evr.expectation_config.kwargs)
                }
            )
        )
        if expect_table_columns_to_match_ordered_list_evr:
            ordered_columns = expect_table_columns_to_match_ordered_list_evr.result[
                "observed_value"
            ]
        else:
            ordered_columns = []
        if set(sorted_columns) == set(ordered_columns):
            return ordered_columns
        else:
            return sorted_columns

    @classmethod
    def _group_evrs_by_column(cls, validation_results):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        columns = {}
        for evr in validation_results.results:
            if "column" in evr.expectation_config.kwargs:
                column = evr.expectation_config.kwargs["column"]
            else:
                column = "Table-level Expectations"
            if column not in columns:
                columns[column] = []
            columns[column].append(evr)
        return columns
