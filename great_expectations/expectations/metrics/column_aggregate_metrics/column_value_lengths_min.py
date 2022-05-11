import warnings

from dateutil.parser import parse

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sparkdf_execution_engine import (
    apply_dateutil_parse,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa


class ColumnValueMin(ColumnAggregateMetricProvider):
    metric_name = "column.min"
    value_keys = ("parse_strings_as_datetimes",)

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if not k.startswith("__"):
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{type(v)}")
        import inspect

        _frame = inspect.currentframe()
        _file = frame.f_code.co_filename
        _func = frame.f_code.co_name
        for (k, v) in frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{type(v)}")
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, type(v).__name__)
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, str(type(v)))
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, type(v))
        print("HELLO WORLD")
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write("hello world")
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write(1)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame.f_locals)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame)
        import inspect

        frame = inspect.currentframe()
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write(frame.f_locals)
        print("HELLO WORLD")
        print("HELLO WORLD")
        import inspect

        inspect.getframeinfo()
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                'The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n',
                DeprecationWarning,
            )
            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column
            return temp_column.min()
        else:
            return column.min()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if not k.startswith("__"):
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{type(v)}")
        import inspect

        _frame = inspect.currentframe()
        _file = frame.f_code.co_filename
        _func = frame.f_code.co_name
        for (k, v) in frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{type(v)}")
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, type(v).__name__)
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, str(type(v)))
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, type(v))
        print("HELLO WORLD")
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write("hello world")
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write(1)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame.f_locals)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame)
        import inspect

        frame = inspect.currentframe()
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write(frame.f_locals)
        print("HELLO WORLD")
        print("HELLO WORLD")
        import inspect

        inspect.getframeinfo()
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                'The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n',
                DeprecationWarning,
            )
        return sa.func.min(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if not k.startswith("__"):
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{v.__class__.__name__}")
        import inspect

        _frame = inspect.currentframe()
        _file = _frame.f_code.co_filename
        _func = _frame.f_code.co_name
        for (k, v) in _frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{type(v)}")
        import inspect

        _frame = inspect.currentframe()
        _file = frame.f_code.co_filename
        _func = frame.f_code.co_name
        for (k, v) in frame.f_locals.items():
            print(f"{_file}:{_func} - {k}:{type(v)}")
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, type(v).__name__)
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, str(type(v)))
        import inspect

        frame = inspect.currentframe()
        for (k, v) in frame.f_back.f_locals.items():
            print(k, type(v))
        print("HELLO WORLD")
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write("hello world")
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write(1)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame.f_locals)
        import inspect
        import json

        frame = inspect.currentframe()
        print(frame)
        import inspect

        frame = inspect.currentframe()
        with open("/Users/cdkini/Code/great_expectations/results.txt", "a") as f:
            f.write(frame.f_locals)
        print("HELLO WORLD")
        print("HELLO WORLD")
        import inspect

        inspect.getframeinfo()
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                'The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n',
                DeprecationWarning,
            )
            try:
                column = apply_dateutil_parse(column=column)
            except TypeError:
                pass
        return F.min(column)
