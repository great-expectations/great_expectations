from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
    _get_dialect_type_module,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetric,
    column_map_condition,
)


class ColumnValuesOfType(ColumnMapMetric):
    condition_metric_name = "column_values.of_type"
    value_keys = ("type_",)

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, value_set, **kwargs):
        return column.isin(value_set)

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, type_, _dialect, _table, **kwargs):
        try:
            col_data = [col for col in _table.columns if col["name"] == column][0]
            col_type = type(col_data["type"])
        except IndexError:
            raise ValueError("Unrecognized column: %s" % column)
        except KeyError:
            raise ValueError("No database type data available for column: %s" % column)

        try:
            # Our goal is to be as explicit as possible. We will match the dialect
            # if that is possible. If there is no dialect available, we *will*
            # match against a top-level SqlAlchemy type if that's possible.
            #
            # This is intended to be a conservative approach.
            #
            # In particular, we *exclude* types that would be valid under an ORM
            # such as "float" for postgresql with this approach

            if type_ is None:
                # vacuously true
                success = True
            else:
                type_module = _get_dialect_type_module(_dialect)
                success = issubclass(col_type, getattr(type_module, type_))

            return {"success": success, "result": {"observed_value": col_type.__name__}}

        except AttributeError:
            raise ValueError("Type not recognized by current driver: %s" % type_)
