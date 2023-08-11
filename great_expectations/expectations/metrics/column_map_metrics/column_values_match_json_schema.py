import json

import jsonschema

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


class ColumnValuesMatchJsonSchema(ColumnMapMetricProvider):
    condition_metric_name = "column_values.match_json_schema"
    condition_value_keys = ("json_schema",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, json_schema, **kwargs):
        def matches_json_schema(val):
            try:
                val_json = json.loads(val)
                jsonschema.validate(val_json, json_schema)
                # jsonschema.validate raises an error if validation fails.
                # So if we make it this far, we know that the validation succeeded.
                return True
            except jsonschema.ValidationError:
                return False
            except jsonschema.SchemaError:
                raise
            except:
                raise

        return column.map(matches_json_schema)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, json_schema, **kwargs):
        # This step insures that Spark UDF defined can be pickled; otherwise, pickle serialization exceptions may occur.
        json_schema = convert_to_json_serializable(data=json_schema)

        def matches_json_schema(val):
            if val is None:
                return False
            try:
                val_json = json.loads(val)
                jsonschema.validate(instance=val_json, schema=json_schema)
                # jsonschema.validate raises an error if validation fails.
                # So if we make it this far, we know that the validation succeeded.
                return True
            except jsonschema.ValidationError:
                return False
            except jsonschema.SchemaError:
                raise
            except:
                raise

        matches_json_schema_udf = F.udf(
            lambda val: matches_json_schema(val=val), pyspark.types.BooleanType()
        )

        return matches_json_schema_udf(column)
