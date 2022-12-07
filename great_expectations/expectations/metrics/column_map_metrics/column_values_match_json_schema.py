import json

import jsonschema

# TODO: <Alex>ALEX</Alex>
from great_expectations.core.util import convert_to_json_serializable

# TODO: <Alex>ALEX</Alex>
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

# TODO: <Alex>ALEX</Alex>
# from great_expectations.util import deep_filter_properties_iterable
# TODO: <Alex>ALEX</Alex>


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
        # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark()] COLUMN-0:\n{column} ; TYPE: {str(type(column))}')
        # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark()] JSON_SCHEMA-0:\n{json_schema} ; TYPE: {str(type(json_schema))}')
        # TODO: <Alex>ALEX</Alex>
        # json_schema_ref: dict = {'properties': {'a': {'type': 'integer'}}, 'required': ['a']}
        # json_schema_ref: dict = deep_filter_properties_iterable(properties=json_schema)
        json_schema_ref: dict = convert_to_json_serializable(data=json_schema)
        a = json_schema == json_schema
        # TODO: <Alex>ALEX</Alex>
        # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark()] JSON_SCHEMA-1-SAME:\n{a} ; TYPE: {str(type(a))}')
        def matches_json_schema(val):
            # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-0:\n{val} ; TYPE: {str(type(val))}')
            if val is None:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-1:\n{val} ; TYPE: {str(type(val))}')
                return False
            try:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-2:\n{val} ; TYPE: {str(type(val))}')
                val_json = json.loads(val)
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-2-JSON:\n{val_json} ; TYPE: {str(type(val_json))}')
                jsonschema.validate(val_json, json_schema)
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-2-JSON-GOOD:\n{val_json} ; TYPE: {str(type(val_json))}')
                # jsonschema.validate raises an error if validation fails.
                # So if we make it this far, we know that the validation succeeded.
                return True
            except jsonschema.ValidationError:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-3-JSON-BAD!!!')
                return False
            except jsonschema.SchemaError:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-4-JSON-BAD!!!')
                raise
            except:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().matches_json_schema()] VAL-5-JSON-BAD!!!')
                raise

        # TODO: <Alex>ALEX</Alex>
        def alex_spark_udf_test(val):
            # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-0:\n{val} ; TYPE: {str(type(val))}')
            if val is None:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-1:\n{val} ; TYPE: {str(type(val))}')
                return False
            try:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-2:\n{val} ; TYPE: {str(type(val))}')
                val_json = json.loads(val)
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-2-JSON:\n{val_json} ; TYPE: {str(type(val_json))}')
                # jsonschema.validate(val_json, json_schema)
                # TODO: <Alex>ALEX</Alex>
                jsonschema.validate(instance=val_json, schema=json_schema_ref)
                # jsonschema.validate(instance=val_json, schema={})
                # TODO: <Alex>ALEX</Alex>
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-2-JSON-GOOD:\n{val_json} ; TYPE: {str(type(val_json))}')
                # jsonschema.validate raises an error if validation fails.
                # So if we make it this far, we know that the validation succeeded.
            except jsonschema.ValidationError:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-3-JSON-BAD!!!')
                return False
            except jsonschema.SchemaError:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-4-JSON-BAD!!!')
                raise
            except:
                # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ALEX_SPARK_UDF_TEST()] VAL-5-JSON-BAD!!!')
                raise

        # TODO: <Alex>ALEX</Alex>
        # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().ABOUT_TO_MAKE_UDF')
        # TODO: <Alex>ALEX</Alex>
        # matches_json_schema_udf = F.udf(matches_json_schema, sparktypes.BooleanType())
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # matches_json_schema_udf = F.udf(lambda val: matches_json_schema(val=val), sparktypes.BooleanType())
        # matches_json_schema_udf = F.udf(lambda val: str(val) == str(val), sparktypes.BooleanType())
        matches_json_schema_udf = F.udf(
            lambda val: alex_spark_udf_test(val=val), sparktypes.BooleanType()
        )
        # TODO: <Alex>ALEX</Alex>
        # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark().MADE_UDF-FINE')
        # TODO: <Alex>ALEX</Alex>
        # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark()] COLUMN-1:\n{column} ; TYPE: {str(type(column))}')
        # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark()] COLUMN-1.NAME:\n{str(column)} ; TYPE: {str(type(str(column)))}')
        try:
            a = matches_json_schema_udf(column)
            # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark()] COLUMN-1-PROCESSED:\n{a} ; TYPE: {str(type(a))}')
            return a
        except Exception as e100:
            # print(f'\n[ALEX_TEST] [ColumnValuesMatchJsonSchema._spark()] EXCEPTION!!!!-UDF?!?!-WHAT?!:\n{e100} ; TYPE: {str(type(e100))}')
            raise e100
        # TODO: <Alex>ALEX</Alex>

        # TODO: <Alex>ALEX</Alex>
        # return matches_json_schema_udf(column)
        # TODO: <Alex>ALEX</Alex>
