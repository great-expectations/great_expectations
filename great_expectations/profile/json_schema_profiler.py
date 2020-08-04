import logging
from enum import Enum
from typing import Any, Dict, Optional

import jsonschema

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationKwargs,
    ExpectationSuite,
)
from great_expectations.profile.base import Profiler, ProfilerTypeMapping

logger = logging.getLogger(__name__)


class JsonSchemaTypes(Enum):
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    ARRAY = "array"
    NULL = "null"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ENUM = "enum"


class JsonSchemaProfiler(Profiler):
    """
    This profiler creates Expectation Suites from JSONSchema artifacts.

    JSON Schema is a vocabulary that allows you to annotate and validate JSON
    documents. https://json-schema.org

    Basic suites can be created from these specifications.

    Note that there is not yet a notion of nested data types in Great
    Expectations so suites generated use column map expectations.

    Also note that this implementation does not traverse nested schemas and
    requires a top level object of type `object`.
    """

    PROFILER_TYPE_LIST_BY_JSON_SCHEMA_TYPE = {
        JsonSchemaTypes.STRING.value: ProfilerTypeMapping.STRING_TYPE_NAMES,
        JsonSchemaTypes.INTEGER.value: ProfilerTypeMapping.INT_TYPE_NAMES,
        JsonSchemaTypes.NUMBER.value: ProfilerTypeMapping.FLOAT_TYPE_NAMES,
        JsonSchemaTypes.BOOLEAN.value: ProfilerTypeMapping.BOOLEAN_TYPE_NAMES,
    }

    def validate(self, schema: dict) -> bool:
        if not isinstance(schema, dict):
            raise TypeError(
                f"This profiler requires a schema of type dict and was passed a {type(schema)}"
            )
        if "type" not in schema.keys():
            raise KeyError(
                f"This profiler requires a json schema with a top level `type` key"
            )
        if schema["type"] != JsonSchemaTypes.OBJECT.value:
            raise TypeError(
                f"This profiler requires a json schema with a top level `type` of `object`"
            )
        validator = jsonschema.validators.validator_for(schema)
        validator.check_schema(schema)
        return True

    def _profile(self, schema: dict, suite_name: str = None) -> ExpectationSuite:
        if not suite_name:
            raise ValueError("Please provide a suite name when using this profiler.")
        expectations = []
        # TODO add recursion to allow creation of suites for nested schema files
        if schema["type"] == JsonSchemaTypes.OBJECT.value:
            for key, details in schema["properties"].items():
                expectations.append(self._create_existence_expectation(key, details))

                type_expectation = self._create_type_expectation(key, details)
                if type_expectation:
                    expectations.append(type_expectation)

                range_expectation = self._create_range_expectation(key, details)
                if range_expectation:
                    expectations.append(range_expectation)

                boolean_expectation = self._create_boolean_expectation(key, details)
                if boolean_expectation:
                    expectations.append(boolean_expectation)

                set_expectation = self._create_set_expectation(key, details)
                if set_expectation:
                    expectations.append(set_expectation)

                string_len_expectation = self._create_string_length_expectation(
                    key, details
                )
                if string_len_expectation:
                    expectations.append(string_len_expectation)
        description = schema.get("description", None)
        meta = None
        if description:
            meta = {
                "notes": {
                    "format": "markdown",
                    "content": [f"### Description:\n{description}"],
                }
            }
        suite = ExpectationSuite(suite_name, expectations=expectations, meta=meta)
        suite.add_citation(
            comment=f"This suite was built by the {self.__class__.__name__}",
        )
        return suite

    def _create_existence_expectation(
        self, key: str, details: dict
    ) -> ExpectationConfiguration:
        kwargs = ExpectationKwargs(column=key)
        description = details.get("description", None)
        meta = None
        if description:
            meta = {
                "notes": {
                    "format": "markdown",
                    "content": [f"### Description:\n{description}"],
                }
            }
        return ExpectationConfiguration("expect_column_to_exist", kwargs, meta=meta)

    def _create_type_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        type_ = details.get("type", None)
        if type_ is None:
            return None

        type_list = self.PROFILER_TYPE_LIST_BY_JSON_SCHEMA_TYPE[type_]
        kwargs = ExpectationKwargs(column=key, type_list=type_list)
        return ExpectationConfiguration(
            "expect_column_values_to_be_in_type_list", kwargs
        )

    def _create_boolean_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        """https://json-schema.org/understanding-json-schema/reference/boolean.html"""
        type_ = details.get("type", None)
        if type_ != JsonSchemaTypes.BOOLEAN.value:
            return None

        # TODO map JSONSchema types to which type backend? Pandas? Should this value set be parameterized per back end?
        kwargs = ExpectationKwargs(column=key, value_set=[True, False])
        return ExpectationConfiguration("expect_column_values_to_be_in_set", kwargs)

    def _create_range_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        """https://json-schema.org/understanding-json-schema/reference/numeric.html#range"""
        type_ = details.get("type", None)
        if type_ not in [JsonSchemaTypes.INTEGER.value, JsonSchemaTypes.NUMBER.value]:
            return None

        minimum = details.get("minimum", None)
        maximum = details.get("maximum", None)
        exclusive_minimum = details.get("exclusiveMinimum", None)
        exclusive_maximum = details.get("exclusiveMaximum", None)

        if (
            minimum is None
            and maximum is None
            and exclusive_minimum is None
            and exclusive_maximum is None
        ):
            return None

        kwargs: Dict[str, Any] = {"column": key}
        if minimum is not None:
            kwargs["min_value"] = minimum
        if maximum is not None:
            kwargs["max_value"] = maximum
        if exclusive_minimum is not None:
            kwargs["min_value"] = exclusive_minimum
            kwargs["strict_min"] = True
        if exclusive_maximum is not None:
            kwargs["max_value"] = exclusive_maximum
            kwargs["strict_max"] = True

        return ExpectationConfiguration(
            "expect_column_values_to_be_between", ExpectationKwargs(kwargs)
        )

    def _create_string_length_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        """https://json-schema.org/understanding-json-schema/reference/string.html#length"""
        type_ = details.get("type", None)
        minimum = details.get("minLength", None)
        maximum = details.get("maxLength", None)

        if type_ != JsonSchemaTypes.STRING.value:
            return None
        if minimum is None and maximum is None:
            return None

        kwargs = {
            "column": key,
        }
        if minimum == maximum:
            kwargs["value"] = minimum
            return ExpectationConfiguration(
                "expect_column_value_lengths_to_equal", ExpectationKwargs(kwargs)
            )
        if minimum is not None:
            kwargs["min_value"] = minimum
        if maximum is not None:
            kwargs["max_value"] = maximum

        return ExpectationConfiguration(
            "expect_column_value_lengths_to_be_between", ExpectationKwargs(kwargs)
        )

    def _create_set_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        """https://json-schema.org/understanding-json-schema/reference/generic.html#enumerated-values"""
        if JsonSchemaTypes.ENUM.value not in details.keys():
            return None
        enum = details.get("enum", None)
        if not isinstance(enum, list):
            return None

        kwargs = ExpectationKwargs(column=key, value_set=enum)
        return ExpectationConfiguration("expect_column_values_to_be_in_set", kwargs)

    def _create_regex_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        # TODO https://json-schema.org/understanding-json-schema/reference/regular_expressions.html
        raise NotImplementedError("regex are not yet implemented.")

    def _create_string_format_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        # TODO https://json-schema.org/understanding-json-schema/reference/string.html#format
        raise NotImplementedError("string format are not yet implemented.")

    def _create_array_expectation(
        self, key: str, details: dict
    ) -> Optional[ExpectationConfiguration]:
        # TODO Non tabular - how do we validate these? https://json-schema.org/understanding-json-schema/reference/array.html
        raise NotImplementedError("arrays are not yet implemented.")
