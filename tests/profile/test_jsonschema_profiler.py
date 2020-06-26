import jsonschema
import pytest

from great_expectations.core import ExpectationSuite
from great_expectations.profile.base import ProfilerTypeMapping
from great_expectations.profile.json_schema_profiler import JsonSchemaProfiler


@pytest.fixture
def simple_schema():
    return {
        "$id": "https://example.com/address.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {"first_name": {"type": "string"}, "age": {"type": "integer"},},
    }


@pytest.fixture
def complex_flat_schema():
    """This includes some descriptions."""
    return {
        "$id": "https://example.com/address.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "description": "An address",
        "type": "object",
        "properties": {
            "post-office-box": {"type": "string"},
            "street-name": {"type": "string"},
            "street-number": {
                "type": "integer",
                "description": "Only the address number.",
            },
            "locality": {"type": "string"},
            "region": {"type": "string"},
            "postal-code": {"type": "string"},
            "country-name": {"type": "string"},
        },
        "required": ["locality", "region", "country-name"],
    }


@pytest.fixture
def boolean_types_schema():
    return {
        "$id": "https://example.com/address.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {"active": {"type": "boolean"}},
    }


@pytest.fixture
def enum_types_schema():
    return {
        "$id": "https://example.com/address.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {"shirt-size": {"enum": ["XS", "S", "M", "XL", "XXL"]}},
    }


@pytest.fixture
def string_lengths_schema():
    """
    This fixture has various combinations string lengths.
    https://json-schema.org/understanding-json-schema/reference/string.html#length
    """
    return {
        "$id": "https://example.com/address.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "comments-no-constraints": {"type": "string"},
            "state-abbreviation-equal-min-max": {
                "type": "string",
                "minLength": 2,
                "maxLength": 2,
            },
            "ICD10-code-3-7": {"type": "string", "minLength": 3, "maxLength": 7},
            "name-no-max": {"type": "string", "minLength": 1},
            "password-max-33": {"type": "string", "maxLength": 33},
        },
    }


@pytest.fixture
def integer_ranges_schema():
    """
    This fixture has various combinations of integer ranges.
    https://json-schema.org/understanding-json-schema/reference/numeric.html#range
    """
    return {
        "$id": "https://example.com/address.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "description": "An address similar to http://microformats.org/wiki/h-card",
        "type": "object",
        "properties": {
            "favorite-number": {"type": "integer"},
            "age-0-130": {"type": "integer", "minimum": 0, "maximum": 130},
            "wheel-count-0-plus": {"type": "integer", "minimum": 0},
            "rpm-max-7000": {"type": "integer", "maximum": 7000},
            "lake-depth-max-minus-100": {"type": "integer", "maximum": -100},
            "floor-exclusive-min-0": {"type": "integer", "exclusiveMinimum": 0},
            "floor-exclusive-max-100": {"type": "integer", "exclusiveMaximum": 100},
            "gear-exclusive-0-6": {
                "type": "integer",
                "exclusiveMinimum": 0,
                "exclusiveMaximum": 6,
            },
        },
    }


@pytest.fixture
def number_ranges_schema():
    """
    This fixture has various combinations of number ranges.
    https://json-schema.org/understanding-json-schema/reference/numeric.html#range
    """
    return {
        "$id": "https://example.com/address.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "description": "An address similar to http://microformats.org/wiki/h-card",
        "type": "object",
        "properties": {
            "favorite-number": {"type": "number"},
            "age-0-130": {"type": "number", "minimum": 0.5, "maximum": 130.5},
            "wheel-count-0-plus": {"type": "number", "minimum": 0.5},
            "rpm-max-7000": {"type": "number", "maximum": 7000.5},
            "lake-depth-max-minus-100": {"type": "number", "maximum": -100.5},
            "floor-exclusive-min-0": {"type": "number", "exclusiveMinimum": 0.5},
            "floor-exclusive-max-100": {"type": "number", "exclusiveMaximum": 100.5},
            "gear-exclusive-0-6": {
                "type": "number",
                "exclusiveMinimum": 0.5,
                "exclusiveMaximum": 6.5,
            },
        },
    }


def test_instantiable():
    profiler = JsonSchemaProfiler()
    assert isinstance(profiler, JsonSchemaProfiler)


def test_validate_returns_true_on_valid_schema(simple_schema):
    profiler = JsonSchemaProfiler()
    assert profiler.validate(simple_schema) is True


def test_profile_raises_errors_on_bad_inputs():
    profiler = JsonSchemaProfiler()
    for bad in [1, 1.1, None, "junk"]:
        with pytest.raises(TypeError):
            profiler.profile(bad, "foo")


def test_profile_raises_error_on_missing_suite_name(simple_schema):
    profiler = JsonSchemaProfiler()
    with pytest.raises(ValueError) as e:
        profiler.profile(simple_schema, suite_name=None)
    message = str(e.value)
    assert "provide a suite name" in message


def test_profile_raises_error_on_schema_missing_top_level_type_key():
    profiler = JsonSchemaProfiler()
    schema = {"a_schema": "missing_type"}
    with pytest.raises(KeyError) as e:
        profiler.profile(schema, "suite")
    message = str(e.value)
    assert "This profiler requires a json schema with a top level `type` key" in message


def test_profile_raises_error_on_schema_with_top_level_type_other_than_object():
    profiler = JsonSchemaProfiler()
    schema = {"type": "array"}
    with pytest.raises(TypeError) as e:
        profiler.profile(schema, "suite")
    message = str(e.value)
    assert (
        "This profiler requires a json schema with a top level `type` of `object`"
        in message
    )


def test_profile_enum_with_bad_input_raises_schema_error(enum_types_schema):
    profiler = JsonSchemaProfiler()
    # mangle the enum list
    enum_types_schema["properties"]["shirt-size"]["enum"] = "foo"
    with pytest.raises(jsonschema.SchemaError):
        profiler.profile(enum_types_schema, "enums")


def test_profile_simple_schema(simple_schema):
    profiler = JsonSchemaProfiler()
    obs = profiler.profile(simple_schema, "simple_suite")
    assert isinstance(obs, ExpectationSuite)
    assert obs.expectation_suite_name == "simple_suite"
    assert [e.to_json_dict() for e in obs.expectations] == [
        {
            "kwargs": {"column": "first_name"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "first_name",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {"column": "age"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "age",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
    ]


def test_profile_boolean_schema(boolean_types_schema):
    profiler = JsonSchemaProfiler()
    obs = profiler.profile(boolean_types_schema, "bools")
    assert isinstance(obs, ExpectationSuite)
    assert obs.expectation_suite_name == "bools"
    assert [e.to_json_dict() for e in obs.expectations] == [
        {
            "meta": {},
            "kwargs": {"column": "active"},
            "expectation_type": "expect_column_to_exist",
        },
        {
            "meta": {},
            "kwargs": {
                "column": "active",
                "type_list": list(ProfilerTypeMapping.BOOLEAN_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
        },
        {
            "meta": {},
            "kwargs": {"column": "active", "value_set": [True, False]},
            "expectation_type": "expect_column_values_to_be_in_set",
        },
    ]


def test_profile_enum_schema(enum_types_schema):
    profiler = JsonSchemaProfiler()
    obs = profiler.profile(enum_types_schema, "enums")
    assert isinstance(obs, ExpectationSuite)
    assert obs.expectation_suite_name == "enums"
    assert [e.to_json_dict() for e in obs.expectations] == [
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "shirt-size"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "shirt-size",
                "value_set": ["XS", "S", "M", "XL", "XXL"],
            },
        },
    ]


def test_profile_string_lengths_schema(string_lengths_schema):
    profiler = JsonSchemaProfiler()
    obs = profiler.profile(string_lengths_schema, "lengths")
    assert isinstance(obs, ExpectationSuite)
    assert obs.expectation_suite_name == "lengths"
    assert [e.to_json_dict() for e in obs.expectations] == [
        {
            "kwargs": {"column": "comments-no-constraints"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "comments-no-constraints",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {"column": "state-abbreviation-equal-min-max"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "state-abbreviation-equal-min-max",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {"column": "state-abbreviation-equal-min-max", "value": 2},
            "expectation_type": "expect_column_value_lengths_to_equal",
            "meta": {},
        },
        {
            "kwargs": {"column": "ICD10-code-3-7"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "ICD10-code-3-7",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {"column": "ICD10-code-3-7", "min_value": 3, "max_value": 7},
            "expectation_type": "expect_column_value_lengths_to_be_between",
            "meta": {},
        },
        {
            "kwargs": {"column": "name-no-max"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "name-no-max",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {"column": "name-no-max", "min_value": 1},
            "expectation_type": "expect_column_value_lengths_to_be_between",
            "meta": {},
        },
        {
            "kwargs": {"column": "password-max-33"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "password-max-33",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {"column": "password-max-33", "max_value": 33},
            "expectation_type": "expect_column_value_lengths_to_be_between",
            "meta": {},
        },
    ]


def test_profile_integer_ranges_schema(integer_ranges_schema):
    profiler = JsonSchemaProfiler()
    obs = profiler.profile(integer_ranges_schema, "integer_ranges")
    assert isinstance(obs, ExpectationSuite)
    assert obs.expectation_suite_name == "integer_ranges"

    assert [e.to_json_dict() for e in obs.expectations] == [
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "favorite-number"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "favorite-number",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "age-0-130"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "age-0-130",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "age-0-130", "min_value": 0, "max_value": 130},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "wheel-count-0-plus"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "wheel-count-0-plus",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "wheel-count-0-plus", "min_value": 0},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "rpm-max-7000"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "rpm-max-7000",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "rpm-max-7000", "max_value": 7000},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "lake-depth-max-minus-100"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "lake-depth-max-minus-100",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "lake-depth-max-minus-100", "max_value": -100},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "floor-exclusive-min-0"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "floor-exclusive-min-0",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "floor-exclusive-min-0",
                "min_value": 0,
                "strict_min": True,
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "floor-exclusive-max-100"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "floor-exclusive-max-100",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "floor-exclusive-max-100",
                "max_value": 100,
                "strict_max": True,
            },
        },
        {
            "kwargs": {"column": "gear-exclusive-0-6"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "gear-exclusive-0-6",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "gear-exclusive-0-6",
                "min_value": 0,
                "strict_min": True,
                "max_value": 6,
                "strict_max": True,
            },
            "expectation_type": "expect_column_values_to_be_between",
            "meta": {},
        },
    ]


def test_profile_number_ranges_schema(number_ranges_schema):
    profiler = JsonSchemaProfiler()
    obs = profiler.profile(number_ranges_schema, "number_ranges")
    assert isinstance(obs, ExpectationSuite)
    assert obs.expectation_suite_name == "number_ranges"

    assert [e.to_json_dict() for e in obs.expectations] == [
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "favorite-number"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "favorite-number",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "age-0-130"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "age-0-130",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "age-0-130", "min_value": 0.5, "max_value": 130.5},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "wheel-count-0-plus"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "wheel-count-0-plus",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "wheel-count-0-plus", "min_value": 0.5},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "rpm-max-7000"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "rpm-max-7000",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "rpm-max-7000", "max_value": 7000.5},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "lake-depth-max-minus-100"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "lake-depth-max-minus-100",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "lake-depth-max-minus-100", "max_value": -100.5},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "floor-exclusive-min-0"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "floor-exclusive-min-0",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "floor-exclusive-min-0",
                "min_value": 0.5,
                "strict_min": True,
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "floor-exclusive-max-100"},
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "floor-exclusive-max-100",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
        },
        {
            "meta": {},
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "floor-exclusive-max-100",
                "max_value": 100.5,
                "strict_max": True,
            },
        },
        {
            "kwargs": {"column": "gear-exclusive-0-6"},
            "expectation_type": "expect_column_to_exist",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "gear-exclusive-0-6",
                "type_list": list(ProfilerTypeMapping.FLOAT_TYPE_NAMES),
            },
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "meta": {},
        },
        {
            "kwargs": {
                "column": "gear-exclusive-0-6",
                "min_value": 0.5,
                "strict_min": True,
                "max_value": 6.5,
                "strict_max": True,
            },
            "expectation_type": "expect_column_values_to_be_between",
            "meta": {},
        },
    ]


def test_has_profile_create_expectations_from_complex_schema(complex_flat_schema):
    profiler = JsonSchemaProfiler()
    obs = profiler.profile(complex_flat_schema, "complex")
    assert isinstance(obs, ExpectationSuite)
    assert obs.expectation_suite_name == "complex"
    assert obs.meta["notes"] == {
        "format": "markdown",
        "content": ["### Description:\nAn address"],
    }

    assert [e.to_json_dict() for e in obs.expectations] == [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "post-office-box"},
            "meta": {},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "post-office-box",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "meta": {},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "street-name"},
            "meta": {},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "street-name",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "meta": {},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "street-number"},
            "meta": {
                "notes": {
                    "format": "markdown",
                    "content": ["### Description:\nOnly the address number."],
                }
            },
        },
        {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "street-number",
                "type_list": list(ProfilerTypeMapping.INT_TYPE_NAMES),
            },
            "meta": {},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "locality"},
            "meta": {},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "locality",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "meta": {},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "region"},
            "meta": {},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "region",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "meta": {},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "postal-code"},
            "meta": {},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "postal-code",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "meta": {},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "country-name"},
            "meta": {},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "country-name",
                "type_list": list(ProfilerTypeMapping.STRING_TYPE_NAMES),
            },
            "meta": {},
        },
    ]
