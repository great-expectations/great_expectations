from __future__ import annotations

import pytest

from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


@pytest.fixture
def config1():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def config2():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def config3():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config4():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config5():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2],  # differs from others
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config6():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3, 4],  # differs from others
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config7():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3, 4],
        },  # differs from others
        meta={"notes": "This is another expectation."},
    )


@pytest.mark.unit
def test_expectation_configuration_get_suite_parameter_dependencies():
    # Getting evaluation parameter dependencies relies on pyparsing, but the expectation
    # configuration is responsible for ensuring that it only returns one copy of required metrics.

    # If different expectations rely on the same upstream dependency,then it is possible for duplicates  # noqa: E501
    # to be present nonetheless
    ec = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "norm",
            "min_value": {
                "$PARAMETER": "(-3 * urn:great_expectations:validations:profile:expect_column_stdev_to_be_between"  # noqa: E501
                ".result.observed_value:column=norm) + "
                "urn:great_expectations:validations:profile:expect_column_mean_to_be_between.result.observed_value"
                ":column=norm"
            },
            "max_value": {
                "$PARAMETER": "(3 * urn:great_expectations:validations:profile:expect_column_stdev_to_be_between"  # noqa: E501
                ".result.observed_value:column=norm) + "
                "urn:great_expectations:validations:profile:expect_column_mean_to_be_between.result.observed_value"
                ":column=norm"
            },
        },
    )

    dependencies = ec.get_suite_parameter_dependencies()
    dependencies["profile"][0]["metric_kwargs_id"]["column=norm"] = set(
        dependencies["profile"][0]["metric_kwargs_id"]["column=norm"]
    )

    assert dependencies == {
        "profile": [
            {
                "metric_kwargs_id": {
                    "column=norm": {
                        "expect_column_stdev_to_be_between.result.observed_value",
                        "expect_column_mean_to_be_between.result.observed_value",
                    }
                }
            }
        ]
    }


@pytest.mark.unit
def test_expectation_configuration_get_suite_parameter_dependencies_with_query_store_formatted_urns():  # noqa: E501
    ec = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "genre_id",
            "value_set": {"$PARAMETER": "urn:great_expectations:stores:query_store:get_pet_names"},
            "result_format": "COMPLETE",
        },
    )

    # Should fully skip `nested_update` calls in method due to lacking an "expectation_suite_name" key  # noqa: E501
    dependencies = ec.get_suite_parameter_dependencies()
    assert dependencies == {}


@pytest.mark.parametrize(
    "notes",
    [
        pytest.param("my notes", id="string notes"),
        pytest.param(["my", "list", "of", "notes"], id="string notes"),
        pytest.param(None, id="no notes"),
    ],
)
@pytest.mark.unit
def test_expectation_configuration_to_domain_obj(notes: str | list[str] | None):
    expectation_type = "expect_column_values_to_be_in_set"
    column = "genre_id"
    value_set = {1, 2, 3}
    meta = {"foo": "bar"}

    config = ExpectationConfiguration(
        expectation_type=expectation_type,
        kwargs={"column": column, "value_set": value_set},
        notes=notes,
        meta=meta,
    )
    expectation = config.to_domain_obj()

    # Check that the expectation object has the same properties as the config
    assert expectation.expectation_type == expectation_type
    assert expectation.column == column
    assert expectation.value_set == value_set
    assert expectation.notes == notes
    assert expectation.meta == meta

    # Ensure that translation to/from config is consistent
    assert expectation.configuration == config
