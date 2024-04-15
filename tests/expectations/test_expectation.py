from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Any, Dict, List

import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ColumnPairMapExpectation,
    MulticolumnMapExpectation,
    UnexpectedRowsExpectation,
    _validate_dependencies_against_available_metrics,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.datasource.fluent.interfaces import Batch
    from great_expectations.datasource.fluent.sqlite_datasource import SqliteDatasource


class FakeMulticolumnExpectation(MulticolumnMapExpectation):
    map_metric = "fake_multicol_metric"


class FakeColumnMapExpectation(ColumnMapExpectation):
    map_metric = "fake_col_metric"


class FakeColumnPairMapExpectation(ColumnPairMapExpectation):
    map_metric = "fake_pair_metric"


@pytest.fixture
def metrics_dict():
    """
    Fixture for metrics dict, which represents Metrics already calculated for given Batch
    """
    return {
        (
            "column_values.nonnull.unexpected_count",
            "e197e9d84e4f8aa077b8dd5f9042b382",
            (),
        ): "i_exist"
    }


def fake_metrics_config_list(
    metric_name: str, metric_domain_kwargs: Dict[str, Any]
) -> List[MetricConfiguration]:
    """
    Helper method to generate list of MetricConfiguration objects for tests.
    """
    return [
        MetricConfiguration(
            metric_name=metric_name,
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs={},
        )
    ]


def fake_expectation_config(
    expectation_type: str, config_kwargs: Dict[str, Any]
) -> ExpectationConfiguration:
    """
    Helper method to generate of ExpectationConfiguration objects for tests.
    """
    return ExpectationConfiguration(
        expectation_type=expectation_type,
        kwargs=config_kwargs,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    [
        (
            FakeMulticolumnExpectation,
            fake_expectation_config("fake_multicolumn_expectation", {"column_list": []}),
        ),
        (
            FakeColumnMapExpectation,
            fake_expectation_config("fake_column_map_expectation", {"column": "col"}),
        ),
        (
            FakeColumnPairMapExpectation,
            fake_expectation_config(
                "fake_column_pair_map_expectation",
                {"column_A": "colA", "column_B": "colB"},
            ),
        ),
    ],
)
def test_multicolumn_expectation_has_default_mostly(fake_expectation_cls, config):
    try:
        fake_expectation = fake_expectation_cls(**config.kwargs)
    except Exception:
        assert False, "Validate configuration threw an error when testing default mostly value"
    assert (
        fake_expectation._get_success_kwargs().get("mostly") == 1
    ), "Default mostly success ratio is not 1"


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    itertools.chain(
        *[
            [
                (
                    FakeMulticolumnExpectation,
                    fake_expectation_config(
                        "fake_multicolumn_expectation", {"column_list": [], "mostly": x}
                    ),
                )
                for x in [0, 0.5, 1]
            ],
            [
                (
                    FakeColumnMapExpectation,
                    fake_expectation_config(
                        "fake_column_map_expectation", {"column": "col", "mostly": x}
                    ),
                )
                for x in [0, 0.5, 1]
            ],
            [
                (
                    FakeColumnPairMapExpectation,
                    fake_expectation_config(
                        "fake_column_pair_map_expectation",
                        {"column_A": "colA", "column_B": "colB", "mostly": x},
                    ),
                )
                for x in [0, 0.5, 1]
            ],
        ]
    ),
)
def test_expectation_succeeds_with_valid_mostly(fake_expectation_cls, config):
    fake_expectation = fake_expectation_cls(**config.kwargs)
    assert (
        fake_expectation._get_success_kwargs().get("mostly") == config.kwargs["mostly"]
    ), "Default mostly success ratio is not 1"


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    [
        (
            FakeMulticolumnExpectation,
            fake_expectation_config(
                "fake_multicolumn_expectation", {"column_list": [], "mostly": -0.5}
            ),
        ),
        (
            FakeColumnMapExpectation,
            fake_expectation_config(
                "fake_column_map_expectation", {"column": "col", "mostly": 1.5}
            ),
        ),
        (
            FakeColumnPairMapExpectation,
            fake_expectation_config(
                "fake_column_pair_map_expectation",
                {"column_A": "colA", "column_B": "colB", "mostly": -1},
            ),
        ),
    ],
)
def test_multicolumn_expectation_validation_errors_with_bad_mostly(fake_expectation_cls, config):
    with pytest.raises(pydantic.ValidationError):
        fake_expectation_cls(**config)


@pytest.mark.unit
def test_validate_dependencies_against_available_metrics_success(metrics_dict):
    metric_config_list: List[MetricConfiguration] = fake_metrics_config_list(
        metric_name="column_values.nonnull.unexpected_count",
        metric_domain_kwargs={
            "batch_id": "projects-projects",
            "column": "i_exist",
        },
    )
    _validate_dependencies_against_available_metrics(
        validation_dependencies=metric_config_list,
        metrics=metrics_dict,
    )


@pytest.mark.unit
def test_validate_dependencies_against_available_metrics_failure(metrics_dict):
    metric_config_list: List[MetricConfiguration] = fake_metrics_config_list(
        metric_name="column_values.nonnull.unexpected_count",
        metric_domain_kwargs={
            "batch_id": "projects-projects",
            "column": "i_dont_exist",
        },
    )
    with pytest.raises(InvalidExpectationConfigurationError):
        _validate_dependencies_against_available_metrics(
            validation_dependencies=metric_config_list,
            metrics=metrics_dict,
        )


@pytest.mark.unit
def test_expectation_configuration_property():
    expectation = gxe.ExpectColumnMaxToBeBetween(column="foo", min_value=0, max_value=10)

    assert expectation.configuration == ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={
            "column": "foo",
            "min_value": 0,
            "max_value": 10,
        },
    )


@pytest.mark.unit
def test_expectation_configuration_property_recognizes_state_changes():
    expectation = gxe.ExpectColumnMaxToBeBetween(column="foo", min_value=0, max_value=10)

    expectation.column = "bar"
    expectation.min_value = 5
    expectation.max_value = 15
    expectation.mostly = 0.95

    assert expectation.configuration == ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={
            "column": "bar",
            "mostly": 0.95,
            "min_value": 5,
            "max_value": 15,
        },
    )


@pytest.mark.unit
def test_unrecognized_expectation_arg_raises_error():
    with pytest.raises(pydantic.ValidationError, match="extra fields not permitted"):
        gxe.ExpectColumnMaxToBeBetween(
            column="foo",
            min_value=0,
            max_value=10,
            mostyl=0.95,  # 'mostly' typo
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT * FROM table", id="no batch"),
        pytest.param("SELECT * FROM {{ batch }}", id="invalid format"),
        pytest.param("SELECT * FROM {active_batch}", id="legacy syntax"),
    ],
)
def test_unexpected_rows_expectation_invalid_query_raises_error(query: str):
    with pytest.raises(pydantic.ValidationError):
        UnexpectedRowsExpectation(unexpected_rows_query=query)


@pytest.fixture
def taxi_db_path() -> str:
    return file_relative_path(__file__, "../test_sets/quickstart/yellow_tripdata.db")


@pytest.fixture
def sqlite_datasource(
    in_memory_runtime_context: AbstractDataContext, taxi_db_path: str
) -> SqliteDatasource:
    context = in_memory_runtime_context
    datasource_name = "my_sqlite_datasource"
    return context.sources.add_sqlite(
        datasource_name, connection_string=f"sqlite:///{taxi_db_path}"
    )


@pytest.fixture
def sqlite_batch(sqlite_datasource: SqliteDatasource) -> Batch:
    datasource = sqlite_datasource
    asset = datasource.add_table_asset("yellow_tripdata_sample_2022_01")

    batch_request = asset.build_batch_request()
    return asset.get_batch_list_from_batch_request(batch_request)[0]


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "query, expected_success, expected_observed_value, expected_unexpected_rows",
    [
        pytest.param(
            "SELECT * FROM {batch} WHERE passenger_count > 7",
            True,
            0,
            [],
            id="success",
        ),
        pytest.param(
            # There is a single instance where passenger_count == 7
            "SELECT * FROM {batch} WHERE passenger_count > 6",
            False,
            1,
            [
                {
                    "?": 48422,
                    "DOLocationID": 132,
                    "PULocationID": 132,
                    "RatecodeID": 5.0,
                    "VendorID": 2,
                    "airport_fee": 0.0,
                    "congestion_surcharge": 0.0,
                    "extra": 0.0,
                    "fare_amount": 70.0,
                    "improvement_surcharge": 0.3,
                    "mta_tax": 0.0,
                    "passenger_count": 7.0,
                    "payment_type": 1,
                    "store_and_fwd_flag": "N",
                    "tip_amount": 21.09,
                    "tolls_amount": 0.0,
                    "total_amount": 91.39,
                    "tpep_dropoff_datetime": "2022-01-01 19:20:46",
                    "tpep_pickup_datetime": "2022-01-01 19:20:43",
                    "trip_distance": 0.0,
                }
            ],
            id="failure",
        ),
    ],
)
def test_unexpected_rows_expectation_validate(
    sqlite_batch: Batch,
    query: str,
    expected_success: bool,
    expected_observed_value: int,
    expected_unexpected_rows: list[dict],
):
    batch = sqlite_batch

    expectation = UnexpectedRowsExpectation(unexpected_rows_query=query)
    result = batch.validate(expectation)

    assert result.success is expected_success

    res = result.result
    assert res["observed_value"] == expected_observed_value

    unexpected_rows = res["details"]["unexpected_rows"]
    assert unexpected_rows == expected_unexpected_rows


class TestSuiteParameterOptions:
    """Tests around the suite_parameter_options property of Expectations.

    Note: evaluation_parameter_options is currently a sorted tuple, but doesn't necessarily have to be
    """  # noqa: E501

    SUITE_PARAMETER_MIN = "my_min"
    SUITE_PARAMETER_MAX = "my_max"
    SUITE_PARAMETER_VALUE = "my_value"

    @pytest.mark.unit
    def test_expectation_without_evaluation_parameter(self):
        expectation = gxe.ExpectColumnValuesToBeBetween(column="foo", min_value=0, max_value=10)
        assert expectation.suite_parameter_options == tuple()

    @pytest.mark.unit
    def test_expectation_with_evaluation_parameter(self):
        expectation = gxe.ExpectColumnValuesToBeBetween(
            column="foo",
            min_value=0,
            max_value={"$PARAMETER": self.SUITE_PARAMETER_MAX},
        )
        assert expectation.suite_parameter_options == (self.SUITE_PARAMETER_MAX,)

    @pytest.mark.unit
    def test_expectation_with_multiple_suite_parameters(self):
        expectation = gxe.ExpectColumnValuesToBeBetween(
            column="foo",
            min_value={"$PARAMETER": self.SUITE_PARAMETER_MIN},
            max_value={"$PARAMETER": self.SUITE_PARAMETER_MAX},
        )
        assert expectation.suite_parameter_options == (
            self.SUITE_PARAMETER_MAX,
            self.SUITE_PARAMETER_MIN,
        )

    @pytest.mark.unit
    def test_expectation_with_duplicate_suite_parameters(self):
        expectation = gxe.ExpectColumnValuesToBeBetween(
            column="foo",
            min_value={"$PARAMETER": self.SUITE_PARAMETER_VALUE},
            max_value={"$PARAMETER": self.SUITE_PARAMETER_VALUE},
        )
        assert expectation.suite_parameter_options == (self.SUITE_PARAMETER_VALUE,)
