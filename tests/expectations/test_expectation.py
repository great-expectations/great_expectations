from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ColumnPairMapExpectation,
    Expectation,
    MulticolumnMapExpectation,
    _validate_dependencies_against_available_metrics,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.model_field_types import (
    MostlyField,  # noqa: TC001  # type needed in pydantic validation
    ValueSetField,  # noqa: TC001  # type needed in pydantic validation
)
from great_expectations.expectations.row_conditions import (
    AndCondition,
    Column,
    ComparisonCondition,
    Operator,
    PassThroughCondition,
)
from great_expectations.expectations.window import Offset, Window
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import ExpectationValidationResult
    from great_expectations.execution_engine.execution_engine import ExecutionEngine
    from great_expectations.expectations.conditions import Condition
    from great_expectations.expectations.model_field_types import ConditionParser

LOGGER = logging.getLogger(__name__)


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
        type=expectation_type,
        kwargs=config_kwargs,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    [
        (
            FakeMulticolumnExpectation,
            fake_expectation_config(
                "fake_multicolumn_expectation", {"column_list": ["column_1", "column_2"]}
            ),
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
    assert fake_expectation._get_success_kwargs().get("mostly") == 1, (
        "Default mostly success ratio is not 1"
    )


_VALID_MOSTLY_CASES: List[List[tuple[type[Expectation], ExpectationConfiguration]]] = [
    [
        (
            FakeMulticolumnExpectation,
            fake_expectation_config(
                "fake_multicolumn_expectation",
                {"column_list": ["column_1", "column_2"], "mostly": x},
            ),
        )
        for x in [0, 0.5, 1]
    ],
    [
        (
            FakeColumnMapExpectation,
            fake_expectation_config("fake_column_map_expectation", {"column": "col", "mostly": x}),
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


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    list(itertools.chain(*_VALID_MOSTLY_CASES)),
)
def test_expectation_succeeds_with_valid_mostly(fake_expectation_cls, config):
    fake_expectation = fake_expectation_cls(**config.kwargs)
    assert fake_expectation._get_success_kwargs().get("mostly") == config.kwargs["mostly"], (
        "Default mostly success ratio is not 1"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "fake_expectation_cls, config",
    [
        (
            FakeMulticolumnExpectation,
            fake_expectation_config(
                "fake_multicolumn_expectation",
                {"column_list": ["column_1", "column_2"], "mostly": -0.5},
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
        type="expect_column_max_to_be_between",
        kwargs={
            "column": "foo",
            "min_value": 0,
            "max_value": 10,
        },
    )


@pytest.mark.unit
def test_expectation_configuration_window():
    expectation = gxe.ExpectColumnMaxToBeBetween(
        column="foo",
        min_value=0,
        max_value=10,
        windows=[
            Window(
                constraint_fn="a",
                parameter_name="b",
                range=5,
                offset=Offset(positive=0.2, negative=0.2),
                strict=True,
            )
        ],
    )

    assert expectation.configuration == ExpectationConfiguration(
        type="expect_column_max_to_be_between",
        kwargs={
            "column": "foo",
            "min_value": 0,
            "max_value": 10,
            "windows": [
                {
                    "constraint_fn": "a",
                    "parameter_name": "b",
                    "range": 5,
                    "offset": {"positive": 0.2, "negative": 0.2},
                    "strict": True,
                }
            ],
        },
    )


@pytest.mark.unit
def test_expectation_configuration_window_empty():
    expectation = gxe.ExpectColumnMaxToBeBetween(
        column="foo",
        min_value=0,
        max_value=10,
        windows=None,
    )

    assert expectation.configuration == ExpectationConfiguration(
        type="expect_column_max_to_be_between",
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

    assert expectation.configuration == ExpectationConfiguration(
        type="expect_column_max_to_be_between",
        kwargs={
            "column": "bar",
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


class TestSuiteParameterOptions:
    """Tests around the suite_parameter_options property of Expectations.

    Note: evaluation_parameter_options is currently a sorted tuple, but doesn't necessarily have to be
    """  # noqa: E501 # FIXME CoP

    SUITE_PARAMETER_MIN = "my_min"
    SUITE_PARAMETER_MAX = "my_max"
    SUITE_PARAMETER_VALUE = "my_value"
    SUITE_PARAMETER_MOSTLY = "my_mostly"

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
    def test_column_map_expectation_with_evaluation_parameter(self):
        expectation = gxe.ExpectColumnValuesToBeNull(
            column="foo", mostly={"$PARAMETER": self.SUITE_PARAMETER_MOSTLY}
        )
        assert expectation.suite_parameter_options == (self.SUITE_PARAMETER_MOSTLY,)

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


@pytest.mark.unit
@pytest.mark.parametrize(
    "column_a,column_b,expected",
    [
        pytest.param("foo", "foo", True, id="equivalent_columns"),
        pytest.param("foo", "bar", False, id="different_columns"),
    ],
)
def test_expectation_equality(column_a: str, column_b: str, expected: bool):
    expectation_a = gxe.ExpectColumnValuesToBeBetween(column=column_a, min_value=0, max_value=10)
    expectation_b = gxe.ExpectColumnValuesToBeBetween(column=column_b, min_value=0, max_value=10)

    assert (expectation_a == expectation_b) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "notes_a,notes_b,expected",
    [
        pytest.param(None, None, True, id="both_none"),
        pytest.param([], None, True, id="both_falsy"),
        pytest.param("my_notes", None, False, id="missing_notes"),
        pytest.param("my_notes", "my_other_notes", False, id="different_notes"),
        pytest.param("my_notes", "my_notes", True, id="equivalent_notes"),
    ],
)
def test_expectation_equality_with_notes(
    notes_a: str | list[str] | None, notes_b: str | list[str] | None, expected: bool
):
    expectation_a = gxe.ExpectColumnValuesToBeBetween(
        column="foo", min_value=0, max_value=10, notes=notes_a
    )
    expectation_b = gxe.ExpectColumnValuesToBeBetween(
        column="foo", min_value=0, max_value=10, notes=notes_b
    )

    assert (expectation_a == expectation_b) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "meta_a,meta_b,expected",
    [
        pytest.param(None, None, True, id="both_none"),
        pytest.param({}, None, True, id="both_falsy"),
        pytest.param({"author": "Bob Dylan"}, None, False, id="missing_meta"),
        pytest.param(
            {"author": "Bob Dylan"}, {"author": "John Lennon"}, False, id="different_meta"
        ),
        pytest.param({"author": "Bob Dylan"}, {"author": "Bob Dylan"}, True, id="equivalent_meta"),
    ],
)
def test_expectation_equality_with_meta(meta_a: dict | None, meta_b: dict | None, expected: bool):
    expectation_a = gxe.ExpectColumnValuesToBeBetween(
        column="foo", min_value=0, max_value=10, meta=meta_a
    )
    expectation_b = gxe.ExpectColumnValuesToBeBetween(
        column="foo", min_value=0, max_value=10, meta=meta_b
    )

    assert (expectation_a == expectation_b) is expected


@pytest.mark.unit
def test_expectation_equality_ignores_rendered_content():
    column = "whatever"
    min_value = 0
    max_value = 10
    expectation_a = gxe.ExpectColumnValuesToBeBetween(
        column=column,
        min_value=min_value,
        max_value=max_value,
    )
    expectation_a.render()
    assert expectation_a.rendered_content

    expectation_b = gxe.ExpectColumnValuesToBeBetween(
        column=column,
        min_value=min_value,
        max_value=max_value,
    )
    expectation_b.rendered_content = None

    assert expectation_a == expectation_b


@pytest.mark.unit
def test_expectation_with_row_condition_generates_rendered_content():
    condition = ComparisonCondition(
        column=Column("status"), operator=Operator.EQUAL, parameter="active"
    )
    condition_2 = Column("age") > 18
    group_condition = AndCondition(conditions=[condition, condition_2])
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="foo",
        min_value=0,
        max_value=10,
        row_condition=group_condition,
    )
    expectation.render()
    assert expectation.rendered_content is not None
    assert expectation.row_condition == group_condition


@pytest.mark.unit
@pytest.mark.parametrize(
    "expectation_a, expectation_b, expected_result",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeBetween(column="foo", min_value=0),
            {},
            False,
            id="different_objects",
        ),
        pytest.param(
            gxe.ExpectColumnDistinctValuesToBeInSet(column="bar", value_set=[1, 2, 3]),
            gxe.ExpectColumnValuesToBeBetween(column="foo", min_value=0),
            True,
            id="different_expectation_types",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeBetween(column="foo", min_value=0),
            gxe.ExpectColumnValuesToBeBetween(column="foo", min_value=0),
            False,
            id="equivalent_expectations",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeBetween(
                column="foo", min_value=0, id="bbbe648e-0a43-431b-81a0-04e68f1473ae"
            ),
            gxe.ExpectColumnValuesToBeBetween(
                column="foo", min_value=0, id="aaae648e-0a43-431b-81a0-04e68f1473ae"
            ),
            False,
            id="equiv_expectations_with_ids",
        ),
    ],
)
def test_expectations___lt__(expectation_a, expectation_b, expected_result):
    assert (expectation_a < expectation_b) is expected_result


@pytest.mark.unit
def test_expectation_sorting():
    expectation_a = gxe.ExpectColumnValuesToBeBetween(
        column="foo", min_value=0, id="80b6d508-a843-426e-97c0-7ff64d35ac04"
    )
    expectation_b = gxe.ExpectColumnValuesToBeBetween(
        column="foo", min_value=0, id="4cd1e63a-880b-46ea-93e8-c11636df18b8"
    )
    expectation_c = gxe.ExpectTableColumnCountToBeBetween()
    expectation_d = gxe.ExpectColumnMaxToBeBetween(column="foo", min_value=0, max_value=10)
    expectation_e = gxe.ExpectColumnMedianToBeBetween(column="foo", min_value=0, max_value=10)

    expectations = [expectation_a, expectation_b, expectation_c, expectation_d, expectation_e]

    assert sorted(expectations) == [
        expectation_d,
        expectation_e,
        expectation_b,
        expectation_a,
        expectation_c,
    ]


class _SampleExpectation(Expectation):
    mostly: MostlyField
    value_set: ValueSetField

    @override
    def _validate(
        self,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Union[ExpectationValidationResult, dict]:
        # just satisfying the abc
        return {}


class TestCustomAnnotatedFields:
    @pytest.mark.parametrize(
        "mostly",
        [
            0,
            0.0,
            0.5,
            0.55555,
            1,
            1.0,
        ],
    )
    @pytest.mark.unit
    def test_valid_mostly_values(self, mostly: float):
        expectation = _SampleExpectation(mostly=mostly, value_set=[1, 2, 3])
        assert expectation.mostly == mostly

    @pytest.mark.parametrize(
        "mostly",
        [
            None,
            "one",
            -1,
            2,
        ],
    )
    @pytest.mark.unit
    def test_invalid_mostly_values(self, mostly: Any):
        with pytest.raises(pydantic.ValidationError):
            _SampleExpectation(mostly=mostly, value_set=[1, 2, 3])

    @pytest.mark.parametrize(
        "value_set,expected_value",
        [
            (["a"], ["a"]),
            ([1], [1]),
            ({"a"}, ["a"]),
            ({1}, [1]),
            ([1, 2, 3], [1, 2, 3]),
            (["a", "b", "c"], ["a", "b", "c"]),
            ({"$PARAMETER": "my_param"}, {"$PARAMETER": "my_param"}),
        ],
    )
    @pytest.mark.unit
    def test_valid_value_set_values(self, value_set: Union[Sequence, set], expected_value: Any):
        # ValueSetField declares list | dict, but its validator also normalizes other
        # iterables (set, tuple) to a list -- which is exactly what this test exercises.
        expectation = _SampleExpectation(mostly=1, value_set=value_set)  # type: ignore[arg-type]
        assert expectation.value_set == expected_value

    @pytest.mark.parametrize(
        "input_value,expected_type",
        [
            ({"1", "2", "3"}, list),
            (("1", "2", "3"), list),
            (["1", "2", "3"], list),
            ({"$PARAMETER": "my_param"}, dict),
        ],
    )
    @pytest.mark.unit
    def test_value_set_field_converts_to_list(self, input_value: Any, expected_type: type) -> None:
        expectation = _SampleExpectation(mostly=1, value_set=input_value)
        assert isinstance(expectation.value_set, expected_type)


class TestExpectationHash:
    @pytest.mark.unit
    def test_hash_consistency_with_equality(self):
        expectation1 = gxe.ExpectColumnValuesToNotBeNull(
            column="test_column", mostly=0.95, meta={"test": "value"}, notes="test notes"
        )
        expectation2 = gxe.ExpectColumnValuesToNotBeNull(
            column="test_column", mostly=0.95, meta={"test": "value"}, notes="test notes"
        )

        assert expectation1 == expectation2
        assert hash(expectation1) == hash(expectation2)

    @pytest.mark.unit
    def test_hash_different_for_different_columns(self):
        expectation1 = gxe.ExpectColumnValuesToNotBeNull(column="test_column_1")
        expectation2 = gxe.ExpectColumnValuesToNotBeNull(column="test_column_2")

        assert expectation1 != expectation2
        assert hash(expectation1) != hash(expectation2)

    @pytest.mark.unit
    def test_hash_different_for_different_mostly(self):
        expectation1 = gxe.ExpectColumnValuesToNotBeNull(column="test_column", mostly=0.95)
        expectation2 = gxe.ExpectColumnValuesToNotBeNull(column="test_column", mostly=0.90)

        assert expectation1 != expectation2
        assert hash(expectation1) != hash(expectation2)

    @pytest.mark.unit
    def test_hash_different_for_different_meta(self):
        expectation1 = gxe.ExpectColumnValuesToNotBeNull(
            column="test_column", meta={"test": "value1"}
        )
        expectation2 = gxe.ExpectColumnValuesToNotBeNull(
            column="test_column", meta={"test": "value2"}
        )

        assert expectation1 != expectation2
        assert hash(expectation1) != hash(expectation2)

    @pytest.mark.unit
    def test_hash_excludes_rendered_content(self):
        expectation1 = gxe.ExpectColumnValuesToNotBeNull(column="test_column")
        expectation2 = gxe.ExpectColumnValuesToNotBeNull(column="test_column")

        expectation1.render()

        assert expectation1 == expectation2
        assert hash(expectation1) == hash(expectation2)

    @pytest.mark.unit
    def test_hash_stable_across_runs(self):
        expectation = gxe.ExpectColumnValuesToNotBeNull(
            column="test_column", mostly=0.95, meta={"test": "value"}, notes="test notes"
        )

        hash1 = hash(expectation)
        hash2 = hash(expectation)
        hash3 = hash(expectation)

        assert hash1 == hash2 == hash3


@pytest.mark.unit
class TestLegacyRowConditionTransformation:
    """Test that legacy string row_conditions are transformed to Condition objects."""

    @pytest.mark.parametrize(
        "row_condition,expected_condition,condition_parser",
        [
            pytest.param(
                'col("age") > 18',
                Column("age") > 18,
                "great_expectations",
                id="numeric_comparison_greater_than",
            ),
            pytest.param(
                'col("price") <= 99.99',
                Column("price") <= 99.99,
                "great_expectations",
                id="numeric_comparison_less_than_or_equal",
            ),
            pytest.param(
                'col("status") == "active"',
                Column("status") == "active",
                "great_expectations",
                id="string_comparison_equal",
            ),
            pytest.param(
                'col("email").notnull()',
                Column("email").is_not_null(),
                "great_expectations",
                id="nullity_check_notnull",
            ),
            pytest.param(
                'col("age") >= 21',
                Column("age") >= 21,
                "great_expectations__experimental__",
                id="deprecated_parser_name",
            ),
            pytest.param(
                'col("temperature") < -10',
                Column("temperature") < -10,
                "great_expectations",
                id="negative_number_comparison",
            ),
            pytest.param(
                'col("balance") >= -99.50',
                Column("balance") >= -99.50,
                "great_expectations",
                id="negative_float_comparison",
            ),
        ],
    )
    def test_legacy_row_condition_transformation(
        self,
        row_condition: str,
        expected_condition: Condition,
        condition_parser: ConditionParser,
    ):
        """Test that legacy row_condition strings are transformed to Condition objects."""
        expectation = gxe.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["active", "pending"],
            row_condition=row_condition,
            condition_parser=condition_parser,
        )

        assert expectation.row_condition == expected_condition
        assert expectation.condition_parser == condition_parser

    def test_no_transformation_when_already_condition_object(self):
        original_condition = Column("age") > 18

        expectation = gxe.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["active"],
            row_condition=original_condition,
        )

        assert expectation.row_condition is original_condition
        assert isinstance(expectation.row_condition, ComparisonCondition)

    def test_no_transformation_when_row_condition_is_none(self):
        expectation = gxe.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["active"],
            row_condition=None,
            condition_parser="great_expectations",
        )

        assert expectation.row_condition is None
        assert expectation.condition_parser == "great_expectations"

    def test_pandas_parser_transformation_to_pass_through_condition(self):
        expectation = gxe.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["active"],
            row_condition='PClass=="1st"',
            condition_parser="pandas",
        )

        assert expectation.row_condition == PassThroughCondition(
            pass_through_filter='PClass=="1st"'
        )

    def test_null_condition_parser_uses_pass_through(self):
        expectation = gxe.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["active"],
            row_condition='PClass=="1st"',
            condition_parser=None,
        )

        assert expectation.row_condition == PassThroughCondition(
            pass_through_filter='PClass=="1st"'
        )

    def test_condition_parser_deprecation_warning(self):
        """Test that using condition_parser raises a DeprecationWarning."""
        with pytest.warns(DeprecationWarning):
            gxe.ExpectColumnValuesToBeInSet(
                column="status",
                value_set=["active"],
                row_condition='col("age") > 18',
                condition_parser="great_expectations",
            )

    def test_string_row_condition_deprecation_warning(self):
        """Test that passing a string to row_condition raises a DeprecationWarning."""
        with pytest.warns(DeprecationWarning):
            gxe.ExpectColumnValuesToBeInSet(
                column="status",
                value_set=["active"],
                row_condition='col("age") > 18',
                condition_parser="great_expectations",
            )

    def test_both_deprecation_warnings_triggered_together(self):
        """Test that both warnings are raised when both deprecated features are used."""
        with pytest.warns(DeprecationWarning) as warning_list:
            gxe.ExpectColumnValuesToBeInSet(
                column="status",
                value_set=["active"],
                row_condition='col("age") > 18',
                condition_parser="great_expectations",
            )

        # Should have two warnings: one for condition_parser, one for string row_condition
        assert len(warning_list) == 2


@pytest.mark.unit
class TestGetSuccessKwarg:
    """Tests for the _get_success_kwarg() single-key accessor method."""

    @pytest.mark.parametrize(
        "key,expected_value",
        [
            pytest.param("column", "test_col", id="string_value"),
            pytest.param("column_index", 5, id="int_value"),
        ],
    )
    def test_returns_configured_value(self, key, expected_value):
        """Values in configuration.kwargs should be returned directly."""
        exp = gxe.ExpectColumnToExist(column="test_col", column_index=5)
        assert exp._get_success_kwarg(key) == expected_value

    @pytest.mark.parametrize(
        "key,default,expected_value",
        [
            pytest.param("table", None, None, id="non_field_key_default_none"),
            pytest.param("table", "fallback", "fallback", id="non_field_key_custom_default"),
            pytest.param("nonexistent_key", "custom", "custom", id="unknown_key_custom_default"),
        ],
    )
    def test_returns_default_and_warns_for_missing_non_field(
        self, key, default, expected_value, caplog
    ):
        """Keys that aren't Pydantic fields should return the provided default and warn."""
        exp = gxe.ExpectColumnToExist(column="test_col")
        with caplog.at_level(logging.WARNING, logger="great_expectations.expectations.expectation"):
            result = exp._get_success_kwarg(key, default=default)
        assert result == expected_value
        assert f'_get_success_kwarg called with key "{key}"' in caplog.text

    def test_returns_field_default_for_optional_field(self):
        """Optional Pydantic fields should return their field default."""
        exp = gxe.ExpectColumnToExist(column="test_col")
        # column_index is an optional field with default=None
        assert exp._get_success_kwarg("column_index") is None

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param("column", id="domain_key"),
            pytest.param("value_set", id="success_key"),
            pytest.param("mostly", id="success_key_with_default"),
        ],
    )
    def test_equivalent_to_get_success_kwargs_get(self, key):
        """_get_success_kwarg should return same values as _get_success_kwargs().get()."""
        exp = gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["a", "b"], mostly=0.95)
        assert exp._get_success_kwarg(key) == exp._get_success_kwargs().get(key)
