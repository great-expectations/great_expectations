import logging
from typing import Dict, List, cast

import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationTestCase,
    ExpectationTestDataCases,
    TestBackend,
    TestData,
)
from great_expectations.core.expectation_diagnostics.supporting_types import (
    ExpectationExecutionEngineDiagnostics,
)
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.util import column_reflection_fallback
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.self_check.util import build_sa_validator_with_data
from great_expectations.self_check.util import (
    build_test_backends_list as build_test_backends_list_v3,
)
from great_expectations.self_check.util import (
    generate_expectation_tests,
    generate_test_table_name,
    should_we_generate_this_test,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sqlalchemy
    from sqlalchemy import create_engine

    # noinspection PyProtectedMember
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.sql import Select
except ImportError:
    sqlalchemy = None
    create_engine = None
    Engine = None
    Select = None
    SQLAlchemyError = None
    logger.debug("Unable to load SqlAlchemy or one of its subclasses.")


def get_table_columns_metric(engine: ExecutionEngine) -> [MetricConfiguration, dict]:
    resolved_metrics: dict = {}

    results: dict

    table_column_types_metric: MetricConfiguration = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs=dict(),
        metric_value_kwargs={
            "include_nested": True,
        },
        metric_dependencies=None,
    )
    results = engine.resolve_metrics(metrics_to_resolve=(table_column_types_metric,))
    resolved_metrics.update(results)

    table_columns_metric: MetricConfiguration = MetricConfiguration(
        metric_name="table.columns",
        metric_domain_kwargs=dict(),
        metric_value_kwargs=None,
        metric_dependencies={
            "table.column_types": table_column_types_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(table_columns_metric,), metrics=resolved_metrics
    )
    resolved_metrics.update(results)

    return table_columns_metric, resolved_metrics


@pytest.fixture(scope="module")
def expectation_and_runtime_configuration_with_evaluation_parameters():
    configuration = ExpectationConfiguration(
        expectation_type="expect_column_min_to_be_between",
        kwargs={
            "column": "live",
            "min_value": {"$PARAMETER": "MIN_VAL_PARAM"},
            "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
            "result_format": "SUMMARY",
        },
        meta={"BasicDatasetProfiler": {"confidence": "very low"}},
    )
    # runtime configuration with evaluation_parameters loaded
    runtime_configuration_with_eval = {
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"column": {"classes": ["badge", "badge-primary"]}},
        },
        "include_column_name": None,
        "evaluation_parameters": {"MIN_VAL_PARAM": 15, "MAX_VAL_PARAM": 20},
    }
    return configuration, runtime_configuration_with_eval


def test_prescriptive_renderer_no_decorator(
    expectation_and_runtime_configuration_with_evaluation_parameters,
):
    (
        configuration,
        runtime_configuration_with_eval,
    ) = expectation_and_runtime_configuration_with_evaluation_parameters

    # noinspection PyShadowingNames
    def bare_bones_prescriptive_renderer(
        configuration=None,
        runtime_configuration=None,
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = configuration.kwargs
        template_str = "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_eval,
    )
    assert len(res) == 1
    # string template should remain constant
    assert (
        res[0].string_template["template"]
        == "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
    )

    # params should contain our evaluation parameters
    assert res[0].string_template["params"]["min_value"] == {
        "$PARAMETER": "MIN_VAL_PARAM"
    }
    assert res[0].string_template["params"]["max_value"] == {
        "$PARAMETER": "MAX_VAL_PARAM"
    }

    # full json dict comparison
    assert res[0].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value",
            "params": {
                "column": "live",
                "min_value": {"$PARAMETER": "MIN_VAL_PARAM"},
                "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
                "result_format": "SUMMARY",
            },
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }


def test_prescriptive_renderer_with_decorator(
    expectation_and_runtime_configuration_with_evaluation_parameters,
):
    (
        configuration,
        runtime_configuration_with_eval,
    ) = expectation_and_runtime_configuration_with_evaluation_parameters

    # noinspection PyShadowingNames
    @render_evaluation_parameter_string
    def bare_bones_prescriptive_renderer(
        configuration=None,
        runtime_configuration=None,
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = configuration.kwargs
        template_str = "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_eval,
    )
    assert len(res) == 3

    # string template should remain constant
    assert (
        res[0].string_template["template"]
        == "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value"
    )

    # params should contain our evaluation parameters
    assert res[0].string_template["params"]["min_value"] == {
        "$PARAMETER": "MIN_VAL_PARAM"
    }
    assert res[0].string_template["params"]["max_value"] == {
        "$PARAMETER": "MAX_VAL_PARAM"
    }
    assert res[0].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "$column minimum value must be greater than or equal to $min_value and less than or equal to $max_value",
            "params": {
                "column": "live",
                "min_value": {"$PARAMETER": "MIN_VAL_PARAM"},
                "max_value": {"$PARAMETER": "MAX_VAL_PARAM"},
                "result_format": "SUMMARY",
            },
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }

    assert (
        res[1].string_template["template"]
        == "\n - $eval_param = $eval_param_value (at time of validation)."
    )
    # params should contain our evaluation parameters
    assert res[1].string_template["params"]["eval_param"] == "MIN_VAL_PARAM"
    assert res[1].string_template["params"]["eval_param_value"] == 15
    assert res[1].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "\n - $eval_param = $eval_param_value (at time of validation).",
            "params": {"eval_param": "MIN_VAL_PARAM", "eval_param_value": 15},
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }

    assert (
        res[2].string_template["template"]
        == "\n - $eval_param = $eval_param_value (at time of validation)."
    )
    # params should contain our evaluation parameters
    assert res[2].string_template["params"]["eval_param"] == "MAX_VAL_PARAM"
    assert res[2].string_template["params"]["eval_param_value"] == 20
    assert res[2].to_json_dict() == {
        "content_block_type": "string_template",
        "string_template": {
            "template": "\n - $eval_param = $eval_param_value (at time of validation).",
            "params": {"eval_param": "MAX_VAL_PARAM", "eval_param_value": 20},
            "styling": {
                "default": {"classes": ["badge", "badge-secondary"]},
                "params": {"column": {"classes": ["badge", "badge-primary"]}},
            },
        },
    }

    # with no runtime_configuration, throw an error
    with pytest.raises(GreatExpectationsError):
        # noinspection PyUnusedLocal
        res = bare_bones_prescriptive_renderer(
            configuration=configuration, runtime_configuration={}
        )

    # configuration should always be of ExpectationConfiguration-type
    with pytest.raises(AttributeError):
        # noinspection PyUnusedLocal,PyTypeChecker
        res = bare_bones_prescriptive_renderer(
            configuration={}, runtime_configuration={}
        )

    # extra evaluation parameters will not have an effect
    runtime_configuration_with_extra = {
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"column": {"classes": ["badge", "badge-primary"]}},
        },
        "include_column_name": None,
        "evaluation_parameters": {
            "MIN_VAL_PARAM": 15,
            "MAX_VAL_PARAM": 20,
            "IAMEXTRA": "EXTRA",
        },
    }

    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_extra,
    )
    assert len(res) == 3

    # missing evaluation_parameters will not render (MAX_VAL_PARAM is missing)
    runtime_configuration_with_missing = {
        "styling": {
            "default": {"classes": ["badge", "badge-secondary"]},
            "params": {"column": {"classes": ["badge", "badge-primary"]}},
        },
        "include_column_name": None,
        "evaluation_parameters": {"MIN_VAL_PARAM": 15},
    }
    res = bare_bones_prescriptive_renderer(
        configuration=configuration,
        runtime_configuration=runtime_configuration_with_missing,
    )
    assert len(res) == 2


# noinspection PyUnusedLocal
def test_table_column_reflection_fallback(test_backends, sa):
    include_sqlalchemy: bool = "sqlite" in test_backends
    include_postgresql: bool = "postgresql" in test_backends
    include_mysql: bool = "mysql" in test_backends
    include_mssql: bool = "mssql" in test_backends
    include_bigquery: bool = "bigquery" in test_backends
    include_trino: bool = "trino" in test_backends

    if not create_engine:
        pytest.skip("Unable to import sqlalchemy.create_engine() -- skipping.")

    test_backend_names: List[str] = build_test_backends_list_v3(
        include_pandas=False,
        include_spark=False,
        include_sqlalchemy=include_sqlalchemy,
        include_postgresql=include_postgresql,
        include_mysql=include_mysql,
        include_mssql=include_mssql,
        include_bigquery=include_bigquery,
        include_trino=include_trino,
    )

    df: pd.DataFrame = pd.DataFrame(
        {
            "name": ["Frank", "Steve", "Jane", "Frank", "Michael"],
            "age": [16, 21, 38, 22, 10],
            "pet": ["fish", "python", "cat", "python", "frog"],
        }
    )

    validators_config: Dict[str, Validator] = {}
    validator: Validator
    backend_name: str
    table_name: str
    for backend_name in test_backend_names:
        if backend_name in ["sqlite", "postgresql", "mysql", "mssql", "trino"]:
            table_name = generate_test_table_name()
            validator = build_sa_validator_with_data(
                df=df,
                sa_engine_name=backend_name,
                schemas=None,
                caching=True,
                table_name=table_name,
                sqlite_db_path=None,
            )
            if validator is not None:
                validators_config[table_name] = validator

    engine: Engine

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    reflected_columns_list: List[Dict[str, str]]
    reflected_column_config: Dict[str, str]
    column_name: str

    validation_result: ExpectationValidationResult

    sqlalchemy_engine: SqlAlchemyExecutionEngine

    for table_name, validator in validators_config.items():
        table_columns_metric, results = get_table_columns_metric(
            engine=validator.execution_engine
        )
        metrics.update(results)
        assert set(metrics[table_columns_metric.id]) == {"name", "age", "pet"}
        selectable: Select = sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(),
            schema=None,
        )
        sqlalchemy_engine = cast(SqlAlchemyExecutionEngine, validator.execution_engine)
        reflected_columns_list = column_reflection_fallback(
            selectable=selectable,
            dialect=sqlalchemy_engine.engine.dialect,
            sqlalchemy_engine=sqlalchemy_engine.engine,
        )
        for column_name in [
            reflected_column_config["name"]
            for reflected_column_config in reflected_columns_list
        ]:
            validation_result = validator.expect_column_to_exist(column=column_name)
            assert validation_result.success

    if validators_config:
        validator = list(validators_config.values())[0]

        validation_result = validator.expect_column_mean_to_be_between(
            column="age", min_value=10
        )
        assert validation_result.success

        validation_result = validator.expect_table_row_count_to_equal(value=5)
        assert validation_result.success

        validation_result = validator.expect_table_row_count_to_equal(value=3)
        assert not validation_result.success


@pytest.mark.skipif(
    sqlalchemy is None,
    reason="sqlalchemy is not installed",
)
def test__generate_expectation_tests__with_test_backends():
    expectation_type = "whatever"
    data = TestData(stuff=[1, 2, 3, 4, 5])
    test_case = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=[],
    )
    test_backends = [
        TestBackend(
            backend="sqlalchemy",
            dialects=["sqlite"],
        ),
    ]
    test_data_cases = [
        ExpectationTestDataCases(
            data=data,
            tests=[test_case],
            test_backends=test_backends,
        )
    ]
    engines = ExpectationExecutionEngineDiagnostics(
        PandasExecutionEngine=True,
        SqlAlchemyExecutionEngine=True,
        SparkDFExecutionEngine=False,
    )

    results = generate_expectation_tests(
        expectation_type=expectation_type,
        test_data_cases=test_data_cases,
        execution_engine_diagnostics=engines,
        raise_exceptions_for_backends=False,
    )
    backends_to_use = [r["backend"] for r in results]
    assert backends_to_use == ["sqlite"]


@pytest.mark.skipif(
    sqlalchemy is None,
    reason="sqlalchemy is not installed",
)
def test__generate_expectation_tests__with_test_backends2():
    expectation_type = "whatever"
    data = TestData(stuff=[1, 2, 3, 4, 5])
    test_case = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=[],
    )
    test_backends = [
        TestBackend(
            backend="sqlalchemy",
            dialects=["sqlite"],
        ),
        TestBackend(
            backend="pandas",
            dialects=None,
        ),
    ]
    test_data_cases = [
        ExpectationTestDataCases(
            data=data,
            tests=[test_case],
            test_backends=test_backends,
        )
    ]
    engines = ExpectationExecutionEngineDiagnostics(
        PandasExecutionEngine=True,
        SqlAlchemyExecutionEngine=True,
        SparkDFExecutionEngine=False,
    )

    results = generate_expectation_tests(
        expectation_type=expectation_type,
        test_data_cases=test_data_cases,
        execution_engine_diagnostics=engines,
        raise_exceptions_for_backends=False,
    )
    backends_to_use = [r["backend"] for r in results]
    assert sorted(backends_to_use) == ["pandas", "sqlite"]


@pytest.mark.skipif(
    sqlalchemy is None,
    reason="sqlalchemy is not installed",
)
def test__generate_expectation_tests__with_no_test_backends():
    expectation_type = "whatever"
    data = TestData(stuff=[1, 2, 3, 4, 5])
    test_case = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=[],
    )
    test_data_cases = [
        ExpectationTestDataCases(
            data=data,
            tests=[test_case],
        )
    ]
    engines = ExpectationExecutionEngineDiagnostics(
        PandasExecutionEngine=True,
        SqlAlchemyExecutionEngine=True,
        SparkDFExecutionEngine=False,
    )

    results = generate_expectation_tests(
        expectation_type=expectation_type,
        test_data_cases=test_data_cases,
        execution_engine_diagnostics=engines,
        raise_exceptions_for_backends=False,
    )
    backends_to_use = [r["backend"] for r in results]

    # If another SQL backend is available wherever this test is being run, it will
    # be included (i.e. postgresql)
    assert "pandas" in backends_to_use
    assert "sqlite" in backends_to_use
    assert "spark" not in backends_to_use


def test__TestBackend__bad_backends():
    with pytest.raises(AssertionError):
        TestBackend(
            backend="dogs",
            dialects=None,
        )


def test__TestBackend__bad_dialects():
    with pytest.raises(AssertionError):
        TestBackend(
            backend="sqlalchemy",
            dialects=None,
        )

    with pytest.raises(AssertionError):
        TestBackend(
            backend="sqlalchemy",
            dialects=[],
        )

    with pytest.raises(AssertionError):
        TestBackend(
            backend="sqlalchemy",
            dialects=["postgresql", "mysql", "ramen"],
        )

    with pytest.raises(AssertionError):
        TestBackend(
            backend="spark",
            dialects=["sqlite"],
        )

    with pytest.raises(AssertionError):
        TestBackend(
            backend="pandas",
            dialects=["sqlite"],
        )

    with pytest.raises(AssertionError):
        TestBackend(
            backend="sqlalchemy",
            dialects="sqlite",
        )

    TestBackend(
        backend="sqlalchemy",
        dialects=["sqlite"],
    )


def test__TestBackend__good_backends_and_dialects():
    tb1 = TestBackend(
        backend="pandas",
        dialects=None,
    )

    tb2 = TestBackend(
        backend="spark",
        dialects=None,
    )

    tb3 = TestBackend(
        backend="sqlalchemy",
        dialects=["sqlite", "postgresql", "mysql"],
    )


def test__should_we_generate_this_test__obvious():
    test_case = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=[],
    )
    backend = "spark"

    assert should_we_generate_this_test(backend, test_case) == True

    test_case2 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=["postgresql"],
    )
    backend2 = "sqlite"

    assert should_we_generate_this_test(backend2, test_case2) == False

    test_case3 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=["postgresql", "sqlite"],
    )
    backend3 = "sqlite"

    assert should_we_generate_this_test(backend3, test_case3) == True

    test_case4 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=["sqlite"],
        only_for=[],
    )
    backend4 = "sqlite"

    assert should_we_generate_this_test(backend4, test_case4) == False

    test_case5 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=["postgresql", "mssql"],
        only_for=["pandas", "sqlite"],
    )
    backend5 = "pandas"

    assert should_we_generate_this_test(backend5, test_case5) == True


def test__should_we_generate_this_test__sqlalchemy():
    test_case = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=["sqlalchemy"],
    )
    backend = "mysql"

    assert should_we_generate_this_test(backend, test_case) == True

    test_case2 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=["sqlalchemy"],
    )
    backend2 = "postgresql"

    assert should_we_generate_this_test(backend2, test_case2) == True

    test_case3 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=["mysql"],
        only_for=["sqlalchemy"],
    )
    backend3 = "mysql"

    assert should_we_generate_this_test(backend3, test_case3) == False

    test_case4 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=["sqlalchemy"],
        only_for=[],
    )
    backend4 = "sqlite"

    assert should_we_generate_this_test(backend4, test_case4) == False

    test_case5 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=["sqlalchemy"],
        only_for=[],
    )
    backend5 = "spark"

    assert should_we_generate_this_test(backend5, test_case5) == True


def test__should_we_generate_this_test__pandas():
    """
    Our CI/CD runs tests against pandas versions 0.23.4, 0.25.3, and latest (1.x currently)

    See: azure-pipelines.yml in project root
    """
    major, minor, *_ = pd.__version__.split(".")

    test_case = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=[],
    )
    backend = "pandas"

    assert should_we_generate_this_test(backend, test_case) == True

    test_case2 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=["pandas", "spark"],
    )
    backend2 = "pandas"

    assert should_we_generate_this_test(backend2, test_case2) == True

    test_case3 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=["pandas>=024"],
    )
    backend3 = "pandas"

    expected3 = False
    if (major == "0" and int(minor) >= 24) or int(major) >= 1:
        expected3 = True

    assert should_we_generate_this_test(backend3, test_case3) == expected3

    test_case4 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=[],
        only_for=["pandas_023"],
    )
    backend4 = "pandas"

    expected4 = False
    if major == "0" and minor == "23":
        expected4 = True

    assert should_we_generate_this_test(backend4, test_case4) == expected4

    test_case5 = ExpectationTestCase(
        title="",
        input={},
        output={},
        exact_match_out=False,
        include_in_gallery=False,
        suppress_test_for=["pandas"],
        only_for=[],
    )
    backend5 = "pandas"

    assert should_we_generate_this_test(backend5, test_case5) == False
