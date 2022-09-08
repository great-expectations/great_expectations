import datetime
import itertools
from copy import copy, deepcopy
from typing import Any, Dict, List

import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations import __version__ as ge_version
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.util import filter_properties_dict


@pytest.fixture
def fake_expectation_suite_name() -> str:
    return "test_expectation_suite_name"


@pytest.fixture
def expect_column_values_to_be_in_set_col_a_with_meta() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def expect_column_values_to_be_in_set_col_a_with_meta_dict() -> dict:
    # Note, value_set is distinct to ensure this is treated as a different expectation
    return {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "a",
            "value_set": [1, 2, 3, 4, 5],
            "result_format": "BASIC",
        },
        "meta": {"notes": "This is an expectation."},
    }

class TestInitMethod:
    """Tests related to ExpectationSuite.__init__()"""

    @pytest.mark.unit
    def test_expectation_suite_init_defaults(self, fake_expectation_suite_name: str):
        suite = ExpectationSuite(expectation_suite_name=fake_expectation_suite_name)

        default_meta = {"great_expectations_version": ge_version}

        assert suite.expectation_suite_name == fake_expectation_suite_name
        assert suite._data_context is None
        assert suite.expectations == []
        assert suite.evaluation_parameters == {}
        assert suite.data_asset_type is None
        assert suite.execution_engine_type is None
        assert suite.meta == default_meta
        assert suite.ge_cloud_id is None


    @pytest.mark.unit
    def test_expectation_suite_init_overrides(
        self,
        fake_expectation_suite_name: str,
        expect_column_values_to_be_in_set_col_a_with_meta: ExpectationConfiguration,
    ):
        class DummyDataContext:
            pass

        class DummyExecutionEngine:
            pass

        dummy_data_context = DummyDataContext()
        test_evaluation_parameters = {"$PARAMETER": "test_evaluation_parameters"}
        test_data_asset_type = "test_data_asset_type"
        dummy_execution_engine_type = type(DummyExecutionEngine())
        default_meta = {"great_expectations_version": ge_version}
        test_meta_base = {"test_key": "test_value"}
        test_meta = {**default_meta, **test_meta_base}
        test_id = "test_id"

        suite = ExpectationSuite(
            expectation_suite_name=fake_expectation_suite_name,
            data_context=dummy_data_context,  # type: ignore[arg-type]
            expectations=[expect_column_values_to_be_in_set_col_a_with_meta],
            evaluation_parameters=test_evaluation_parameters,
            data_asset_type=test_data_asset_type,
            execution_engine_type=dummy_execution_engine_type,  # type: ignore[arg-type]
            meta=test_meta,
            ge_cloud_id=test_id,
        )
        assert suite.expectation_suite_name == fake_expectation_suite_name
        assert suite._data_context == dummy_data_context
        assert suite.expectations == [expect_column_values_to_be_in_set_col_a_with_meta]
        assert suite.evaluation_parameters == test_evaluation_parameters
        assert suite.data_asset_type == test_data_asset_type
        assert suite.execution_engine_type == dummy_execution_engine_type
        assert suite.meta == test_meta
        assert suite.ge_cloud_id == test_id


    @pytest.mark.unit
    def test_expectation_suite_init_overrides_expectations_dict_and_obj(
        self, fake_expectation_suite_name: str,
        expect_column_values_to_be_in_set_col_a_with_meta_dict: dict,
        expect_column_values_to_be_in_set_col_a_with_meta: ExpectationConfiguration,
    ):
        """What does this test and why?

        The expectations param of ExpectationSuite takes a list of ExpectationConfiguration or dicts and both can be provided at the same time. We need to make sure they both show up as expectation configurations in the instantiated ExpectationSuite.
        """

        test_expectations_input = [
            expect_column_values_to_be_in_set_col_a_with_meta_dict,
            expect_column_values_to_be_in_set_col_a_with_meta,
        ]

        suite = ExpectationSuite(
            expectation_suite_name=fake_expectation_suite_name,
            expectations=test_expectations_input,  # type: ignore[arg-type]
        )
        assert suite.expectation_suite_name == fake_expectation_suite_name

        test_expected_expectations = [
            ExpectationConfiguration(
                **expect_column_values_to_be_in_set_col_a_with_meta_dict
            ),
            expect_column_values_to_be_in_set_col_a_with_meta,
        ]
        assert len(suite.expectations) == 2
        assert suite.expectations == test_expected_expectations


    @pytest.mark.unit
    def test_expectation_suite_init_overrides_non_json_serializable_meta(
        self, fake_expectation_suite_name: str,
    ):
        """What does this test and why?

        meta field overrides need to be json serializable, if not we raise an exception.
        """

        class NotSerializable:
            def __dict__(self):
                raise NotImplementedError

        test_meta = {"this_is_not_json_serializable": NotSerializable()}

        with pytest.raises(InvalidExpectationConfigurationError):
            ExpectationSuite(
                expectation_suite_name=fake_expectation_suite_name,
                meta=test_meta,  # type: ignore[arg-type]
            )


class TestAddCitation:

    @pytest.fixture
    def empty_suite_with_meta(self, fake_expectation_suite_name: str) -> ExpectationSuite:
        return ExpectationSuite(
            expectation_suite_name=fake_expectation_suite_name,
            meta={"notes": "This is an expectation suite."},
        )

    @pytest.mark.unit
    def test_empty_suite_with_meta_fixture(self, empty_suite_with_meta: ExpectationSuite):
        assert "citations" not in empty_suite_with_meta.meta

    @pytest.mark.unit
    def test_add_citation_comment(self, empty_suite_with_meta: ExpectationSuite):
        empty_suite_with_meta.add_citation("hello!")
        assert empty_suite_with_meta.meta["citations"][0].get("comment") == "hello!"


    @pytest.mark.unit
    def test_add_citation_comment_required(self, empty_suite_with_meta: ExpectationSuite):
        with pytest.raises(TypeError) as e:
            empty_suite_with_meta.add_citation()  # type: ignore[call-arg]
        assert "add_citation() missing 1 required positional argument: 'comment'" in str(e.value)


    @pytest.mark.unit
    def test_add_citation_not_specified_params_filtered(self, empty_suite_with_meta: ExpectationSuite):
        empty_suite_with_meta.add_citation("fake_comment", batch_spec={"fake": "batch_spec"}, batch_markers={"fake": "batch_markers"})
        assert "citations" in empty_suite_with_meta.meta

        citation_keys = set(empty_suite_with_meta.meta["citations"][0].keys())
        # Note: citation_date is always added if not provided
        assert citation_keys == {"comment", 'citation_date', 'batch_spec', 'batch_markers',}
        # batch_definition (along with other keys that are not provided) should be filtered out
        assert "batch_definition" not in citation_keys


    @pytest.mark.unit
    @pytest.mark.v2_api
    def test_add_citation_accepts_v2_api_params(self, empty_suite_with_meta: ExpectationSuite):
        """This test ensures backward compatibility with the v2 api and can be removed when deprecated."""
        empty_suite_with_meta.add_citation("fake_comment", batch_kwargs={"fake": "batch_kwargs"})

        assert empty_suite_with_meta.meta["citations"][0]["batch_kwargs"] == {'fake': 'batch_kwargs'}
        citation_keys = set(empty_suite_with_meta.meta["citations"][0].keys())
        assert citation_keys == {'comment', 'citation_date', 'batch_kwargs'}

    @pytest.mark.unit
    def test_add_citation_all_params(self):
        raise NotImplementedError

    @pytest.mark.unit
    def test_add_citation_citation_date_str_override(self):
        raise NotImplementedError

    @pytest.mark.unit
    def test_add_citation_citation_date_datetime_override(self):
        raise NotImplementedError

    @pytest.mark.unit
    def test_add_citation_with_existing_citations(self):
        raise NotImplementedError

    @pytest.mark.unit
    def test_add_citation_with_profiler_config(self, baseline_suite, profiler_config):
        assert (
                "citations" not in baseline_suite.meta
                or len(baseline_suite.meta["citations"]) == 0
        )
        baseline_suite.add_citation(
            "adding profiler config citation",
            profiler_config=profiler_config,
        )
        assert baseline_suite.meta["citations"][0].get("profiler_config") == profiler_config

#### Below this line are mainly existing tests and fixtures that we are in the process of cleaning up


@pytest.fixture
def exp2() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp3() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp4() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def column_pair_expectation() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_pair_values_to_be_in_set",
        kwargs={
            "column_A": "1",
            "column_B": "b",
            "value_set": [(1, 1), (2, 2)],
            "result_format": "BASIC",
        },
    )


@pytest.fixture
def table_exp1() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_ordered_list",
        kwargs={"value": ["a", "b", "c"]},
    )


@pytest.fixture
def table_exp2() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 0, "max_value": 1},
    )


@pytest.fixture
def table_exp3() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal", kwargs={"value": 1}
    )


@pytest.fixture
def empty_suite(empty_data_context) -> ExpectationSuite:
    context: DataContext = empty_data_context
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[],
        meta={"notes": "This is an expectation suite."},
        data_context=context,
    )


@pytest.fixture
def single_expectation_suite(
    expect_column_values_to_be_in_set_col_a_with_meta, empty_data_context
) -> ExpectationSuite:
    context: DataContext = empty_data_context
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[expect_column_values_to_be_in_set_col_a_with_meta],
        meta={"notes": "This is an expectation suite."},
        data_context=context,
    )


@pytest.fixture
def suite_with_table_and_column_expectations(
    expect_column_values_to_be_in_set_col_a_with_meta,
    exp2,
    exp3,
    exp4,
    column_pair_expectation,
    table_exp1,
    table_exp2,
    table_exp3,
    empty_data_context,
) -> ExpectationSuite:
    context: DataContext = empty_data_context
    suite = ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[
            expect_column_values_to_be_in_set_col_a_with_meta,
            exp2,
            exp3,
            exp4,
            column_pair_expectation,
            table_exp1,
            table_exp2,
            table_exp3,
        ],
        meta={"notes": "This is an expectation suite."},
        data_context=context,
    )
    assert suite.expectations == [
        expect_column_values_to_be_in_set_col_a_with_meta,
        exp2,
        exp3,
        exp4,
        column_pair_expectation,
        table_exp1,
        table_exp2,
        table_exp3,
    ]
    return suite


@pytest.fixture
def baseline_suite(
    expect_column_values_to_be_in_set_col_a_with_meta, exp2, empty_data_context
) -> ExpectationSuite:
    context: DataContext = empty_data_context
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[expect_column_values_to_be_in_set_col_a_with_meta, exp2],
        meta={"notes": "This is an expectation suite."},
        data_context=context,
    )


@pytest.fixture
def identical_suite(
    expect_column_values_to_be_in_set_col_a_with_meta, exp3, empty_data_context
) -> ExpectationSuite:
    context: DataContext = empty_data_context
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[expect_column_values_to_be_in_set_col_a_with_meta, exp3],
        meta={"notes": "This is an expectation suite."},
        data_context=context,
    )


@pytest.fixture
def equivalent_suite(
    expect_column_values_to_be_in_set_col_a_with_meta, exp3, empty_data_context
) -> ExpectationSuite:
    context: DataContext = empty_data_context
    return ExpectationSuite(
        expectation_suite_name="danger",
        expectations=[expect_column_values_to_be_in_set_col_a_with_meta, exp3],
        meta={
            "notes": "This is another expectation suite, with a different name and meta"
        },
        data_context=context,
    )


@pytest.fixture
def different_suite(
    expect_column_values_to_be_in_set_col_a_with_meta, exp4, empty_data_context
) -> ExpectationSuite:
    context: DataContext = empty_data_context
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[expect_column_values_to_be_in_set_col_a_with_meta, exp4],
        meta={"notes": "This is an expectation suite."},
        data_context=context,
    )


@pytest.fixture
def profiler_config():
    # Profiler configuration is pulled from the Bobster use case in tests/rule_based_profiler/
    yaml_config = """
    # This profiler is meant to be used on the NYC taxi data:
    # tests/test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_20(18|19|20)-*.csv
    variables:
      # BatchRequest yielding thirty five (35) batches (January, 2018 -- November, 2020 trip data)
      jan_2018_thru_nov_2020_monthly_tripdata_batch_request:
        datasource_name: taxi_pandas
        data_connector_name: monthly
        data_asset_name: my_reports
        data_connector_query:
          index: ":-1"
      confidence_level: 9.5e-1
      mostly: 1.0

    rules:
      row_count_range_rule:
        domain_builder:
          class_name: TableDomainBuilder
        parameter_builders:
          - parameter_name: row_count_range
            class_name: NumericMetricRangeMultiBatchParameterBuilder
            metric_name: table.row_count
            confidence_level: $variables.confidence_level
            truncate_values:
              lower_bound: 0
            round_decimals: 0
        expectation_configuration_builders:
         - expectation_type: expect_table_row_count_to_be_between
           class_name: DefaultExpectationConfigurationBuilder
           module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
           min_value: $parameter.row_count_range.value.min_value
           max_value: $parameter.row_count_range.value.max_value
           mostly: $variables.mostly
           meta:
             profiler_details: $parameter.row_count_range.details
    """
    yaml = YAML()
    return yaml.load(yaml_config)


@pytest.mark.unit
def test_expectation_suite_equality(baseline_suite, identical_suite, equivalent_suite):
    """Equality should depend on all defined properties of a configuration object, but not on whether the *instances*
    are the same."""
    assert (
        baseline_suite is not identical_suite
    )  # different instances, but same content
    assert baseline_suite == identical_suite  # different instances, but same content
    assert not (baseline_suite != identical_suite)  # ne works properly
    assert not (baseline_suite == equivalent_suite)  # different meta
    assert baseline_suite != equivalent_suite  # ne works properly


@pytest.mark.unit
def test_expectation_suite_equivalence(
    baseline_suite,
    identical_suite,
    equivalent_suite,
    different_suite,
    single_expectation_suite,
):
    """Equivalence should depend only on properties that affect the result of the expectation."""
    assert baseline_suite.isEquivalentTo(baseline_suite)  # no difference
    assert baseline_suite.isEquivalentTo(identical_suite)
    assert baseline_suite.isEquivalentTo(equivalent_suite)  # different meta
    assert not baseline_suite.isEquivalentTo(
        different_suite
    )  # different value_set in one expectation
    assert not single_expectation_suite.isEquivalentTo(baseline_suite)


@pytest.mark.unit
def test_expectation_suite_dictionary_equivalence(baseline_suite: ExpectationSuite):

    baseline_suite_dict: dict = expectationSuiteSchema.dump(baseline_suite)

    assert baseline_suite.isEquivalentTo(baseline_suite_dict)

    modified_suite = deepcopy(baseline_suite_dict)
    modified_suite["expectations"][0]["kwargs"]["value_set"][0] = -1

    modified_suite_dict: dict = expectationSuiteSchema.dump(modified_suite)

    assert not baseline_suite.isEquivalentTo(modified_suite_dict)


@pytest.mark.unit
def test_expectation_suite_copy(baseline_suite):
    suite_copy = copy(baseline_suite)
    assert suite_copy == baseline_suite
    suite_copy.data_asset_type = "blarg!"
    assert (
        baseline_suite.data_asset_type != "blarg"
    )  # copy on primitive properties shouldn't propagate
    suite_copy.expectations[0].meta["notes"] = "a different note"
    assert (
        baseline_suite.expectations[0].meta["notes"] == "a different note"
    )  # copy on deep attributes does propagate


@pytest.mark.unit
def test_expectation_suite_deepcopy(baseline_suite):
    suite_deepcopy = deepcopy(baseline_suite)
    assert suite_deepcopy == baseline_suite
    suite_deepcopy.data_asset_type = "blarg!"
    assert (
        baseline_suite.data_asset_type != "blarg"
    )  # copy on primitive properties shouldn't propagate
    suite_deepcopy.expectations[0].meta["notes"] = "a different note"
    # deepcopy on deep attributes does not propagate
    assert baseline_suite.expectations[0].meta["notes"] == "This is an expectation."


@pytest.mark.unit
def test_suite_without_metadata_includes_ge_version_metadata_if_none_is_provided():
    suite = ExpectationSuite("foo")
    assert "great_expectations_version" in suite.meta.keys()


@pytest.mark.unit
def test_suite_does_not_overwrite_existing_version_metadata():
    suite = ExpectationSuite(
        "foo",
        meta={"great_expectations_version": "0.0.0"},
    )
    assert "great_expectations_version" in suite.meta.keys()
    assert suite.meta["great_expectations_version"] == "0.0.0"


@pytest.mark.unit
def test_suite_with_metadata_includes_ge_version_metadata(baseline_suite):
    assert "great_expectations_version" in baseline_suite.meta.keys()





@pytest.mark.unit
def test_get_citations_with_no_citations(baseline_suite):
    assert "citations" not in baseline_suite.meta
    assert baseline_suite.get_citations() == []


@pytest.mark.unit
def test_get_citations_not_sorted(baseline_suite):
    assert "citations" not in baseline_suite.meta

    baseline_suite.add_citation("first", citation_date="2000-01-01")
    baseline_suite.add_citation("third", citation_date="2000-01-03")
    baseline_suite.add_citation("second", citation_date="2000-01-02")
    properties_dict_list: List[Dict[str, Any]] = baseline_suite.get_citations(
        sort=False
    )
    for properties_dict in properties_dict_list:
        filter_properties_dict(
            properties=properties_dict, clean_falsy=True, inplace=True
        )
        properties_dict.pop("interactive", None)

    assert properties_dict_list == [
        {"citation_date": "2000-01-01T00:00:00.000000Z", "comment": "first"},
        {"citation_date": "2000-01-03T00:00:00.000000Z", "comment": "third"},
        {"citation_date": "2000-01-02T00:00:00.000000Z", "comment": "second"},
    ]


@pytest.mark.unit
def test_get_citations_sorted(baseline_suite):
    assert "citations" not in baseline_suite.meta

    dt: datetime.datetime

    baseline_suite.add_citation("first", citation_date="2000-01-01")
    baseline_suite.add_citation("third", citation_date="2000-01-03")
    baseline_suite.add_citation("second", citation_date="2000-01-02")
    properties_dict_list: List[Dict[str, Any]] = baseline_suite.get_citations(sort=True)
    for properties_dict in properties_dict_list:
        filter_properties_dict(
            properties=properties_dict, clean_falsy=True, inplace=True
        )
        properties_dict.pop("interactive", None)

    assert properties_dict_list == [
        {
            "citation_date": "2000-01-01T00:00:00.000000Z",
            "comment": "first",
        },
        {
            "citation_date": "2000-01-02T00:00:00.000000Z",
            "comment": "second",
        },
        {
            "citation_date": "2000-01-03T00:00:00.000000Z",
            "comment": "third",
        },
    ]


@pytest.mark.unit
def test_get_citations_with_multiple_citations_containing_batch_kwargs(baseline_suite):
    assert "citations" not in baseline_suite.meta

    baseline_suite.add_citation(
        "first", batch_kwargs={"path": "first"}, citation_date="2000-01-01"
    )
    baseline_suite.add_citation(
        "second", batch_kwargs={"path": "second"}, citation_date="2001-01-01"
    )
    baseline_suite.add_citation("third", citation_date="2002-01-01")

    properties_dict_list: List[Dict[str, Any]] = baseline_suite.get_citations(
        sort=True, require_batch_kwargs=True
    )
    for properties_dict in properties_dict_list:
        filter_properties_dict(
            properties=properties_dict, clean_falsy=True, inplace=True
        )
        properties_dict.pop("interactive", None)

    assert properties_dict_list == [
        {
            "citation_date": "2000-01-01T00:00:00.000000Z",
            "batch_kwargs": {"path": "first"},
            "comment": "first",
        },
        {
            "citation_date": "2001-01-01T00:00:00.000000Z",
            "batch_kwargs": {"path": "second"},
            "comment": "second",
        },
    ]


@pytest.mark.unit
def test_get_citations_with_multiple_citations_containing_profiler_config(
    baseline_suite, profiler_config
):
    assert "citations" not in baseline_suite.meta

    baseline_suite.add_citation(
        "first",
        citation_date="2000-01-01",
        profiler_config=profiler_config,
    )
    baseline_suite.add_citation(
        "second",
        citation_date="2001-01-01",
        profiler_config=profiler_config,
    )
    baseline_suite.add_citation("third", citation_date="2002-01-01")

    properties_dict_list: List[Dict[str, Any]] = baseline_suite.get_citations(
        sort=True, require_profiler_config=True
    )
    for properties_dict in properties_dict_list:
        filter_properties_dict(
            properties=properties_dict, clean_falsy=True, inplace=True
        )
        properties_dict.pop("interactive", None)

    assert properties_dict_list == [
        {
            "citation_date": "2000-01-01T00:00:00.000000Z",
            "profiler_config": profiler_config,
            "comment": "first",
        },
        {
            "citation_date": "2001-01-01T00:00:00.000000Z",
            "profiler_config": profiler_config,
            "comment": "second",
        },
    ]


@pytest.mark.unit
def test_get_table_expectations_returns_empty_list_on_empty_suite(empty_suite):
    assert empty_suite.get_table_expectations() == []


@pytest.mark.unit
def test_get_table_expectations_returns_empty_list_on_suite_without_any(baseline_suite):
    assert baseline_suite.get_table_expectations() == []


@pytest.mark.unit
def test_get_table_expectations(
    suite_with_table_and_column_expectations, table_exp1, table_exp2, table_exp3
):
    obs = suite_with_table_and_column_expectations.get_table_expectations()
    assert obs == [table_exp1, table_exp2, table_exp3]


@pytest.mark.unit
def test_get_column_expectations_returns_empty_list_on_empty_suite(empty_suite):
    assert empty_suite.get_column_expectations() == []


@pytest.mark.unit
def test_get_column_expectations(
    suite_with_table_and_column_expectations,
    expect_column_values_to_be_in_set_col_a_with_meta,
    exp2,
    exp3,
    exp4,
):
    obs = suite_with_table_and_column_expectations.get_column_expectations()
    assert obs == [expect_column_values_to_be_in_set_col_a_with_meta, exp2, exp3, exp4]


@pytest.mark.unit
def test_get_expectations_by_expectation_type(
    suite_with_table_and_column_expectations,
    expect_column_values_to_be_in_set_col_a_with_meta,
    exp2,
    exp3,
    exp4,
    column_pair_expectation,
    table_exp1,
    table_exp2,
    table_exp3,
):
    obs = (
        suite_with_table_and_column_expectations.get_grouped_and_ordered_expectations_by_expectation_type()
    )
    assert obs == [
        table_exp1,
        table_exp2,
        table_exp3,
        expect_column_values_to_be_in_set_col_a_with_meta,
        exp2,
        exp3,
        exp4,
        column_pair_expectation,
    ]


@pytest.mark.unit
def test_get_expectations_by_domain_type(
    suite_with_table_and_column_expectations,
    expect_column_values_to_be_in_set_col_a_with_meta,
    exp2,
    exp3,
    exp4,
    column_pair_expectation,
    table_exp1,
    table_exp2,
    table_exp3,
):
    obs = (
        suite_with_table_and_column_expectations.get_grouped_and_ordered_expectations_by_domain_type()
    )
    assert list(itertools.chain.from_iterable(obs.values())) == [
        table_exp1,
        table_exp2,
        table_exp3,
        expect_column_values_to_be_in_set_col_a_with_meta,
        exp2,
        exp3,
        exp4,
        column_pair_expectation,
    ]


class DataContextSendUsageMessageSpy:
    def __init__(self):
        self.messages = []

    def send_usage_message(
        self,
        event,
        event_payload,
        success,
    ):
        self.messages.append(
            {
                "event": event,
                "event_payload": event_payload,
                "success": success,
            }
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "success",
    [
        pytest.param(True, id="success=True"),
        pytest.param(False, id="success=False"),
    ],
)
def test_expectation_suite_send_usage_message(success: bool):
    """Ensure usage stats event is sent on expectation suite."""

    dc_message_spy = DataContextSendUsageMessageSpy()

    suite = ExpectationSuite(
        expectation_suite_name="suite_name",
        data_context=dc_message_spy,  # type: ignore[arg-type]
    )

    suite.send_usage_event(success=success)

    assert dc_message_spy.messages
    assert len(dc_message_spy.messages) == 1
    assert dc_message_spy.messages[0] == {
        "event": UsageStatsEvents.EXPECTATION_SUITE_ADD_EXPECTATION.value,
        "event_payload": {},
        "success": success,
    }
