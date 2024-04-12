from __future__ import annotations

import datetime
import itertools
from copy import copy, deepcopy
from typing import Any, Dict, List, Union
from unittest import mock
from unittest.mock import MagicMock, Mock  # noqa: TID251
from uuid import UUID, uuid4

import pytest

import great_expectations as gx
import great_expectations.exceptions.exceptions as gx_exceptions
import great_expectations.expectations as gxe
from great_expectations import __version__ as ge_version
from great_expectations import get_context, set_context
from great_expectations.analytics.events import (
    ExpectationSuiteExpectationCreatedEvent,
    ExpectationSuiteExpectationDeletedEvent,
    ExpectationSuiteExpectationUpdatedEvent,
)
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import AbstractDataContext
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import Expectation
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.util import filter_properties_dict


@pytest.fixture
def fake_expectation_suite_name() -> str:
    return "test_expectation_suite_name"


@pytest.fixture
def expect_column_values_to_be_in_set_col_a_with_meta() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3],
        },
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
        },
        "meta": {"notes": "This is an expectation."},
    }


@pytest.fixture
def empty_suite_with_meta(fake_expectation_suite_name: str) -> ExpectationSuite:
    return ExpectationSuite(
        name=fake_expectation_suite_name,
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def suite_with_single_expectation(
    expect_column_values_to_be_in_set_col_a_with_meta: ExpectationConfiguration,
    empty_suite_with_meta: ExpectationSuite,
) -> ExpectationSuite:
    empty_suite_with_meta.add_expectation_configuration(
        expect_column_values_to_be_in_set_col_a_with_meta
    )
    return empty_suite_with_meta


class TestInit:
    """Tests related to ExpectationSuite.__init__()"""

    @pytest.mark.unit
    def test_expectation_suite_init_defaults(self, fake_expectation_suite_name: str):
        suite = ExpectationSuite(name=fake_expectation_suite_name)

        default_meta = {"great_expectations_version": ge_version}

        assert suite.name == fake_expectation_suite_name
        assert suite.expectations == []
        assert suite.suite_parameters == {}
        assert suite.execution_engine_type is None
        assert suite.meta == default_meta
        assert suite.id is None

    @pytest.mark.unit
    def test_expectation_suite_init_overrides(
        self,
        fake_expectation_suite_name: str,
        expect_column_values_to_be_in_set_col_a_with_meta: ExpectationConfiguration,
    ):
        class DummyExecutionEngine:
            pass

        test_suite_parameters = {"$PARAMETER": "test_suite_parameters"}
        dummy_execution_engine_type = type(DummyExecutionEngine())
        default_meta = {"great_expectations_version": ge_version}
        test_meta_base = {"test_key": "test_value"}
        test_meta = {**default_meta, **test_meta_base}
        test_id = "test_id"

        suite = ExpectationSuite(
            name=fake_expectation_suite_name,
            expectations=[expect_column_values_to_be_in_set_col_a_with_meta],
            suite_parameters=test_suite_parameters,
            execution_engine_type=dummy_execution_engine_type,  # type: ignore[arg-type]
            meta=test_meta,
            id=test_id,
        )
        assert suite.name == fake_expectation_suite_name
        assert suite.expectation_configurations == [
            expect_column_values_to_be_in_set_col_a_with_meta
        ]
        assert suite.suite_parameters == test_suite_parameters
        assert suite.execution_engine_type == dummy_execution_engine_type
        assert suite.meta == test_meta
        assert suite.id == test_id

    @pytest.mark.unit
    def test_expectation_suite_init_overrides_expectations_dict_and_obj(
        self,
        fake_expectation_suite_name: str,
        expect_column_values_to_be_in_set_col_a_with_meta_dict: dict,
        expect_column_values_to_be_in_set_col_a_with_meta: ExpectationConfiguration,
    ):
        """What does this test and why?

        The expectations param of ExpectationSuite takes a list of ExpectationConfiguration or dicts and both can be provided at the same time. We need to make sure they both show up as expectation configurations in the instantiated ExpectationSuite.
        """  # noqa: E501

        test_expectations_input = [
            expect_column_values_to_be_in_set_col_a_with_meta_dict,
            expect_column_values_to_be_in_set_col_a_with_meta,
        ]

        suite = ExpectationSuite(
            name=fake_expectation_suite_name,
            expectations=test_expectations_input,  # type: ignore[arg-type]
        )
        assert suite.name == fake_expectation_suite_name

        test_expected_expectations = [
            ExpectationConfiguration(**expect_column_values_to_be_in_set_col_a_with_meta_dict),
            expect_column_values_to_be_in_set_col_a_with_meta,
        ]
        assert len(suite.expectations) == 2
        assert suite.expectation_configurations == test_expected_expectations

    @pytest.mark.unit
    def test_expectation_suite_init_overrides_non_json_serializable_meta(
        self,
        fake_expectation_suite_name: str,
    ):
        """What does this test and why?

        meta field overrides need to be json serializable, if not we raise an exception.
        """

        class NotSerializable:
            def __dict__(self):
                raise NotImplementedError

        test_meta = {"this_is_not_json_serializable": NotSerializable()}

        with pytest.raises(InvalidExpectationConfigurationError) as e:
            ExpectationSuite(
                name=fake_expectation_suite_name,
                meta=test_meta,  # type: ignore[arg-type]
            )
        assert "is of type NotSerializable which cannot be serialized to json" in str(e.value)


class TestCRUDMethods:
    """Tests related to the 1.0 CRUD API."""

    expectation_suite_name = "test-suite"

    @pytest.fixture
    def expectation(self) -> gxe.ExpectColumnValuesToBeInSet:
        return gxe.ExpectColumnValuesToBeInSet(
            column="a",
            value_set=[1, 2, 3],
            result_format="BASIC",
        )

    @pytest.mark.unit
    def test_instantiate_suite_with_expectations(self, expectation):
        context = Mock(spec=AbstractDataContext)
        set_context(project=context)
        columns = ["a", "b", "c", "d", "e"]
        expectations = [expectation.copy(update={"column": column}) for column in columns]
        suite = ExpectationSuite(name=self.expectation_suite_name, expectations=expectations)
        assert suite.expectations == expectations

    @pytest.mark.unit
    def test_instantiate_suite_fails_with_expectations_with_ids(self, expectation):
        context = Mock(spec=AbstractDataContext)
        set_context(project=context)
        columns = ["a", "b", "c", "d", "e"]
        expectations = [
            expectation.copy(update={"column": column, "id": uuid4()}) for column in columns
        ]
        with pytest.raises(
            ValueError,
            match="Expectations in parameter `expectations` must not belong to another ExpectationSuite.",  # noqa: E501
        ):
            ExpectationSuite(name=self.expectation_suite_name, expectations=expectations)

    @pytest.mark.unit
    def test_add_success_with_saved_suite(self, expectation):
        context = Mock(spec=AbstractDataContext)
        set_context(project=context)
        suite = ExpectationSuite(name=self.expectation_suite_name)

        with mock.patch.object(ExpectationSuite, "_submit_expectation_created_event"):
            created_expectation = suite.add_expectation(expectation=expectation)

        assert created_expectation == context.expectations_store.add_expectation.return_value
        context.expectations_store.add_expectation.assert_called_once_with(
            suite=suite, expectation=expectation
        )

    @pytest.mark.unit
    def test_add_success_with_unsaved_suite(self, expectation):
        context = Mock(spec=AbstractDataContext)
        context.expectations_store.has_key.return_value = False
        set_context(project=context)
        suite = ExpectationSuite(name=self.expectation_suite_name)

        created_expectation = suite.add_expectation(expectation=expectation)

        assert created_expectation == expectation
        assert len(suite.expectations) == 1

        # expect that adding an expectation to this suite doesnt have the side effect of
        # persisting the suite to the data context
        context.expectations_store.set.assert_not_called()

    @pytest.mark.unit
    def test_add_doesnt_duplicate_when_expectation_already_exists(self, expectation):
        context = Mock(spec=AbstractDataContext)
        context.expectations_store.has_key.return_value = True
        set_context(project=context)
        suite = ExpectationSuite(
            name=self.expectation_suite_name,
            expectations=[expectation.configuration],
        )

        suite.add_expectation(expectation=expectation)

        assert len(suite.expectations) == 1
        context.expectations_store.update.assert_not_called()

    @pytest.mark.unit
    def test_add_doesnt_mutate_suite_when_save_fails(self, expectation):
        context = Mock(spec=AbstractDataContext)
        context.expectations_store.add_expectation.side_effect = (
            ConnectionError()
        )  # arbitrary exception
        context.expectations_store.has_key.return_value = True
        set_context(project=context)
        suite = ExpectationSuite(
            name="test-suite",
        )

        with pytest.raises(ConnectionError):  # exception type isn't important
            suite.add_expectation(expectation=expectation)

        assert len(suite.expectations) == 0, "Expectation must not be added to Suite."

    @pytest.mark.unit
    def test_delete_success_with_saved_suite(self, expectation):
        context = Mock(spec=AbstractDataContext)
        set_context(project=context)
        context.expectations_store.has_key.return_value = True
        suite = ExpectationSuite(
            name=self.expectation_suite_name,
            expectations=[expectation.configuration],
        )

        deleted_expectation = suite.delete_expectation(expectation=expectation)

        assert deleted_expectation == expectation
        assert suite.expectations == []

        # expect that the data context is kept in sync with the mutation
        context.expectations_store.delete_expectation.assert_called_once_with(
            suite=suite, expectation=expectation
        )

    @pytest.mark.unit
    def test_delete_success_with_unsaved_suite(self, expectation):
        context = Mock(spec=AbstractDataContext)
        set_context(project=context)
        context.expectations_store.has_key.return_value = False
        suite = ExpectationSuite(
            name=self.expectation_suite_name,
            expectations=[expectation.configuration],
        )

        deleted_expectation = suite.delete_expectation(expectation=expectation)

        assert deleted_expectation == expectation
        assert suite.expectations == []
        # expect that deleting an expectation from this suite doesnt have the side effect of
        # persisting the suite to the data context
        context.expectations_store.delete_expectation.assert_not_called()

    @pytest.mark.unit
    def test_delete_fails_when_expectation_is_not_found(self, expectation):
        context = Mock(spec=AbstractDataContext)
        set_context(project=context)
        suite = ExpectationSuite(
            name="test-suite",
        )

        with pytest.raises(KeyError, match="No matching expectation was found."):
            suite.delete_expectation(expectation=expectation)

        context.expectations_store.delete_expectation.assert_not_called()

    @pytest.mark.unit
    def test_delete_doesnt_mutate_suite_when_save_fails(self, expectation):
        context = Mock(spec=AbstractDataContext)
        context.expectations_store.has_key.return_value = True
        context.expectations_store.delete_expectation.side_effect = (
            ConnectionError()
        )  # arbitrary exception
        set_context(project=context)
        suite = ExpectationSuite(
            name="test-suite",
            expectations=[
                expectation.configuration,
            ],
        )

        with pytest.raises(ConnectionError):  # exception type isn't important
            suite.delete_expectation(expectation=expectation)

        assert len(suite.expectations) == 1, "Expectation must still be in Suite."

    @pytest.mark.unit
    def test_save_success(self):
        context = Mock(spec=AbstractDataContext)
        context.expectations_store.has_key.return_value = True
        set_context(project=context)
        suite = ExpectationSuite(
            name=self.expectation_suite_name,
        )
        store_key = context.expectations_store.get_key.return_value

        suite.save()

        # expect that the data context is kept in sync
        context.expectations_store.update.assert_called_once_with(key=store_key, value=suite)

    @pytest.mark.filesystem
    def test_filesystem_context_update_suite_adds_ids(self, empty_data_context, expectation):
        context = empty_data_context
        self._test_update_suite_adds_ids(context, expectation)

    @pytest.mark.cloud
    def test_cloud_context_update_suite_adds_ids(self, empty_cloud_context_fluent, expectation):
        context = empty_cloud_context_fluent
        self._test_update_suite_adds_ids(context, expectation)

    def _test_update_suite_adds_ids(self, context, expectation):
        suite_name = "test-suite"
        suite = ExpectationSuite(suite_name)
        suite = context.suites.add(suite)
        uuid_to_test = suite.id
        try:
            UUID(uuid_to_test)
        except TypeError:
            pytest.fail(f"Expected UUID in ExpectationSuite.id, found {uuid_to_test}")
        expectation.id = None
        suite.add_expectation(expectation)
        expectation = copy(expectation)
        expectation.column = "foo"
        suite.add_expectation(expectation)
        assert len(suite.expectations) == 2
        for expectation in suite.expectations:
            uuid_to_test = expectation.id
            try:
                UUID(uuid_to_test)
            except TypeError:
                pytest.fail(f"Expected UUID in ExpectationConfiguration.id, found {uuid_to_test}")

    @pytest.mark.unit
    def test_suite_add_expectation_doesnt_allow_adding_an_expectation_with_id(self, expectation):
        suite = ExpectationSuite("test-suite")
        provided_id = "6b3f003d-d97b-4649-ba23-3f4e30986297"
        expectation.id = provided_id

        with pytest.raises(
            RuntimeError,
            match="Cannot add Expectation because it already belongs to an ExpectationSuite.",
        ):
            suite.add_expectation(expectation)

    @pytest.mark.cloud
    def test_cloud_expectation_can_be_saved_after_added(
        self, empty_cloud_context_fluent, expectation
    ):
        context = empty_cloud_context_fluent
        self._test_expectation_can_be_saved_after_added(context, expectation)

    @pytest.mark.filesystem
    def test_filesystem_expectation_can_be_saved_after_added(self, empty_data_context, expectation):
        context = empty_data_context
        self._test_expectation_can_be_saved_after_added(context, expectation)

    def _test_expectation_can_be_saved_after_added(self, context, expectation):
        suite_name = "test-suite"
        suite = ExpectationSuite(suite_name)
        suite = context.suites.add(suite)
        suite.add_expectation(expectation)
        updated_column_name = "foo"
        assert expectation.column != updated_column_name
        expectation.column = updated_column_name
        expectation.save()
        assert suite.expectations[0].column == updated_column_name
        suite = context.suites.get(suite_name)
        assert len(suite.expectations) == 1
        assert suite.expectations[0].column == updated_column_name

    @pytest.mark.cloud
    def test_cloud_expectation_can_be_saved_after_update(
        self, empty_cloud_context_fluent, expectation
    ):
        context = empty_cloud_context_fluent
        self._test_expectation_can_be_saved_after_update(context, expectation)

    @pytest.mark.filesystem
    def test_filesystem_expectation_can_be_saved_after_update(
        self, empty_data_context, expectation
    ):
        context = empty_data_context
        self._test_expectation_can_be_saved_after_update(context, expectation)

    def _test_expectation_can_be_saved_after_update(self, context, expectation):
        suite_name = "test-suite"
        suite = ExpectationSuite(suite_name, expectations=[expectation.configuration])
        suite = context.suites.add(suite)
        expectation = suite.expectations[0]
        updated_column_name = "foo"
        expectation.column = updated_column_name
        expectation.save()
        assert suite.expectations[0].column == updated_column_name
        suite = context.suites.get(suite_name)
        assert len(suite.expectations) == 1
        assert suite.expectations[0].column == updated_column_name

    @pytest.mark.filesystem
    def test_expectation_save_only_persists_single_change(self, empty_data_context):
        context = empty_data_context
        suite_name = "test-suite"
        expectations = [
            gxe.ExpectColumnValuesToBeInSet(
                column="a",
                value_set=[1, 2, 3],
            ),
            gxe.ExpectColumnValuesToBeInSet(
                column="b",
                value_set=[4, 5, 6],
            ),
        ]

        suite = ExpectationSuite(
            name=suite_name,
            expectations=[e.configuration for e in expectations],
        )
        context.suites.add(suite)

        assert suite.expectations[0].column == "a"
        assert suite.expectations[1].column == "b"

        # Change the column names of both expectations
        expectation = suite.expectations[0]
        other_expectation = suite.expectations[1]
        expectation.column = "c"
        other_expectation.column = "d"

        # Only persist change made to first expectation
        expectation.save()

        updated_suite = context.suites.get(name=suite_name)

        assert updated_suite.expectations[0].column == "c"
        assert updated_suite.expectations[1].column == "b"

    @pytest.mark.cloud
    def test_expectation_save_callback_can_come_from_any_copy_of_a_suite(
        self, empty_cloud_context_fluent
    ):
        """Equivalent calls to ExpectationSuite._save_expectation from different copies of a
        single ExpectationSuite must produce equivalent side effects.

        In some cases, the ExpectationsStore replaces Expectations from a given suite with Expectations
        from another copy of the same suite, in order to keep the ExpectationSuite in memory up to date
        with the remote ExpectationSuite. ExpectationSuite._save_expectation (and the corresponding logic
        the suite uses within the ExpectationsStore) must work equivalently regardless of which Suite instance
        it belongs to.
        """  # noqa: E501
        # Arrange
        context = empty_cloud_context_fluent
        suite_name = "test-suite"
        suite_a = ExpectationSuite(name=suite_name)
        column_name = "a"
        updated_column_name = "foo"
        expectations = [
            gxe.ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=[1, 2, 3],
            ),
            gxe.ExpectColumnValuesToBeInSet(
                column="b",
                value_set=[4, 5, 6],
            ),
        ]
        for expectation in expectations:
            suite_a.add_expectation(expectation)

        context.suites.add(suite_a)

        suite_b = context.suites.get(name=suite_name)

        # Act
        suite_a.expectations = suite_b.expectations
        expectation = suite_a.expectations[0]
        # the following assert is the method equivalent of `is`
        assert expectation._save_callback == suite_b._save_expectation
        assert expectation.column == column_name
        expectation.column = updated_column_name
        expectation.save()

        # Assert
        fetched_suite = context.suites.get(name=suite_name)
        assert (
            suite_a.expectations[0].column
            == fetched_suite.expectations[0].column
            == updated_column_name
        )

    @pytest.mark.unit
    def test_expectation_suite_name_can_be_updated(self, empty_cloud_context_fluent):
        """Expect that ExpectationSuite.name can be updated directly"""
        suite = ExpectationSuite(name=self.expectation_suite_name)
        new_name = "updated name"
        suite.name = "updated name"
        assert suite.name == new_name


class TestSuiteParameterOptions:
    """Tests around the suite_parameter_options property of ExpectationSuites.

    Note: suite_parameter_options is currently a sorted tuple, but doesn't necessarily have to be
    """

    SUITE_PARAMETER_VALUE_SET = "my_value_set"
    SUITE_PARAMETER_MIN = "my_min"
    SUITE_PARAMETER_MAX = "my_max"

    @pytest.fixture
    def expectation_suite(self) -> ExpectationSuite:
        get_context(mode="ephemeral")
        return ExpectationSuite("test-suite")

    @pytest.fixture
    def expectation_with_suite_parameter(
        self,
    ) -> Expectation:
        return gxe.ExpectColumnDistinctValuesToBeInSet(
            column="a", value_set={"$PARAMETER": self.SUITE_PARAMETER_VALUE_SET}
        )

    @pytest.fixture
    def expectation_with_duplicate_suite_parameter(
        self,
    ) -> Expectation:
        return gxe.ExpectColumnDistinctValuesToContainSet(
            column="a", value_set={"$PARAMETER": self.SUITE_PARAMETER_VALUE_SET}
        )

    @pytest.fixture
    def expectation_with_multiple_suite_parameters(
        self,
    ) -> Expectation:
        return gxe.ExpectColumnValuesToBeBetween(
            column="c",
            min_value={"$PARAMETER": self.SUITE_PARAMETER_MIN},
            max_value={"$PARAMETER": self.SUITE_PARAMETER_MAX},
        )

    @pytest.fixture
    def expectation_without_suite_parameters(
        self,
    ) -> Expectation:
        return gxe.ExpectColumnDistinctValuesToBeInSet(column="d", value_set=[1, 2])

    @pytest.mark.unit
    def test_empty_suite(self, expectation_suite: ExpectationSuite):
        assert expectation_suite.suite_parameter_options == tuple()

    @pytest.mark.unit
    def test_expectations_but_no_evaluation_params(
        self,
        expectation_suite: ExpectationSuite,
        expectation_without_suite_parameters: Expectation,
    ):
        expectation_suite.add_expectation(expectation_without_suite_parameters)

        assert expectation_suite.suite_parameter_options == tuple()

    @pytest.mark.unit
    def test_expectation_with_suite_parameter(
        self,
        expectation_suite: ExpectationSuite,
        expectation_with_suite_parameter: Expectation,
    ):
        expectation_suite.add_expectation(expectation_with_suite_parameter)

        assert expectation_suite.suite_parameter_options == (self.SUITE_PARAMETER_VALUE_SET,)

    @pytest.mark.unit
    def test_duplicate_suite_parameters_only_show_once(
        self,
        expectation_suite: ExpectationSuite,
        expectation_with_suite_parameter: Expectation,
        expectation_with_duplicate_suite_parameter: Expectation,
    ):
        expectation_suite.add_expectation(expectation_with_suite_parameter)
        expectation_suite.add_expectation(expectation_with_duplicate_suite_parameter)

        assert expectation_suite.suite_parameter_options == (self.SUITE_PARAMETER_VALUE_SET,)

    @pytest.mark.unit
    def test_multiple_suite_parameters_on_one_expectation(
        self,
        expectation_suite: ExpectationSuite,
        expectation_with_multiple_suite_parameters: Expectation,
    ):
        expectation_suite.add_expectation(expectation_with_multiple_suite_parameters)

        assert expectation_suite.suite_parameter_options == (
            self.SUITE_PARAMETER_MAX,
            self.SUITE_PARAMETER_MIN,
        )

    @pytest.mark.unit
    def test_multiple_suite_parameters_across_multiple_expectation(
        self,
        expectation_suite: ExpectationSuite,
        expectation_with_suite_parameter: Expectation,
        expectation_with_multiple_suite_parameters: Expectation,
    ):
        expectation_suite.add_expectation(expectation_with_suite_parameter)
        expectation_suite.add_expectation(expectation_with_multiple_suite_parameters)

        assert expectation_suite.suite_parameter_options == (
            self.SUITE_PARAMETER_MAX,
            self.SUITE_PARAMETER_MIN,
            self.SUITE_PARAMETER_VALUE_SET,
        )


@pytest.mark.unit
class TestAddCitation:
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
    def test_add_citation_not_specified_params_filtered(
        self, empty_suite_with_meta: ExpectationSuite
    ):
        empty_suite_with_meta.add_citation(
            "fake_comment",
            batch_spec={"fake": "batch_spec"},
            batch_markers={"fake": "batch_markers"},
        )
        assert "citations" in empty_suite_with_meta.meta

        citation_keys = set(empty_suite_with_meta.meta["citations"][0].keys())
        # Note: citation_date is always added if not provided
        assert citation_keys == {
            "comment",
            "citation_date",
            "batch_spec",
            "batch_markers",
        }
        # batch_definition (along with other keys that are not provided) should be filtered out
        assert "batch_definition" not in citation_keys

    @pytest.mark.unit
    @pytest.mark.v2_api
    def test_add_citation_accepts_v2_api_params(self, empty_suite_with_meta: ExpectationSuite):
        """This test ensures backward compatibility with the v2 api and can be removed when deprecated."""  # noqa: E501
        empty_suite_with_meta.add_citation(
            "fake_comment",
            batch_kwargs={"fake": "batch_kwargs"},
            batch_parameters={"fake": "batch_parameters"},
        )

        assert empty_suite_with_meta.meta["citations"][0]["batch_kwargs"] == {
            "fake": "batch_kwargs"
        }
        assert empty_suite_with_meta.meta["citations"][0]["batch_parameters"] == {
            "fake": "batch_parameters"
        }
        citation_keys = set(empty_suite_with_meta.meta["citations"][0].keys())
        assert citation_keys == {
            "comment",
            "citation_date",
            "batch_kwargs",
            "batch_parameters",
        }

    @pytest.mark.unit
    def test_add_citation_all_params(self, empty_suite_with_meta: ExpectationSuite):
        """Note, this does not include v2 api params e.g. batch_kwargs"""
        empty_suite_with_meta.add_citation(
            "fake_comment",
            batch_request={"fake": "batch_request"},
            batch_definition={"fake": "batch_definition"},
            batch_spec={"fake": "batch_spec"},
            batch_markers={"fake": "batch_markers"},
            profiler_config={"fake": "profiler_config"},
            citation_date="2022-09-08",
        )
        assert "citations" in empty_suite_with_meta.meta

        citation_keys = set(empty_suite_with_meta.meta["citations"][0].keys())
        assert citation_keys == {
            "comment",
            "citation_date",
            "batch_request",
            "batch_definition",
            "batch_spec",
            "batch_markers",
            "profiler_config",
        }

    @pytest.mark.parametrize(
        "citation_date",
        [
            pytest.param("2022-09-08", id="str override"),
            pytest.param(datetime.datetime(2022, 9, 8), id="datetime override"),
        ],
    )
    @pytest.mark.unit
    def test_add_citation_citation_date_override(
        self,
        citation_date: Union[str, datetime.datetime],
        empty_suite_with_meta: ExpectationSuite,
    ):
        empty_suite_with_meta.add_citation("fake_comment", citation_date=citation_date)
        assert (
            empty_suite_with_meta.meta["citations"][0]["citation_date"]
            == "2022-09-08T00:00:00.000000Z"
        )

    @pytest.mark.unit
    def test_add_citation_with_existing_citations(self, empty_suite_with_meta: ExpectationSuite):
        empty_suite_with_meta.add_citation("fake_comment1")
        assert "citations" in empty_suite_with_meta.meta
        assert len(empty_suite_with_meta.meta["citations"]) == 1
        empty_suite_with_meta.add_citation("fake_comment2")
        assert len(empty_suite_with_meta.meta["citations"]) == 2

    @pytest.mark.unit
    def test_add_citation_with_profiler_config(self, empty_suite_with_meta: ExpectationSuite):
        empty_suite_with_meta.add_citation(
            "fake_comment",
            profiler_config={"fake": "profiler_config"},
        )

        assert empty_suite_with_meta.meta["citations"][0]["profiler_config"] == {
            "fake": "profiler_config"
        }
        citation_keys = set(empty_suite_with_meta.meta["citations"][0].keys())
        assert citation_keys == {"comment", "citation_date", "profiler_config"}


class TestIsEquivalentTo:
    @pytest.mark.unit
    def test_is_equivalent_to_expectation_suite_and_dict_true(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        """Suite should be equivalent to its dict representation."""
        suite_with_single_expectation_dict: dict = expectationSuiteSchema.dump(
            suite_with_single_expectation
        )

        assert suite_with_single_expectation.isEquivalentTo(suite_with_single_expectation_dict)

    @pytest.mark.unit
    def test_is_equivalent_to_expectation_suite_and_dict_false(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        modified_suite = deepcopy(suite_with_single_expectation)
        modified_suite.expectations[0].value_set[0] = -1

        modified_suite_dict: dict = expectationSuiteSchema.dump(modified_suite)

        assert not suite_with_single_expectation.isEquivalentTo(modified_suite_dict)

    @pytest.mark.unit
    def test_is_equivalent_to_invalid_expectation_config_dict_returns_notimplemented(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        return_value = suite_with_single_expectation.isEquivalentTo(
            {"not_valid": "expectation_suite_config_dict"}
        )
        assert return_value == NotImplemented

    @pytest.mark.unit
    def test_is_equivalent_to_unsupported_class_returns_notimplemented(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        """If we are not comparing to an ExpectationSuite or dict then we should return NotImplemented."""  # noqa: E501

        class UnsupportedClass:
            pass

        return_value = suite_with_single_expectation.isEquivalentTo(UnsupportedClass())
        assert return_value == NotImplemented

    @pytest.mark.unit
    def test_is_equivalent_to_expectation_suite_classes_true_with_changes_to_non_considered_attributes(  # noqa: E501
        self, suite_with_single_expectation: ExpectationSuite
    ):
        """Only expectation equivalence is considered for suite equivalence. Marked as integration since this uses the ExpectationConfiguration.isEquivalentTo() under the hood."""  # noqa: E501
        different_but_equivalent_suite = deepcopy(suite_with_single_expectation)
        different_but_equivalent_suite.name = "different_name"
        different_but_equivalent_suite.meta = {"notes": "Different meta."}
        different_but_equivalent_suite.id = "different_id"

        assert suite_with_single_expectation.isEquivalentTo(different_but_equivalent_suite)
        assert different_but_equivalent_suite.isEquivalentTo(suite_with_single_expectation)

    @pytest.mark.unit
    def test_is_equivalent_to_expectation_suite_classes_false(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        """Only expectation equivalence is considered for suite equivalence. Marked as integration since this uses the ExpectationConfiguration.isEquivalentTo() under the hood."""  # noqa: E501
        different_and_not_equivalent_suite = deepcopy(suite_with_single_expectation)
        # Set different column in expectation kwargs
        expectation = different_and_not_equivalent_suite.expectations[0]
        expectation.column = "b"
        expectation.value_set = [1, 2, 3]

        assert not suite_with_single_expectation.isEquivalentTo(different_and_not_equivalent_suite)
        assert not different_and_not_equivalent_suite.isEquivalentTo(suite_with_single_expectation)

    @pytest.mark.unit
    def test_is_equivalent_to_expectation_suite_classes_false_multiple_equivalent_expectations(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        """Only expectation equivalence is considered for suite equivalence, and the same number of expectations in the suite is required for equivalence. Marked as integration since this uses the ExpectationConfiguration.isEquivalentTo() under the hood."""  # noqa: E501
        different_and_not_equivalent_suite = deepcopy(suite_with_single_expectation)
        # Add a copy of the existing expectation, using list .append() to bypass add_expectation logic to handle overwrite  # noqa: E501
        different_and_not_equivalent_suite.expectations.append(
            different_and_not_equivalent_suite._build_expectation(
                expectation_configuration=different_and_not_equivalent_suite.expectation_configurations[
                    0
                ]
            )
        )
        assert len(suite_with_single_expectation.expectations) == 1
        assert len(different_and_not_equivalent_suite.expectations) == 2

        assert not suite_with_single_expectation.isEquivalentTo(different_and_not_equivalent_suite)
        assert not different_and_not_equivalent_suite.isEquivalentTo(suite_with_single_expectation)


class TestEqDunder:
    @pytest.mark.unit
    def test_equality_to_unsupported_class_is_false(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        """If we are not comparing to an ExpectationSuite or dict then we should return False."""

        class UnsupportedClass:
            pass

        return_value = suite_with_single_expectation == UnsupportedClass()
        assert not return_value

    @pytest.mark.unit
    def test_expectation_suite_equality_single_expectation_true(
        self, suite_with_single_expectation: ExpectationSuite
    ):
        different_but_equivalent_suite = deepcopy(suite_with_single_expectation)
        assert suite_with_single_expectation == different_but_equivalent_suite
        assert different_but_equivalent_suite == suite_with_single_expectation

        assert not suite_with_single_expectation != different_but_equivalent_suite
        assert not different_but_equivalent_suite != suite_with_single_expectation

    @pytest.mark.parametrize(
        "attribute,new_value",
        [
            pytest.param("name", "different_name"),
            pytest.param(
                "data_context",
                MagicMock(),
                marks=pytest.mark.xfail(
                    strict=True,
                    raises=AssertionError,
                    reason="Currently data_context is not considered in ExpectationSuite equality",
                ),
            ),
            pytest.param("expectations", []),
            pytest.param("suite_parameters", {"different": "suite_parameters"}),
            pytest.param(
                "execution_engine_type",
                type(ExecutionEngine),
                marks=pytest.mark.xfail(
                    strict=True,
                    raises=AssertionError,
                    reason="Currently execution_engine_type is not considered in ExpectationSuite equality",  # noqa: E501
                ),
            ),
            pytest.param("meta", {"notes": "Different meta."}),
            pytest.param(
                "id",
                "different_id",
                marks=pytest.mark.xfail(
                    strict=True,
                    raises=AssertionError,
                    reason="Currently id is not considered in ExpectationSuite equality",
                ),
            ),
        ],
    )
    @pytest.mark.unit
    def test_expectation_suite_equality_false(
        self,
        attribute: str,
        new_value: Union[str, Dict[str, str]],
        suite_with_single_expectation: ExpectationSuite,
    ):
        different_but_equivalent_suite = deepcopy(suite_with_single_expectation)

        setattr(different_but_equivalent_suite, attribute, new_value)

        assert not suite_with_single_expectation == different_but_equivalent_suite
        assert not different_but_equivalent_suite == suite_with_single_expectation
        assert suite_with_single_expectation != different_but_equivalent_suite
        assert different_but_equivalent_suite != suite_with_single_expectation


# ### Below this line are mainly existing tests and fixtures that we are in the process of cleaning up  # noqa: E501


@pytest.fixture
def exp2() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3]},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp3() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "value_set": [-1, -2, -3],
        },
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp4() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "value_set": [1, 2, 3],
        },
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def column_pair_expectation() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_pair_values_to_be_in_set",
        kwargs={
            "column_A": "1",
            "column_B": "b",
            "value_pairs_set": [(1, 1), (2, 2)],
        },
    )


@pytest.fixture
def table_exp1() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_ordered_list",
        kwargs={"column_list": ["a", "b", "c"]},
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
def empty_suite() -> ExpectationSuite:
    return ExpectationSuite(
        name="warning",
        expectations=[],
        meta={"notes": "This is an expectation suite."},
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
) -> ExpectationSuite:
    suite = ExpectationSuite(
        name="warning",
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
    )
    return suite


@pytest.fixture
def baseline_suite(expect_column_values_to_be_in_set_col_a_with_meta, exp2) -> ExpectationSuite:
    return ExpectationSuite(
        name="warning",
        expectations=[expect_column_values_to_be_in_set_col_a_with_meta, exp2],
        meta={"notes": "This is an expectation suite."},
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

    yaml = YAMLHandler()
    return yaml.load(yaml_config)


@pytest.mark.unit
def test_expectation_suite_copy(baseline_suite):
    suite_copy = copy(baseline_suite)
    assert suite_copy == baseline_suite
    suite_copy.name = "blarg!"
    assert baseline_suite.name != "blarg"  # copy on primitive properties shouldn't propagate


@pytest.mark.unit
def test_expectation_suite_deepcopy(baseline_suite):
    suite_deepcopy = deepcopy(baseline_suite)
    assert suite_deepcopy == baseline_suite
    suite_deepcopy.name = "blarg!"
    assert baseline_suite.name != "blarg"  # copy on primitive properties shouldn't propagate
    suite_deepcopy.expectation_configurations[0].meta["notes"] = "a different note"
    # deepcopy on deep attributes does not propagate
    assert baseline_suite.expectation_configurations[0].meta["notes"] == "This is an expectation."


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
    properties_dict_list: List[Dict[str, Any]] = baseline_suite.get_citations(sort=False)
    for properties_dict in properties_dict_list:
        filter_properties_dict(properties=properties_dict, clean_falsy=True, inplace=True)
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
        filter_properties_dict(properties=properties_dict, clean_falsy=True, inplace=True)
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

    baseline_suite.add_citation("first", batch_kwargs={"path": "first"}, citation_date="2000-01-01")
    baseline_suite.add_citation(
        "second", batch_kwargs={"path": "second"}, citation_date="2001-01-01"
    )
    baseline_suite.add_citation("third", citation_date="2002-01-01")

    properties_dict_list: List[Dict[str, Any]] = baseline_suite.get_citations(
        sort=True, require_batch_kwargs=True
    )
    for properties_dict in properties_dict_list:
        filter_properties_dict(properties=properties_dict, clean_falsy=True, inplace=True)
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
        filter_properties_dict(properties=properties_dict, clean_falsy=True, inplace=True)
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
    obs = suite_with_table_and_column_expectations.get_grouped_and_ordered_expectations_by_expectation_type()  # noqa: E501
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
    obs = suite_with_table_and_column_expectations.get_grouped_and_ordered_expectations_by_domain_type()  # noqa: E501
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
def test_add_expectation_fails_validation(empty_suite_with_meta: ExpectationSuite):
    suite = empty_suite_with_meta

    expectation_type = "my_fake_expectation"
    kwargs = {"foo": "bar"}
    expectation_configuration = ExpectationConfiguration(
        expectation_type=expectation_type,
        kwargs=kwargs,
    )

    with pytest.raises(gx_exceptions.InvalidExpectationConfigurationError) as e:
        suite.add_expectation_configuration(expectation_configuration)

    assert f"{expectation_type} not found" in str(e)


class TestExpectationSuiteAnalytics:
    @pytest.fixture
    def empty_suite(self, in_memory_runtime_context) -> ExpectationSuite:
        return in_memory_runtime_context.add_expectation_suite("my_suite")

    @pytest.fixture
    def expect_column_values_to_be_between(self) -> Expectation:
        return gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=6)

    @pytest.mark.unit
    def test_add_expectation_emits_event(self, empty_suite, expect_column_values_to_be_between):
        suite = empty_suite
        expectation = expect_column_values_to_be_between

        with mock.patch("great_expectations.core.expectation_suite.submit_event") as mock_submit:
            _ = suite.add_expectation(expectation)

        mock_submit.assert_called_once_with(
            event=ExpectationSuiteExpectationCreatedEvent(
                expectation_id=mock.ANY,
                expectation_suite_id=mock.ANY,
                expectation_type="expect_column_values_to_be_between",
                custom_exp_type=False,
            )
        )

    @pytest.mark.unit
    def test_add_custom_expectation_emits_event(self, empty_suite):
        suite = empty_suite

        class ExpectColumnValuesToBeBetweenOneAndTen(gxe.ExpectColumnValuesToBeBetween):
            min_value: int = 1
            max_value: int = 10

        expectation = ExpectColumnValuesToBeBetweenOneAndTen(column="passenger_count")

        with mock.patch("great_expectations.core.expectation_suite.submit_event") as mock_submit:
            _ = suite.add_expectation(expectation)

        mock_submit.assert_called_once_with(
            event=ExpectationSuiteExpectationCreatedEvent(
                expectation_id=mock.ANY,
                expectation_suite_id=mock.ANY,
                expectation_type=mock.ANY,
                custom_exp_type=True,
            )
        )

        # Due to anonymizer randomization, we can't assert the exact expectation type
        # We can however assert that it has been hashed
        expectation_type = mock_submit.call_args.kwargs["event"].expectation_type
        assert not expectation_type.startswith("expect_")

    @pytest.mark.unit
    def test_delete_expectation_emits_event(self, empty_suite, expect_column_values_to_be_between):
        suite = empty_suite
        expectation = expect_column_values_to_be_between

        suite.add_expectation(expectation)

        with mock.patch("great_expectations.core.expectation_suite.submit_event") as mock_submit:
            suite.delete_expectation(expectation)

        mock_submit.assert_called_once_with(
            event=ExpectationSuiteExpectationDeletedEvent(
                expectation_id=mock.ANY,
                expectation_suite_id=mock.ANY,
            )
        )

    @pytest.mark.unit
    def test_expectation_save_callback_emits_event(
        self, empty_suite, expect_column_values_to_be_between
    ):
        suite = empty_suite
        expectation = expect_column_values_to_be_between

        expectation = suite.add_expectation(expectation)
        expectation.column = "fare_amount"

        with mock.patch("great_expectations.core.expectation_suite.submit_event") as mock_submit:
            expectation.save()

        mock_submit.assert_called_once_with(
            event=ExpectationSuiteExpectationUpdatedEvent(
                expectation_id=mock.ANY,
                expectation_suite_id=mock.ANY,
            )
        )


@pytest.mark.unit
def test_identifier_bundle_with_existing_id():
    suite = ExpectationSuite(name="my_suite", id="fa34fbb7-124d-42ff-9760-e410ee4584a0")

    assert suite.identifier_bundle() == _IdentifierBundle(
        name="my_suite", id="fa34fbb7-124d-42ff-9760-e410ee4584a0"
    )


@pytest.mark.unit
def test_identifier_bundle_no_id():
    _ = gx.get_context(mode="ephemeral")
    suite = ExpectationSuite(name="my_suite", id=None)

    actual = suite.identifier_bundle()
    expected = {"name": "my_suite", "id": mock.ANY}

    assert actual.dict() == expected
    assert actual.id is not None
