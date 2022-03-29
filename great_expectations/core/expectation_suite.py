import datetime
import json
import logging
import uuid
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

import great_expectations as ge
from great_expectations import __version__ as ge_version
from great_expectations.core.evaluation_parameters import (
    _deduplicate_evaluation_parameter_dependencies,
)
from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration,
    ExpectationConfigurationSchema,
    expectationConfigurationSchema,
)
from great_expectations.core.util import (
    convert_to_json_serializable,
    ensure_json_serializable,
    get_datetime_string_from_strftime_format,
    nested_update,
    parse_string_to_datetime,
)
from great_expectations.exceptions import (
    DataContextError,
    InvalidExpectationConfigurationError,
)
from great_expectations.marshmallow__shade import (
    Schema,
    ValidationError,
    fields,
    pre_dump,
)
from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)


class ExpectationSuite(SerializableDictDot):
    """
    This ExpectationSuite object has create, read, update, and delete functionality for its expectations:
        -create: self.add_expectation()
        -read: self.find_expectation_indexes()
        -update: self.add_expectation() or self.patch_expectation()
        -delete: self.remove_expectation()
    """

    def __init__(
        self,
        expectation_suite_name,
        data_context=None,
        expectations=None,
        evaluation_parameters=None,
        data_asset_type=None,
        execution_engine_type=None,
        meta=None,
        ge_cloud_id=None,
    ):
        self.expectation_suite_name = expectation_suite_name
        self.ge_cloud_id = ge_cloud_id
        self._data_context = data_context

        if expectations is None:
            expectations = []
        self.expectations = [
            ExpectationConfiguration(**expectation)
            if isinstance(expectation, dict)
            else expectation
            for expectation in expectations
        ]
        if evaluation_parameters is None:
            evaluation_parameters = {}
        self.evaluation_parameters = evaluation_parameters
        self.data_asset_type = data_asset_type
        self.execution_engine_type = execution_engine_type
        if meta is None:
            meta = {"great_expectations_version": ge_version}
        if (
            "great_expectations.__version__" not in meta.keys()
            and "great_expectations_version" not in meta.keys()
        ):
            meta["great_expectations_version"] = ge_version
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta

    def add_citation(
        self,
        comment: str,
        batch_request: Optional[
            Union[str, Dict[str, Union[str, Dict[str, Any]]]]
        ] = None,
        batch_definition: Optional[dict] = None,
        batch_spec: Optional[dict] = None,
        batch_kwargs: Optional[dict] = None,
        batch_markers: Optional[dict] = None,
        batch_parameters: Optional[dict] = None,
        profiler_config: Optional[dict] = None,
        citation_date: Optional[Union[str, datetime.datetime]] = None,
    ):
        if "citations" not in self.meta:
            self.meta["citations"] = []

        if isinstance(citation_date, str):
            citation_date = parse_string_to_datetime(datetime_string=citation_date)

        citation_date = citation_date or datetime.datetime.now(datetime.timezone.utc)
        citation: Dict[str, Any] = {
            "citation_date": get_datetime_string_from_strftime_format(
                format_str="%Y-%m-%dT%H:%M:%S.%fZ", datetime_obj=citation_date
            ),
            "batch_request": batch_request,
            "batch_definition": batch_definition,
            "batch_spec": batch_spec,
            "batch_kwargs": batch_kwargs,
            "batch_markers": batch_markers,
            "batch_parameters": batch_parameters,
            "profiler_config": profiler_config,
            "comment": comment,
        }
        ge.util.filter_properties_dict(
            properties=citation, clean_falsy=True, inplace=True
        )
        self.meta["citations"].append(citation)

    def isEquivalentTo(self, other):
        """
        ExpectationSuite equivalence relies only on expectations and evaluation parameters. It does not include:
        - data_asset_name
        - expectation_suite_name
        - meta
        - data_asset_type
        """
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other_dict: dict = expectationSuiteSchema.load(other)
                    other: ExpectationSuite = ExpectationSuite(
                        **other_dict, data_context=self._data_context
                    )
                except ValidationError:
                    logger.debug(
                        "Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                        "dict other could not be instantiated as an ExpectationConfiguration"
                    )
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented

        return len(self.expectations) == len(other.expectations) and all(
            [
                mine.isEquivalentTo(theirs)
                for (mine, theirs) in zip(self.expectations, other.expectations)
            ]
        )

    def __eq__(self, other):
        """ExpectationSuite equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all(
            (
                self.expectation_suite_name == other.expectation_suite_name,
                self.expectations == other.expectations,
                self.evaluation_parameters == other.evaluation_parameters,
                self.data_asset_type == other.data_asset_type,
                self.meta == other.meta,
            )
        )

    def __ne__(self, other):
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __deepcopy__(self, memo: dict):
        cls = self.__class__
        result = cls.__new__(cls)

        memo[id(self)] = result

        attributes_to_copy = set(ExpectationSuiteSchema().fields.keys())
        for key in attributes_to_copy:
            setattr(result, key, deepcopy(getattr(self, key)))

        setattr(result, "_data_context", self._data_context)

        return result

    def to_json_dict(self):
        myself = expectationSuiteSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself["expectations"] = convert_to_json_serializable(myself["expectations"])
        try:
            myself["evaluation_parameters"] = convert_to_json_serializable(
                myself["evaluation_parameters"]
            )
        except KeyError:
            pass  # Allow evaluation parameters to be missing if empty
        myself["meta"] = convert_to_json_serializable(myself["meta"])
        return myself

    def get_evaluation_parameter_dependencies(self):
        dependencies = {}
        for expectation in self.expectations:
            t = expectation.get_evaluation_parameter_dependencies()
            nested_update(dependencies, t)

        dependencies = _deduplicate_evaluation_parameter_dependencies(dependencies)
        return dependencies

    def get_citations(
        self,
        sort: bool = True,
        require_batch_kwargs: bool = False,
        require_batch_request: bool = False,
        require_profiler_config: bool = False,
    ) -> List[Dict[str, Any]]:
        citations: List[Dict[str, Any]] = self.meta.get("citations", [])
        if require_batch_kwargs:
            citations = self._filter_citations(
                citations=citations, filter_key="batch_kwargs"
            )
        if require_batch_request:
            citations = self._filter_citations(
                citations=citations, filter_key="batch_request"
            )
        if require_profiler_config:
            citations = self._filter_citations(
                citations=citations, filter_key="profiler_config"
            )
        if not sort:
            return citations
        return self._sort_citations(citations=citations)

    def get_table_expectations(self):
        """Return a list of table expectations."""
        return [
            e
            for e in self.expectations
            if e.expectation_type.startswith("expect_table_")
        ]

    def get_column_expectations(self):
        """Return a list of column map expectations."""
        return [e for e in self.expectations if "column" in e.kwargs]

    @staticmethod
    def _filter_citations(
        citations: List[Dict[str, Any]], filter_key
    ) -> List[Dict[str, Any]]:
        citations_with_bk: List[Dict[str, Any]] = []
        for citation in citations:
            if filter_key in citation and citation.get(filter_key):
                citations_with_bk.append(citation)
        return citations_with_bk

    @staticmethod
    def _sort_citations(citations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(citations, key=lambda x: x["citation_date"])

    # CRUD methods #

    def append_expectation(self, expectation_config):
        """Appends an expectation.

           Args:
               expectation_config (ExpectationConfiguration): \
                   The expectation to be added to the list.

           Notes:
               May want to add type-checking in the future.
        """
        self.expectations.append(expectation_config)

    def remove_expectation(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        remove_multiple_matches: bool = False,
        ge_cloud_id: Optional[Union[str, uuid.UUID]] = None,
    ) -> List[ExpectationConfiguration]:
        """

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against for
                the removal of expectations.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                 based on all configuration parameters
            remove_multiple_matches: If True, will remove multiple matching expectations. If False, will raise a ValueError.
        Returns: The list of deleted ExpectationConfigurations

        Raises:
            No match
            More than 1 match, if remove_multiple_matches = False
        """
        if expectation_configuration is None and ge_cloud_id is None:
            raise TypeError(
                "Must provide either expectation_configuration or ge_cloud_id"
            )

        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration=expectation_configuration,
            match_type=match_type,
            ge_cloud_id=ge_cloud_id,
        )
        if len(found_expectation_indexes) < 1:
            raise ValueError("No matching expectation was found.")

        elif len(found_expectation_indexes) > 1:
            if remove_multiple_matches:
                removed_expectations = []
                for index in sorted(found_expectation_indexes, reverse=True):
                    removed_expectations.append(self.expectations.pop(index))
                return removed_expectations
            else:
                raise ValueError(
                    "More than one matching expectation was found. Specify more precise matching criteria,"
                    "or set remove_multiple_matches=True"
                )

        else:
            return [self.expectations.pop(found_expectation_indexes[0])]

    def remove_all_expectations_of_type(
        self, expectation_types: Union[List[str], str]
    ) -> List[ExpectationConfiguration]:
        if isinstance(expectation_types, str):
            expectation_types = [expectation_types]
        removed_expectations = [
            expectation
            for expectation in self.expectations
            if expectation.expectation_type in expectation_types
        ]
        self.expectations = [
            expectation
            for expectation in self.expectations
            if expectation.expectation_type not in expectation_types
        ]

        return removed_expectations

    def find_expectation_indexes(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        ge_cloud_id: str = None,
    ) -> List[int]:
        """
        Find indexes of Expectations matching the given ExpectationConfiguration on the given match_type.
        If a ge_cloud_id is provided, match_type is ignored and only indexes of Expectations
        with matching ge_cloud_id are returned.

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to
                find the index of any matching Expectation Configurations on the suite.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                 based on all configuration parameters
            ge_cloud_id: Great Expectations Cloud id

        Returns: A list of indexes of matching ExpectationConfiguration

        Raises:
            InvalidExpectationConfigurationError

        """
        if expectation_configuration is None and ge_cloud_id is None:
            raise TypeError(
                "Must provide either expectation_configuration or ge_cloud_id"
            )
        if expectation_configuration and not isinstance(
            expectation_configuration, ExpectationConfiguration
        ):
            raise InvalidExpectationConfigurationError(
                "Ensure that expectation configuration is valid."
            )
        match_indexes = []
        for idx, expectation in enumerate(self.expectations):
            if ge_cloud_id is not None:
                if str(expectation.ge_cloud_id) == str(ge_cloud_id):
                    match_indexes.append(idx)
            else:
                if expectation.isEquivalentTo(expectation_configuration, match_type):
                    match_indexes.append(idx)

        return match_indexes

    def find_expectations(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        ge_cloud_id: Optional[str] = None,
    ) -> List[ExpectationConfiguration]:
        """
        Find Expectations matching the given ExpectationConfiguration on the given match_type.
        If a ge_cloud_id is provided, match_type is ignored and only Expectations with matching
        ge_cloud_id are returned.

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to
                find the index of any matching Expectation Configurations on the suite.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                 based on all configuration parameters
            ge_cloud_id: Great Expectations Cloud id

        Returns: A list of matching ExpectationConfigurations
        """

        if expectation_configuration is None and ge_cloud_id is None:
            raise TypeError(
                "Must provide either expectation_configuration or ge_cloud_id"
            )
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type, ge_cloud_id
        )
        if len(found_expectation_indexes) > 0:
            return [self.expectations[idx] for idx in found_expectation_indexes]
        else:
            return []

    def replace_expectation(
        self,
        new_expectation_configuration: Union[ExpectationConfiguration, dict],
        existing_expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        ge_cloud_id: Optional[str] = None,
    ):
        """
        Find Expectations matching the given ExpectationConfiguration on the given match_type.
        If a ge_cloud_id is provided, match_type is ignored and only Expectations with matching
        ge_cloud_id are returned.

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to
                find the index of any matching Expectation Configurations on the suite.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                 based on all configuration parameters
            ge_cloud_id: Great Expectations Cloud id

        Returns: A list of matching ExpectationConfigurations
        """
        if existing_expectation_configuration is None and ge_cloud_id is None:
            raise TypeError(
                "Must provide either existing_expectation_configuration or ge_cloud_id"
            )

        if isinstance(new_expectation_configuration, dict):
            new_expectation_configuration = expectationConfigurationSchema.load(
                new_expectation_configuration
            )

        found_expectation_indexes = self.find_expectation_indexes(
            existing_expectation_configuration, match_type, ge_cloud_id
        )
        if len(found_expectation_indexes) > 1:
            raise ValueError(
                "More than one matching expectation was found. Please be more specific with your search "
                "criteria"
            )
        elif len(found_expectation_indexes) == 0:
            raise ValueError("No matching Expectation was found.")

        self.expectations[found_expectation_indexes[0]] = new_expectation_configuration

    def patch_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        op: str,
        path: str,
        value: Any,
        match_type: str,
    ) -> ExpectationConfiguration:
        """

        Args:
             expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to
                 find the expectation to patch.
             op: A jsonpatch operation (one of 'add','update', or 'remove') (see http://jsonpatch.com/)
             path: A jsonpatch path for the patch operation (see http://jsonpatch.com/)
             value: The value to patch (see http://jsonpatch.com/)
             match_type: The match type to use for find_expectation_index()

        Returns: The patched ExpectationConfiguration

        Raises:
            No match
            More than 1 match

        """
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )

        if len(found_expectation_indexes) < 1:
            raise ValueError("No matching expectation was found.")
        elif len(found_expectation_indexes) > 1:
            raise ValueError(
                "More than one matching expectation was found. Please be more specific with your search "
                "criteria"
            )

        self.expectations[found_expectation_indexes[0]].patch(op, path, value)
        return self.expectations[found_expectation_indexes[0]]

    def _add_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        send_usage_event: bool,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> ExpectationConfiguration:
        """
        This is a private method for adding expectations that allows for usage_events to be suppressed when
        Expectations are added through internal processing (ie. while building profilers, rendering or validation). It
        takes in send_usage_event boolean.

        Args:
            expectation_configuration: The ExpectationConfiguration to add or update
            send_usage_event: Whether to send a usage_statistics event. When called through ExpectationSuite class'
                public add_expectation() method, this is set to `True`.
            match_type: The criteria used to determine whether the Suite already has an ExpectationConfiguration
                and so whether we should add or replace.
            overwrite_existing: If the expectation already exists, this will overwrite if True and raise an error if
                False.
        Returns:
            The ExpectationConfiguration to add or replace.
        Raises:
            More than one match
            One match if overwrite_existing = False
        """

        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )

        if len(found_expectation_indexes) > 1:
            if send_usage_event:
                self.send_usage_event(success=False)
            raise ValueError(
                "More than one matching expectation was found. Please be more specific with your search "
                "criteria"
            )
        elif len(found_expectation_indexes) == 1:
            # Currently, we completely replace the expectation_configuration, but we could potentially use patch_expectation
            # to update instead. We need to consider how to handle meta in that situation.
            # patch_expectation = jsonpatch.make_patch(self.expectations[found_expectation_index] \
            #   .kwargs, expectation_configuration.kwargs)
            # patch_expectation.apply(self.expectations[found_expectation_index].kwargs, in_place=True)
            if overwrite_existing:
                # if existing Expectation has a ge_cloud_id, add it back to the new Expectation Configuration
                existing_expectation_ge_cloud_id = self.expectations[
                    found_expectation_indexes[0]
                ].ge_cloud_id
                if existing_expectation_ge_cloud_id is not None:
                    expectation_configuration.ge_cloud_id = (
                        existing_expectation_ge_cloud_id
                    )
                self.expectations[
                    found_expectation_indexes[0]
                ] = expectation_configuration
            else:
                if send_usage_event:
                    self.send_usage_event(success=False)
                raise DataContextError(
                    "A matching ExpectationConfiguration already exists. If you would like to overwrite this "
                    "ExpectationConfiguration, set overwrite_existing=True"
                )
        else:
            self.append_expectation(expectation_configuration)
        if send_usage_event:
            self.send_usage_event(success=True)
        return expectation_configuration

    def send_usage_event(self, success: bool):
        usage_stats_event_name: str = "expectation_suite.add_expectation"
        usage_stats_event_payload: dict = {}
        if self._data_context is not None:
            self._data_context.send_usage_message(
                event=usage_stats_event_name,
                event_payload=usage_stats_event_payload,
                success=success,
            )

    def add_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> ExpectationConfiguration:
        """
        Args:
            expectation_configuration: The ExpectationConfiguration to add or update
            match_type: The criteria used to determine whether the Suite already has an ExpectationConfiguration
                and so whether we should add or replace.
            overwrite_existing: If the expectation already exists, this will overwrite if True and raise an error if
                False.
        Returns:
            The ExpectationConfiguration to add or replace.
        Raises:
            More than one match
            One match if overwrite_existing = False
        """
        return self._add_expectation(
            expectation_configuration=expectation_configuration,
            send_usage_event=True,
            match_type=match_type,
            overwrite_existing=overwrite_existing,
        )

    def get_grouped_and_ordered_expectations_by_column(
        self, expectation_type_filter: Optional[str] = None
    ) -> Tuple[Dict[str, List[ExpectationConfiguration]], List[str]]:
        expectations_by_column = {}
        ordered_columns = []

        for expectation in self.expectations:
            if "column" in expectation.kwargs:
                column = expectation.kwargs["column"]
            else:
                column = "_nocolumn"
            if column not in expectations_by_column:
                expectations_by_column[column] = []

            if (
                expectation_type_filter is None
                or expectation.expectation_type == expectation_type_filter
            ):
                expectations_by_column[column].append(expectation)

            # if possible, get the order of columns from expect_table_columns_to_match_ordered_list
            if (
                expectation.expectation_type
                == "expect_table_columns_to_match_ordered_list"
            ):
                exp_column_list = expectation.kwargs["column_list"]
                if exp_column_list and len(exp_column_list) > 0:
                    ordered_columns = exp_column_list

        # Group items by column
        sorted_columns = sorted(list(expectations_by_column.keys()))

        # only return ordered columns from expect_table_columns_to_match_ordered_list evr if they match set of column
        # names from entire evr, else use alphabetic sort
        if set(sorted_columns) == set(ordered_columns):
            return expectations_by_column, ordered_columns
        else:
            return expectations_by_column, sorted_columns


class ExpectationSuiteSchema(Schema):
    expectation_suite_name = fields.Str()
    ge_cloud_id = fields.UUID(required=False, allow_none=True)
    expectations = fields.List(fields.Nested(ExpectationConfigurationSchema))
    evaluation_parameters = fields.Dict(allow_none=True)
    data_asset_type = fields.Str(allow_none=True)
    meta = fields.Dict()

    # NOTE: 20191107 - JPC - we may want to remove clean_empty and update tests to require the other fields;
    # doing so could also allow us not to have to make a copy of data in the pre_dump method.
    def clean_empty(self, data):
        if isinstance(data, ExpectationSuite):
            if not hasattr(data, "evaluation_parameters"):
                pass
            elif len(data.evaluation_parameters) == 0:
                del data.evaluation_parameters

            if not hasattr(data, "meta"):
                pass
            elif data.meta is None or data.meta == []:
                pass
            elif len(data.meta) == 0:
                del data.meta
        elif isinstance(data, dict):
            if not data.get("evaluation_parameters"):
                pass
            elif len(data.get("evaluation_parameters")) == 0:
                data.pop("evaluation_parameters")

            if not data.get("meta"):
                pass
            elif data.get("meta") is None or data.get("meta") == []:
                pass
            elif len(data.get("meta")) == 0:
                data.pop("meta")
        return data

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        if isinstance(data, ExpectationSuite):
            data.meta = convert_to_json_serializable(data.meta)
        elif isinstance(data, dict):
            data["meta"] = convert_to_json_serializable(data.get("meta"))
        data = self.clean_empty(data)
        return data


expectationSuiteSchema = ExpectationSuiteSchema()
