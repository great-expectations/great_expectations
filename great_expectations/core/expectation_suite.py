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
from great_expectations.core.usage_statistics.events import UsageStatsEvents
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
    "\n    This ExpectationSuite object has create, read, update, and delete functionality for its expectations:\n        -create: self.add_expectation()\n        -read: self.find_expectation_indexes()\n        -update: self.add_expectation() or self.patch_expectation()\n        -delete: self.remove_expectation()\n"

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
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.expectation_suite_name = expectation_suite_name
        self.ge_cloud_id = ge_cloud_id
        self._data_context = data_context
        if expectations is None:
            expectations = []
        self.expectations = [
            (
                ExpectationConfiguration(**expectation)
                if isinstance(expectation, dict)
                else expectation
            )
            for expectation in expectations
        ]
        if evaluation_parameters is None:
            evaluation_parameters = {}
        self.evaluation_parameters = evaluation_parameters
        self.data_asset_type = data_asset_type
        self.execution_engine_type = execution_engine_type
        if meta is None:
            meta = {"great_expectations_version": ge_version}
        if ("great_expectations.__version__" not in meta.keys()) and (
            "great_expectations_version" not in meta.keys()
        ):
            meta["great_expectations_version"] = ge_version
        ensure_json_serializable(meta)
        self.meta = meta

    def add_citation(
        self,
        comment: str,
        batch_request: Optional[
            Union[(str, Dict[(str, Union[(str, Dict[(str, Any)])])])]
        ] = None,
        batch_definition: Optional[dict] = None,
        batch_spec: Optional[dict] = None,
        batch_kwargs: Optional[dict] = None,
        batch_markers: Optional[dict] = None,
        batch_parameters: Optional[dict] = None,
        profiler_config: Optional[dict] = None,
        citation_date: Optional[Union[(str, datetime.datetime)]] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if "citations" not in self.meta:
            self.meta["citations"] = []
        if isinstance(citation_date, str):
            citation_date = parse_string_to_datetime(datetime_string=citation_date)
        citation_date = citation_date or datetime.datetime.now(datetime.timezone.utc)
        citation: Dict[(str, Any)] = {
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        ExpectationSuite equivalence relies only on expectations and evaluation parameters. It does not include:\n        - data_asset_name\n        - expectation_suite_name\n        - meta\n        - data_asset_type\n        "
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other_dict: dict = expectationSuiteSchema.load(other)
                    other: ExpectationSuite = ExpectationSuite(
                        **other_dict, data_context=self._data_context
                    )
                except ValidationError:
                    logger.debug(
                        "Unable to evaluate equivalence of ExpectationConfiguration object with dict because dict other could not be instantiated as an ExpectationConfiguration"
                    )
                    return NotImplemented
            else:
                return NotImplemented
        return (len(self.expectations) == len(other.expectations)) and all(
            [
                mine.isEquivalentTo(theirs)
                for (mine, theirs) in zip(self.expectations, other.expectations)
            ]
        )

    def __eq__(self, other):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "ExpectationSuite equality ignores instance identity, relying only on properties."
        if not isinstance(other, self.__class__):
            return NotImplemented
        return all(
            (
                (self.expectation_suite_name == other.expectation_suite_name),
                (self.expectations == other.expectations),
                (self.evaluation_parameters == other.evaluation_parameters),
                (self.data_asset_type == other.data_asset_type),
                (self.meta == other.meta),
            )
        )

    def __ne__(self, other):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return not (self == other)

    def __repr__(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return json.dumps(self.to_json_dict(), indent=2)

    def __deepcopy__(self, memo: dict):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        attributes_to_copy = set(ExpectationSuiteSchema().fields.keys())
        for key in attributes_to_copy:
            setattr(result, key, deepcopy(getattr(self, key)))
        setattr(result, "_data_context", self._data_context)
        return result

    def to_json_dict(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        myself = expectationSuiteSchema.dump(self)
        myself["expectations"] = convert_to_json_serializable(myself["expectations"])
        try:
            myself["evaluation_parameters"] = convert_to_json_serializable(
                myself["evaluation_parameters"]
            )
        except KeyError:
            pass
        myself["meta"] = convert_to_json_serializable(myself["meta"])
        return myself

    def get_evaluation_parameter_dependencies(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
    ) -> List[Dict[(str, Any)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        citations: List[Dict[(str, Any)]] = self.meta.get("citations", [])
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Return a list of table expectations."
        return [
            e
            for e in self.expectations
            if e.expectation_type.startswith("expect_table_")
        ]

    def get_column_expectations(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Return a list of column map expectations."
        return [e for e in self.expectations if ("column" in e.kwargs)]

    @staticmethod
    def _filter_citations(
        citations: List[Dict[(str, Any)]], filter_key
    ) -> List[Dict[(str, Any)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        citations_with_bk: List[Dict[(str, Any)]] = []
        for citation in citations:
            if (filter_key in citation) and citation.get(filter_key):
                citations_with_bk.append(citation)
        return citations_with_bk

    @staticmethod
    def _sort_citations(citations: List[Dict[(str, Any)]]) -> List[Dict[(str, Any)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return sorted(citations, key=(lambda x: x["citation_date"]))

    def append_expectation(self, expectation_config) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Appends an expectation.\n\n           Args:\n               expectation_config (ExpectationConfiguration):                    The expectation to be added to the list.\n\n           Notes:\n               May want to add type-checking in the future.\n        "
        self.expectations.append(expectation_config)

    def remove_expectation(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        remove_multiple_matches: bool = False,
        ge_cloud_id: Optional[Union[(str, uuid.UUID)]] = None,
    ) -> List[ExpectationConfiguration]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n\n        Args:\n            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against for\n                the removal of expectations.\n            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based\n                on the data evaluated by that expectation, 'success' to match based on all configuration parameters\n                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match\n                 based on all configuration parameters\n            remove_multiple_matches: If True, will remove multiple matching expectations. If False, will raise a ValueError.\n        Returns: The list of deleted ExpectationConfigurations\n\n        Raises:\n            No match\n            More than 1 match, if remove_multiple_matches = False\n        "
        if (expectation_configuration is None) and (ge_cloud_id is None):
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
                    "More than one matching expectation was found. Specify more precise matching criteria,or set remove_multiple_matches=True"
                )
        else:
            return [self.expectations.pop(found_expectation_indexes[0])]

    def remove_all_expectations_of_type(
        self, expectation_types: Union[(List[str], str)]
    ) -> List[ExpectationConfiguration]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if isinstance(expectation_types, str):
            expectation_types = [expectation_types]
        removed_expectations = [
            expectation
            for expectation in self.expectations
            if (expectation.expectation_type in expectation_types)
        ]
        self.expectations = [
            expectation
            for expectation in self.expectations
            if (expectation.expectation_type not in expectation_types)
        ]
        return removed_expectations

    def find_expectation_indexes(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        ge_cloud_id: str = None,
    ) -> List[int]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Find indexes of Expectations matching the given ExpectationConfiguration on the given match_type.\n        If a ge_cloud_id is provided, match_type is ignored and only indexes of Expectations\n        with matching ge_cloud_id are returned.\n\n        Args:\n            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to\n                find the index of any matching Expectation Configurations on the suite.\n            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based\n                on the data evaluated by that expectation, 'success' to match based on all configuration parameters\n                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match\n                 based on all configuration parameters\n            ge_cloud_id: Great Expectations Cloud id\n\n        Returns: A list of indexes of matching ExpectationConfiguration\n\n        Raises:\n            InvalidExpectationConfigurationError\n\n        "
        if (expectation_configuration is None) and (ge_cloud_id is None):
            raise TypeError(
                "Must provide either expectation_configuration or ge_cloud_id"
            )
        if expectation_configuration and (
            not isinstance(expectation_configuration, ExpectationConfiguration)
        ):
            raise InvalidExpectationConfigurationError(
                "Ensure that expectation configuration is valid."
            )
        match_indexes = []
        for (idx, expectation) in enumerate(self.expectations):
            if ge_cloud_id is not None:
                if str(expectation.ge_cloud_id) == str(ge_cloud_id):
                    match_indexes.append(idx)
            elif expectation.isEquivalentTo(expectation_configuration, match_type):
                match_indexes.append(idx)
        return match_indexes

    def find_expectations(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        ge_cloud_id: Optional[str] = None,
    ) -> List[ExpectationConfiguration]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Find Expectations matching the given ExpectationConfiguration on the given match_type.\n        If a ge_cloud_id is provided, match_type is ignored and only Expectations with matching\n        ge_cloud_id are returned.\n\n        Args:\n            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to\n                find the index of any matching Expectation Configurations on the suite.\n            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based\n                on the data evaluated by that expectation, 'success' to match based on all configuration parameters\n                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match\n                 based on all configuration parameters\n            ge_cloud_id: Great Expectations Cloud id\n\n        Returns: A list of matching ExpectationConfigurations\n        "
        if (expectation_configuration is None) and (ge_cloud_id is None):
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
        new_expectation_configuration: Union[(ExpectationConfiguration, dict)],
        existing_expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        ge_cloud_id: Optional[str] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Find Expectations matching the given ExpectationConfiguration on the given match_type.\n        If a ge_cloud_id is provided, match_type is ignored and only Expectations with matching\n        ge_cloud_id are returned.\n\n        Args:\n            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to\n                find the index of any matching Expectation Configurations on the suite.\n            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based\n                on the data evaluated by that expectation, 'success' to match based on all configuration parameters\n                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match\n                 based on all configuration parameters\n            ge_cloud_id: Great Expectations Cloud id\n\n        Returns: A list of matching ExpectationConfigurations\n        "
        if (existing_expectation_configuration is None) and (ge_cloud_id is None):
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
                "More than one matching expectation was found. Please be more specific with your search criteria"
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n\n        Args:\n             expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to\n                 find the expectation to patch.\n             op: A jsonpatch operation (one of 'add','update', or 'remove') (see http://jsonpatch.com/)\n             path: A jsonpatch path for the patch operation (see http://jsonpatch.com/)\n             value: The value to patch (see http://jsonpatch.com/)\n             match_type: The match type to use for find_expectation_index()\n\n        Returns: The patched ExpectationConfiguration\n\n        Raises:\n            No match\n            More than 1 match\n\n        "
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )
        if len(found_expectation_indexes) < 1:
            raise ValueError("No matching expectation was found.")
        elif len(found_expectation_indexes) > 1:
            raise ValueError(
                "More than one matching expectation was found. Please be more specific with your search criteria"
            )
        self.expectations[found_expectation_indexes[0]].patch(op, path, value)
        return self.expectations[found_expectation_indexes[0]]

    def _add_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        send_usage_event: bool = True,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> ExpectationConfiguration:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        This is a private method for adding expectations that allows for usage_events to be suppressed when\n        Expectations are added through internal processing (ie. while building profilers, rendering or validation). It\n        takes in send_usage_event boolean.\n\n        Args:\n            expectation_configuration: The ExpectationConfiguration to add or update\n            send_usage_event: Whether to send a usage_statistics event. When called through ExpectationSuite class'\n                public add_expectation() method, this is set to `True`.\n            match_type: The criteria used to determine whether the Suite already has an ExpectationConfiguration\n                and so whether we should add or replace.\n            overwrite_existing: If the expectation already exists, this will overwrite if True and raise an error if\n                False.\n\n        Returns:\n            The ExpectationConfiguration to add or replace.\n        Raises:\n            More than one match\n            One match if overwrite_existing = False\n        "
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )
        if len(found_expectation_indexes) > 1:
            if send_usage_event:
                self.send_usage_event(success=False)
            raise ValueError(
                "More than one matching expectation was found. Please be more specific with your search criteria"
            )
        elif len(found_expectation_indexes) == 1:
            if overwrite_existing:
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
                    "A matching ExpectationConfiguration already exists. If you would like to overwrite this ExpectationConfiguration, set overwrite_existing=True"
                )
        else:
            self.append_expectation(expectation_configuration)
        if send_usage_event:
            self.send_usage_event(success=True)
        return expectation_configuration

    def send_usage_event(self, success: bool) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        usage_stats_event_payload: dict = {}
        if self._data_context is not None:
            self._data_context.send_usage_message(
                event=UsageStatsEvents.EXPECTATION_SUITE_ADD_EXPECTATION.value,
                event_payload=usage_stats_event_payload,
                success=success,
            )

    def add_expectation_configurations(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        send_usage_event: bool = True,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> List[ExpectationConfiguration]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Args:\n            expectation_configurations: The List of candidate new/modifed "ExpectationConfiguration" objects for Suite.\n            send_usage_event: Whether to send a usage_statistics event. When called through ExpectationSuite class\'\n                public add_expectation() method, this is set to `True`.\n            match_type: The criteria used to determine whether the Suite already has an "ExpectationConfiguration"\n                object, matching the specified criteria, and thus whether we should add or replace (i.e., "upsert").\n            overwrite_existing: If "ExpectationConfiguration" already exists, this will cause it to be overwritten if\n                True and raise an error if False.\n\n        Returns:\n            The List of "ExpectationConfiguration" objects attempted to be added or replaced (can differ from the list\n            of "ExpectationConfiguration" objects in "self.expectations" at the completion of this method\'s execution).\n        Raises:\n            More than one match\n            One match if overwrite_existing = False\n        '
        expectation_configuration: ExpectationConfiguration
        expectation_configurations_attempted_to_be_added: List[
            ExpectationConfiguration
        ] = [
            self.add_expectation(
                expectation_configuration=expectation_configuration,
                send_usage_event=send_usage_event,
                match_type=match_type,
                overwrite_existing=overwrite_existing,
            )
            for expectation_configuration in expectation_configurations
        ]
        return expectation_configurations_attempted_to_be_added

    def add_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        send_usage_event: bool = True,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> ExpectationConfiguration:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Args:\n            expectation_configuration: The ExpectationConfiguration to add or update\n            send_usage_event: Whether to send a usage_statistics event. When called through ExpectationSuite class'\n                public add_expectation() method, this is set to `True`.\n            match_type: The criteria used to determine whether the Suite already has an ExpectationConfiguration\n                and so whether we should add or replace.\n            overwrite_existing: If the expectation already exists, this will overwrite if True and raise an error if\n                False.\n\n        Returns:\n            The ExpectationConfiguration to add or replace.\n        Raises:\n            More than one match\n            One match if overwrite_existing = False\n        "
        return self._add_expectation(
            expectation_configuration=expectation_configuration,
            send_usage_event=send_usage_event,
            match_type=match_type,
            overwrite_existing=overwrite_existing,
        )

    def get_grouped_and_ordered_expectations_by_column(
        self, expectation_type_filter: Optional[str] = None
    ) -> Tuple[(Dict[(str, List[ExpectationConfiguration])], List[str])]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        expectations_by_column = {}
        ordered_columns = []
        for expectation in self.expectations:
            if "column" in expectation.kwargs:
                column = expectation.kwargs["column"]
            else:
                column = "_nocolumn"
            if column not in expectations_by_column:
                expectations_by_column[column] = []
            if (expectation_type_filter is None) or (
                expectation.expectation_type == expectation_type_filter
            ):
                expectations_by_column[column].append(expectation)
            if (
                expectation.expectation_type
                == "expect_table_columns_to_match_ordered_list"
            ):
                exp_column_list = expectation.kwargs["column_list"]
                if exp_column_list and (len(exp_column_list) > 0):
                    ordered_columns = exp_column_list
        sorted_columns = sorted(list(expectations_by_column.keys()))
        if set(sorted_columns) == set(ordered_columns):
            return (expectations_by_column, ordered_columns)
        else:
            return (expectations_by_column, sorted_columns)


class ExpectationSuiteSchema(Schema):
    expectation_suite_name = fields.Str()
    ge_cloud_id = fields.UUID(required=False, allow_none=True)
    expectations = fields.List(fields.Nested(ExpectationConfigurationSchema))
    evaluation_parameters = fields.Dict(allow_none=True)
    data_asset_type = fields.Str(allow_none=True)
    meta = fields.Dict()

    def clean_empty(self, data):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if isinstance(data, ExpectationSuite):
            if not hasattr(data, "evaluation_parameters"):
                pass
            elif len(data.evaluation_parameters) == 0:
                del data.evaluation_parameters
            if not hasattr(data, "meta"):
                pass
            elif (data.meta is None) or (data.meta == []):
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
            elif (data.get("meta") is None) or (data.get("meta") == []):
                pass
            elif len(data.get("meta")) == 0:
                data.pop("meta")
        return data

    @pre_dump
    def prepare_dump(self, data, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        data = deepcopy(data)
        if isinstance(data, ExpectationSuite):
            data.meta = convert_to_json_serializable(data.meta)
        elif isinstance(data, dict):
            data["meta"] = convert_to_json_serializable(data.get("meta"))
        data = self.clean_empty(data)
        return data


expectationSuiteSchema = ExpectationSuiteSchema()
