from __future__ import annotations

import json
import logging
import uuid
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from marshmallow import Schema, fields, post_dump, post_load, pre_dump

import great_expectations.exceptions as gx_exceptions
from great_expectations import __version__ as ge_version
from great_expectations._docs_decorators import (
    public_api,
)
from great_expectations.analytics.anonymizer import anonymize
from great_expectations.analytics.client import submit as submit_event
from great_expectations.analytics.events import (
    ExpectationSuiteExpectationCreatedEvent,
    ExpectationSuiteExpectationDeletedEvent,
    ExpectationSuiteExpectationUpdatedEvent,
)
from great_expectations.compatibility.pydantic import ValidationError as PydanticValidationError
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.freshness_diagnostics import (
    ExpectationSuiteFreshnessDiagnostics,
)
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.exceptions import (
    ExpectationSuiteError,
    ExpectationSuiteNotAddedError,
    ExpectationSuiteNotFoundError,
    ExpectationSuiteNotFreshError,
    StoreBackendError,
)
from great_expectations.exceptions.exceptions import InvalidKeyError
from great_expectations.types import SerializableDictDot
from great_expectations.util import (
    convert_to_json_serializable,  # noqa: TID251
    ensure_json_serializable,  # noqa: TID251
)

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues
    from great_expectations.data_context.store.expectations_store import ExpectationsStore
    from great_expectations.expectations.expectation import Expectation
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )

    _TExpectation = TypeVar("_TExpectation", bound=Expectation)

logger = logging.getLogger(__name__)


@public_api
class ExpectationSuite(SerializableDictDot):
    """Set-like collection of Expectations.

    Args:
        name: Name of the Expectation Suite
        expectations: Expectation Configurations to associate with this Expectation Suite.
        suite_parameters: Suite parameters to be substituted when evaluating Expectations.
        meta: Metadata related to the suite.
        id: Great Expectations Cloud id for this Expectation Suite.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: Optional[str] = None,
        expectations: Optional[Sequence[Union[dict, ExpectationConfiguration, Expectation]]] = None,
        suite_parameters: Optional[dict] = None,
        meta: Optional[dict] = None,
        notes: str | list[str] | None = None,
        id: Optional[str] = None,
    ) -> None:
        if not name or not isinstance(name, str):
            raise ValueError("name must be provided as a non-empty string")  # noqa: TRY003
        self.name = name
        self.id = id

        self.expectations = []
        for exp in expectations or []:
            try:
                self.expectations.append(self._process_expectation(exp))
            except gx_exceptions.InvalidExpectationConfigurationError as e:
                logger.exception(
                    f"Could not add expectation; provided configuration is not valid: {e.message}"
                )

        if suite_parameters is None:
            suite_parameters = {}
        self.suite_parameters = suite_parameters
        if meta is None:
            meta = {"great_expectations_version": ge_version}
        if (
            "great_expectations.__version__" not in meta
            and "great_expectations_version" not in meta
        ):
            meta["great_expectations_version"] = ge_version
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta
        self.notes = notes

    @property
    def _store(self) -> ExpectationsStore:
        return project_manager.get_expectations_store()

    @property
    def _include_rendered_content(self) -> bool:
        return project_manager.is_using_cloud()

    @property
    def suite_parameter_options(self) -> tuple[str, ...]:
        """SuiteParameter options for this ExpectationSuite.

        Returns:
            tuple[str, ...]: The keys of the suite parameters used by all Expectations of this suite at runtime.
        """  # noqa: E501
        output: set[str] = set()
        for expectation in self.expectations:
            output.update(expectation.suite_parameter_options)
        return tuple(sorted(output))

    @public_api
    def add_expectation(self, expectation: _TExpectation) -> _TExpectation:
        """Add an Expectation to the collection."""
        if expectation.id:
            raise RuntimeError(  # noqa: TRY003
                "Cannot add Expectation because it already belongs to an ExpectationSuite. "
                "If you want to update an existing Expectation, please call Expectation.save(). "
                "If you are copying this Expectation to a new ExpectationSuite, please copy "
                "it first (the core expectations and some others support copy(expectation)) "
                "and set `Expectation.id = None`."
            )
        should_save_expectation = self._has_been_saved()
        expectation_is_unique = all(
            expectation.configuration != existing_expectation.configuration
            for existing_expectation in self.expectations
        )
        if expectation_is_unique:
            # suite is a set-like collection, so don't add if it not unique
            self.expectations.append(expectation)
            if should_save_expectation:
                try:
                    expectation = self._store.add_expectation(suite=self, expectation=expectation)
                    self.expectations[-1].id = expectation.id
                except Exception as exc:
                    self.expectations.pop()
                    raise exc  # noqa: TRY201

        expectation.register_save_callback(save_callback=self._save_expectation)

        self._submit_expectation_created_event(expectation=expectation)

        return expectation

    def _submit_expectation_created_event(self, expectation: Expectation) -> None:
        if expectation.__module__.startswith("great_expectations."):
            custom_exp_type = False
            expectation_type = expectation.expectation_type
        else:
            custom_exp_type = True
            expectation_type = anonymize(expectation.expectation_type)

        submit_event(
            event=ExpectationSuiteExpectationCreatedEvent(
                expectation_id=expectation.id,
                expectation_suite_id=self.id,
                expectation_type=expectation_type,
                custom_exp_type=custom_exp_type,
            )
        )

    def _process_expectation(
        self, expectation_like: Union[Expectation, ExpectationConfiguration, dict]
    ) -> Expectation:
        """Transform an Expectation from one of its various serialized forms to the Expectation type,
        and bind it to this ExpectationSuite.

        Raises:
            ValueError: If expectation_like is of type Expectation and expectation_like.id is not None.
        """  # noqa: E501
        from great_expectations.expectations.expectation import Expectation
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        if isinstance(expectation_like, Expectation):
            if expectation_like.id:
                raise ValueError(  # noqa: TRY003
                    "Expectations in parameter `expectations` must not belong to another ExpectationSuite. "  # noqa: E501
                    "Instead, please use copies of Expectations, by calling `copy.copy(expectation)`."  # noqa: E501
                )
            expectation_like.register_save_callback(save_callback=self._save_expectation)
            return expectation_like
        elif isinstance(expectation_like, ExpectationConfiguration):
            return self._build_expectation(expectation_configuration=expectation_like)
        elif isinstance(expectation_like, dict):
            return self._build_expectation(
                expectation_configuration=ExpectationConfiguration(**expectation_like)
            )
        else:
            raise TypeError(  # noqa: TRY003
                f"Expected Expectation, ExpectationConfiguration, or dict, but received type {type(expectation_like)}."  # noqa: E501
            )

    @public_api
    def delete_expectation(self, expectation: Expectation) -> Expectation:
        """Delete an Expectation from the collection.

        Raises:
            KeyError: Expectation not found in suite.
        """
        remaining_expectations = [
            exp for exp in self.expectations if exp.configuration != expectation.configuration
        ]
        if len(remaining_expectations) != len(self.expectations) - 1:
            raise KeyError("No matching expectation was found.")  # noqa: TRY003
        self.expectations = remaining_expectations

        if self._has_been_saved():
            # only persist on delete if the suite has already been saved
            try:
                self._store.delete_expectation(suite=self, expectation=expectation)
            except Exception as exc:
                # rollback this change
                # expectation suite is set-like so order of expectations doesn't matter
                self.expectations.append(expectation)
                raise exc  # noqa: TRY201

        submit_event(
            event=ExpectationSuiteExpectationDeletedEvent(
                expectation_id=expectation.id,
                expectation_suite_id=self.id,
            )
        )

        return expectation

    @public_api
    def save(self) -> None:
        """Save this ExpectationSuite."""
        # TODO: Need to emit an event from here - we've opted out of an ExpectationSuiteUpdated event for now  # noqa: E501
        if self._include_rendered_content:
            self.render()
        key = self._store.get_key(name=self.name, id=self.id)
        self._store.update(key=key, value=self)

    def is_fresh(self) -> ExpectationSuiteFreshnessDiagnostics:
        diagnostics = self._is_added()
        if not diagnostics.success:
            return diagnostics
        return self._is_fresh()

    def _is_added(self) -> ExpectationSuiteFreshnessDiagnostics:
        return ExpectationSuiteFreshnessDiagnostics(
            errors=[] if self.id else [ExpectationSuiteNotAddedError(name=self.name)]
        )

    def _is_fresh(self) -> ExpectationSuiteFreshnessDiagnostics:
        suite_dict: dict | None
        try:
            key = self._store.get_key(name=self.name, id=self.id)
            suite_dict = self._store.get(key=key)
        except (
            StoreBackendError,  # Generic error from stores
            InvalidKeyError,  # Ephemeral context error
        ):
            suite_dict = None
        if not suite_dict:
            return ExpectationSuiteFreshnessDiagnostics(
                errors=[ExpectationSuiteNotFoundError(name=self.name)]
            )

        suite: ExpectationSuite | None
        try:
            suite = self._store.deserialize_suite_dict(suite_dict=suite_dict)
        except PydanticValidationError:
            suite = None
        if not suite:
            return ExpectationSuiteFreshnessDiagnostics(
                errors=[ExpectationSuiteError(f"Could not deserialize suite '{self.name}'")]
            )

        return ExpectationSuiteFreshnessDiagnostics(
            errors=[] if self == suite else [ExpectationSuiteNotFreshError(name=self.name)]
        )

    def _has_been_saved(self) -> bool:
        """Has this ExpectationSuite been persisted to a Store?"""
        # todo: this should only check local keys instead of potentially querying the remote backend
        key = self._store.get_key(name=self.name, id=self.id)
        return self._store.has_key(key=key)

    def _save_expectation(self, expectation) -> Expectation:
        expectation = self._store.update_expectation(suite=self, expectation=expectation)
        submit_event(
            event=ExpectationSuiteExpectationUpdatedEvent(
                expectation_id=expectation.id, expectation_suite_id=self.id
            )
        )
        return expectation

    @property
    def expectation_configurations(self) -> list[ExpectationConfiguration]:
        return [exp.configuration for exp in self.expectations]

    @expectation_configurations.setter
    def expectation_configurations(self, value):
        raise AttributeError(  # noqa: TRY003
            "Cannot set ExpectationSuite.expectation_configurations. "
            "Please use ExpectationSuite.expectations instead."
        )

    def __eq__(self, other):  # type: ignore[explicit-override] # FIXME
        """ExpectationSuite equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all(
            (
                self.name == other.name,
                self.expectations == other.expectations,
                self.suite_parameters == other.suite_parameters,
                self.meta == other.meta,
            )
        )

    def __ne__(self, other):  # type: ignore[explicit-override] # FIXME
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):  # type: ignore[explicit-override] # FIXME
        return json.dumps(self.to_json_dict(), indent=2)

    @override
    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __deepcopy__(self, memo: dict):
        cls = self.__class__
        result = cls.__new__(cls)

        memo[id(self)] = result

        attributes_to_copy = set(ExpectationSuiteSchema().fields.keys())
        for key in attributes_to_copy:
            setattr(result, key, deepcopy(getattr(self, key), memo))

        return result

    @public_api
    @override
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this ExpectationSuite.

        Returns:
            A JSON-serializable dict representation of this ExpectationSuite.
        """
        myself = expectationSuiteSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed  # noqa: E501
        # schemas to get serialization all-the-way down via dump
        expectation_configurations = [exp.configuration for exp in self.expectations]
        myself["expectations"] = convert_to_json_serializable(expectation_configurations)
        try:
            myself["suite_parameters"] = convert_to_json_serializable(myself["suite_parameters"])
        except KeyError:
            pass  # Allow suite parameters to be missing if empty
        myself["meta"] = convert_to_json_serializable(myself["meta"])
        return myself

    def remove_expectation(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        remove_multiple_matches: bool = False,
        id: Optional[Union[str, uuid.UUID]] = None,
    ) -> List[ExpectationConfiguration]:
        """Remove an ExpectationConfiguration from the ExpectationSuite.

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                based on all configuration parameters.
            remove_multiple_matches: If True, will remove multiple matching expectations.
            id: Great Expectations Cloud id for an Expectation.

        Returns:
            The list of deleted ExpectationConfigurations.

        Raises:
            TypeError: Must provide either expectation_configuration or id.
            ValueError: No match or multiple matches found (and remove_multiple_matches=False).
        """  # noqa: E501
        expectation_configurations = [exp.configuration for exp in self.expectations]
        if expectation_configuration is None and id is None:
            raise TypeError("Must provide either expectation_configuration or id")  # noqa: TRY003

        found_expectation_indexes = self._find_expectation_indexes(
            expectation_configuration=expectation_configuration,
            match_type=match_type,
            id=id,  # type: ignore[arg-type]
        )
        if len(found_expectation_indexes) < 1:
            raise ValueError("No matching expectation was found.")  # noqa: TRY003

        elif len(found_expectation_indexes) > 1:
            if remove_multiple_matches:
                removed_expectations = []
                for index in sorted(found_expectation_indexes, reverse=True):
                    removed_expectations.append(expectation_configurations.pop(index))
                self.expectations = [
                    self._build_expectation(config) for config in expectation_configurations
                ]
                return removed_expectations
            else:
                raise ValueError(  # noqa: TRY003
                    "More than one matching expectation was found. Specify more precise matching criteria,"  # noqa: E501
                    "or set remove_multiple_matches=True"
                )

        else:
            result = [expectation_configurations.pop(found_expectation_indexes[0])]
            self.expectations = [
                self._build_expectation(config) for config in expectation_configurations
            ]
            return result

    def _find_expectation_indexes(
        self,
        expectation_configuration: Optional[ExpectationConfiguration] = None,
        match_type: str = "domain",
        id: Optional[str] = None,
    ) -> List[int]:
        """
        Find indexes of Expectations matching the given ExpectationConfiguration on the given match_type.
        If a id is provided, match_type is ignored and only indexes of Expectations
        with matching id are returned.

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to
                find the index of any matching Expectation Configurations on the suite.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                 based on all configuration parameters
            id: Great Expectations Cloud id

        Returns: A list of indexes of matching ExpectationConfiguration

        Raises:
            InvalidExpectationConfigurationError

        """  # noqa: E501
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        if expectation_configuration is None and id is None:
            raise TypeError("Must provide either expectation_configuration or id")  # noqa: TRY003

        if expectation_configuration and not isinstance(
            expectation_configuration, ExpectationConfiguration
        ):
            raise gx_exceptions.InvalidExpectationConfigurationError(  # noqa: TRY003
                "Ensure that expectation configuration is valid."
            )

        match_indexes = []
        for idx, expectation in enumerate(self.expectations):
            if id is not None:
                if expectation.id == id:
                    match_indexes.append(idx)
            else:  # noqa: PLR5501
                if expectation.configuration.isEquivalentTo(
                    other=expectation_configuration,  # type: ignore[arg-type]
                    match_type=match_type,
                ):
                    match_indexes.append(idx)

        return match_indexes

    def _add_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> ExpectationConfiguration:
        """
        If successful, upserts ExpectationConfiguration into this ExpectationSuite.

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
        """  # noqa: E501

        found_expectation_indexes = self._find_expectation_indexes(
            expectation_configuration=expectation_configuration, match_type=match_type
        )

        if len(found_expectation_indexes) > 1:
            raise ValueError(  # noqa: TRY003
                "More than one matching expectation was found. Please be more specific with your search "  # noqa: E501
                "criteria"
            )
        elif len(found_expectation_indexes) == 1:
            # Currently, we completely replace the expectation_configuration, but we could potentially use patch_expectation  # noqa: E501
            # to update instead. We need to consider how to handle meta in that situation.
            # patch_expectation = jsonpatch.make_patch(self.expectations[found_expectation_index] \
            #   .kwargs, expectation_configuration.kwargs)
            # patch_expectation.apply(self.expectations[found_expectation_index].kwargs, in_place=True)  # noqa: E501
            if overwrite_existing:
                # if existing Expectation has a id, add it back to the new Expectation Configuration
                existing_expectation_id = self.expectations[found_expectation_indexes[0]].id
                if existing_expectation_id is not None:
                    expectation_configuration.id = existing_expectation_id

                self.expectations[found_expectation_indexes[0]] = self._build_expectation(
                    expectation_configuration=expectation_configuration
                )
            else:
                raise gx_exceptions.DataContextError(  # noqa: TRY003
                    "A matching ExpectationConfiguration already exists. If you would like to overwrite this "  # noqa: E501
                    "ExpectationConfiguration, set overwrite_existing=True"
                )
        else:
            self.expectations.append(
                self._build_expectation(expectation_configuration=expectation_configuration)
            )

        return expectation_configuration

    def add_expectation_configurations(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> List[ExpectationConfiguration]:
        """Upsert a list of ExpectationConfigurations into this ExpectationSuite.

        Args:
            expectation_configurations: The List of candidate new/modifed "ExpectationConfiguration" objects for Suite.
            match_type: The criteria used to determine whether the Suite already has an "ExpectationConfiguration"
                object, matching the specified criteria, and thus whether we should add or replace (i.e., "upsert").
            overwrite_existing: If "ExpectationConfiguration" already exists, this will cause it to be overwritten if
                True and raise an error if False.

        Returns:
            The List of "ExpectationConfiguration" objects attempted to be added or replaced (can differ from the list
            of "ExpectationConfiguration" objects in "self.expectations" at the completion of this method's execution).

        Raises:
            More than one match
            One match if overwrite_existing = False
        """  # noqa: E501
        expectation_configuration: ExpectationConfiguration
        expectation_configurations_attempted_to_be_added: List[ExpectationConfiguration] = [
            self.add_expectation_configuration(
                expectation_configuration=expectation_configuration,
                match_type=match_type,
                overwrite_existing=overwrite_existing,
            )
            for expectation_configuration in expectation_configurations
        ]
        return expectation_configurations_attempted_to_be_added

    def add_expectation_configuration(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> ExpectationConfiguration:
        """Upsert specified ExpectationConfiguration into this ExpectationSuite.

        Args:
            expectation_configuration: The ExpectationConfiguration to add or update.
            match_type: The criteria used to determine whether the Suite already has an ExpectationConfiguration
                and so whether we should add or replace.
            overwrite_existing: If the expectation already exists, this will overwrite if True and raise an error if
                False.

        Returns:
            The ExpectationConfiguration to add or replace.

        Raises:
            ValueError: More than one match
            DataContextError: One match if overwrite_existing = False

        # noqa: DAR402
        """  # noqa: E501
        self._build_expectation(expectation_configuration)
        return self._add_expectation(
            expectation_configuration=expectation_configuration,
            match_type=match_type,
            overwrite_existing=overwrite_existing,
        )

    def _build_expectation(
        self, expectation_configuration: ExpectationConfiguration
    ) -> Expectation:
        try:
            expectation = expectation_configuration.to_domain_obj()
            expectation.register_save_callback(save_callback=self._save_expectation)
            return expectation
        except (
            gx_exceptions.ExpectationNotFoundError,
            gx_exceptions.InvalidExpectationConfigurationError,
        ) as e:
            raise gx_exceptions.InvalidExpectationConfigurationError(  # noqa: TRY003
                f"Could not add expectation; provided configuration is not valid: {e.message}"
            ) from e

    def render(self) -> None:
        """
        Renders content using the atomic prescriptive renderer for each expectation configuration associated with
           this ExpectationSuite to ExpectationConfiguration.rendered_content.
        """  # noqa: E501
        for expectation in self.expectations:
            expectation.render()

    def identifier_bundle(self) -> _IdentifierBundle:
        # Utilized as a custom json_encoder
        diagnostics = self.is_fresh()
        diagnostics.raise_for_error()

        return _IdentifierBundle(name=self.name, id=self.id)


_TExpectationSuite = TypeVar("_TExpectationSuite", ExpectationSuite, dict)


class ExpectationSuiteSchema(Schema):
    name = fields.Str()
    id = fields.UUID(required=False, allow_none=True)
    expectations = fields.List(fields.Nested("ExpectationConfigurationSchema"))
    suite_parameters = fields.Dict(allow_none=True)
    meta = fields.Dict()
    notes = fields.Raw(required=False, allow_none=True)

    # NOTE: 20191107 - JPC - we may want to remove clean_empty and update tests to require the other fields;  # noqa: E501
    # doing so could also allow us not to have to make a copy of data in the pre_dump method.
    # noinspection PyMethodMayBeStatic
    def clean_empty(self, data: _TExpectationSuite) -> _TExpectationSuite:
        if isinstance(data, ExpectationSuite):
            # We are hitting this TypeVar narrowing mypy bug: https://github.com/python/mypy/issues/10817
            data = self._clean_empty_suite(data)  # type: ignore[assignment]
        elif isinstance(data, dict):
            data = self._clean_empty_dict(data)
        return data

    @staticmethod
    def _clean_empty_suite(data: ExpectationSuite) -> ExpectationSuite:
        if not hasattr(data, "suite_parameters"):
            pass
        elif len(data.suite_parameters) == 0:
            del data.suite_parameters

        if not hasattr(data, "meta") or (data.meta is None or data.meta == []):
            pass
        elif len(data.meta) == 0:
            del data.meta
        return data

    @staticmethod
    def _clean_empty_dict(data: dict) -> dict:
        if "suite_parameters" in data and len(data["suite_parameters"]) == 0:
            data.pop("suite_parameters")
        if "meta" in data and len(data["meta"]) == 0:
            data.pop("meta")
        if "notes" in data and not data.get("notes"):
            data.pop("notes")
        return data

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        for key in data:
            if key.startswith("_"):
                continue
            data[key] = convert_to_json_serializable(data[key])

        data = self.clean_empty(data)
        return data

    @post_dump(pass_original=True)
    def insert_expectations(self, data, original_data, **kwargs) -> dict:
        if isinstance(original_data, dict):
            expectations = original_data.get("expectations", [])
        else:
            expectations = original_data.expectation_configurations
        data["expectations"] = convert_to_json_serializable(expectations)
        return data

    @post_load
    def _convert_uuids_to_str(self, data, **kwargs):
        """
        Utilize UUID for data validation but convert to string before usage in business logic
        """
        attr = "id"
        uuid_val = data.get(attr)
        if uuid_val:
            data[attr] = str(uuid_val)
        return data


expectationSuiteSchema: ExpectationSuiteSchema = ExpectationSuiteSchema()
