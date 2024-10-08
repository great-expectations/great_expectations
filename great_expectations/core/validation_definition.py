from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Optional, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import (
    BaseModel,
    Extra,
    ValidationError,
    validator,
)
from great_expectations.constants import DATAFRAME_REPLACEMENT_STR
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
)
from great_expectations.core.freshness_diagnostics import (
    ValidationDefinitionFreshnessDiagnostics,
)
from great_expectations.core.result_format import DEFAULT_RESULT_FORMAT
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.serdes import _EncodedValidationData, _IdentifierBundle
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.exceptions import (
    ValidationDefinitionNotAddedError,
    ValidationDefinitionNotFreshError,
)
from great_expectations.exceptions.exceptions import (
    InvalidKeyError,
    StoreBackendError,
    ValidationDefinitionNotFoundError,
)
from great_expectations.validator.v1_validator import Validator

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.core.result_format import ResultFormatUnion
    from great_expectations.core.suite_parameters import SuiteParameterDict
    from great_expectations.data_context.store.validation_results_store import (
        ValidationResultsStore,
    )
    from great_expectations.datasource.fluent.batch_request import BatchParameters
    from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource


@public_api
class ValidationDefinition(BaseModel):
    """
    Responsible for running a suite against data and returning a validation result.

    Args:
        name: The name of the validation.
        data: A batch definition to validate.
        suite: A grouping of expectations to validate against the data.
        id: A unique identifier for the validation; added when persisted with a store.

    """

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True  # Necessary for compatibility with suite's Marshmallow dep
        copy_on_model_validation = (
            "none"  # Necessary to prevent cloning when passing to a checkpoint
        )
        validate_assignment = True
        """
        When serialized, the suite and data fields should be encoded as a set of identifiers.
        These will be used as foreign keys to retrieve the actual objects from the appropriate stores.

        Example:
        {
            "name": "my_validation",
            "data": {
                "datasource": {
                    "name": "my_datasource",
                    "id": "a758816-64c8-46cb-8f7e-03c12cea1d67"
                },
                "asset": {
                    "name": "my_asset",
                    "id": "b5s8816-64c8-46cb-8f7e-03c12cea1d67"
                },
                "batch_definition": {
                    "name": "my_batch_definition",
                    "id": "3a758816-64c8-46cb-8f7e-03c12cea1d67"
                }
            },
            "suite": {
                "name": "my_suite",
                "id": "8r2g816-64c8-46cb-8f7e-03c12cea1d67"
            },
            "id": "20dna816-64c8-46cb-8f7e-03c12cea1d67"
        }
        """  # noqa: E501
        json_encoders = {
            ExpectationSuite: lambda e: e.identifier_bundle(),
            BatchDefinition: lambda b: b.identifier_bundle(),
        }

    name: str
    data: BatchDefinition
    suite: ExpectationSuite
    id: Union[str, None] = None

    @property
    @public_api
    def batch_definition(self) -> BatchDefinition:
        return self.data

    @property
    @public_api
    def asset(self) -> DataAsset:
        return self.data.data_asset

    @property
    def data_source(self) -> Datasource:
        return self.asset.datasource

    @property
    def _validation_results_store(self) -> ValidationResultsStore:
        return project_manager.get_validation_results_store()

    def is_fresh(self) -> ValidationDefinitionFreshnessDiagnostics:
        validation_definition_diagnostics = ValidationDefinitionFreshnessDiagnostics(
            errors=[] if self.id else [ValidationDefinitionNotAddedError(name=self.name)]
        )
        suite_diagnostics = self.suite.is_fresh()
        data_diagnostics = self.data.is_fresh()
        validation_definition_diagnostics.update_with_children(suite_diagnostics, data_diagnostics)

        if not validation_definition_diagnostics.success:
            return validation_definition_diagnostics

        store = project_manager.get_validation_definition_store()
        key = store.get_key(name=self.name, id=self.id)

        try:
            validation_definition = store.get(key=key)
        except (
            StoreBackendError,  # Generic error from stores
            InvalidKeyError,  # Ephemeral context error
        ):
            return ValidationDefinitionFreshnessDiagnostics(
                errors=[ValidationDefinitionNotFoundError(name=self.name)]
            )

        return ValidationDefinitionFreshnessDiagnostics(
            errors=[]
            if self == validation_definition
            else [ValidationDefinitionNotFreshError(name=self.name)]
        )

    @validator("suite", pre=True)
    def _validate_suite(cls, v: dict | ExpectationSuite):
        # Input will be a dict of identifiers if being deserialized or a suite object if being constructed by a user.  # noqa: E501
        if isinstance(v, dict):
            return cls._decode_suite(v)
        elif isinstance(v, ExpectationSuite):
            return v
        raise ValueError(  # noqa: TRY003
            "Suite must be a dictionary (if being deserialized) or an ExpectationSuite object."
        )

    @validator("data", pre=True)
    def _validate_data(cls, v: dict | BatchDefinition):
        # Input will be a dict of identifiers if being deserialized or a rich type if being constructed by a user.  # noqa: E501
        if isinstance(v, dict):
            return cls._decode_data(v)
        elif isinstance(v, BatchDefinition):
            return v
        raise ValueError(  # noqa: TRY003
            "Data must be a dictionary (if being deserialized) or a BatchDefinition object."
        )

    @classmethod
    def _decode_suite(cls, suite_dict: dict) -> ExpectationSuite:
        # Take in raw JSON, ensure it contains appropriate identifiers, and use them to retrieve the actual suite.  # noqa: E501
        try:
            suite_identifiers = _IdentifierBundle.parse_obj(suite_dict)
        except ValidationError as e:
            raise ValueError("Serialized suite did not contain expected identifiers") from e  # noqa: TRY003

        name = suite_identifiers.name
        id = suite_identifiers.id

        expectation_store = project_manager.get_expectations_store()
        key = expectation_store.get_key(name=name, id=id)

        try:
            config: dict = expectation_store.get(key)
        except gx_exceptions.InvalidKeyError as e:
            raise ValueError(f"Could not find suite with name: {name} and id: {id}") from e  # noqa: TRY003

        suite = ExpectationSuite(**config)
        if suite._include_rendered_content:
            suite.render()
        return suite

    @classmethod
    def _decode_data(cls, data_dict: dict) -> BatchDefinition:
        # Take in raw JSON, ensure it contains appropriate identifiers, and use them to retrieve the actual data.  # noqa: E501
        try:
            data_identifiers = _EncodedValidationData.parse_obj(data_dict)
        except ValidationError as e:
            raise ValueError("Serialized data did not contain expected identifiers") from e  # noqa: TRY003

        ds_name = data_identifiers.datasource.name
        asset_name = data_identifiers.asset.name
        batch_definition_name = data_identifiers.batch_definition.name

        datasource_dict = project_manager.get_datasources()
        try:
            ds = datasource_dict[ds_name]
        except KeyError as e:
            raise ValueError(f"Could not find datasource named '{ds_name}'.") from e  # noqa: TRY003

        try:
            asset = ds.get_asset(asset_name)
        except LookupError as e:
            raise ValueError(  # noqa: TRY003
                f"Could not find asset named '{asset_name}' within '{ds_name}' datasource."
            ) from e

        try:
            batch_definition = asset.get_batch_definition(batch_definition_name)
        except KeyError as e:
            raise ValueError(  # noqa: TRY003
                f"Could not find batch definition named '{batch_definition_name}' within '{asset_name}' asset and '{ds_name}' datasource."  # noqa: E501
            ) from e

        return batch_definition

    @public_api
    def run(
        self,
        *,
        checkpoint_id: Optional[str] = None,
        batch_parameters: Optional[BatchParameters] = None,
        expectation_parameters: Optional[SuiteParameterDict] = None,
        result_format: ResultFormatUnion = DEFAULT_RESULT_FORMAT,
        run_id: RunIdentifier | None = None,
    ) -> ExpectationSuiteValidationResult:
        """
        Runs a validation using the configured data and suite.

        Args:
            batch_parameters: The dictionary of parameters necessary for selecting the
              correct batch to run the validation on. The keys are strings that are determined
              by the BatchDefinition used to instantiate this ValidationDefinition. For example:

              - whole table -> None
              - yearly -> year
              - monthly -> year, month
              - daily -> year, month, day

            expectation_parameters: A dictionary of parameters values for any expectations using
              parameterized values (the $PARAMETER syntax). The keys are the parameter names
              and the values are the values to be used for this validation run.
            result_format: A parameter controlling how much diagnostic information the result
              contains.
            checkpoint_id: This is used by the checkpoints code when it runs a validation
              definition. Otherwise, it should be None.
            run_id: An identifier for this run. Typically, this should be set to None and it will
              be generated by this call.
        """
        diagnostics = self.is_fresh()
        if not diagnostics.success:
            # The validation definition itself is not added but all children are - we can add it for the user # noqa: E501
            if not diagnostics.parent_added and diagnostics.children_added:
                self._add_to_store()
            else:
                diagnostics.raise_for_error()

        validator = Validator(
            batch_definition=self.batch_definition,
            batch_parameters=batch_parameters,
            result_format=result_format,
        )
        results = validator.validate_expectation_suite(self.suite, expectation_parameters)
        results.meta["validation_id"] = self.id
        results.meta["checkpoint_id"] = checkpoint_id

        # NOTE: We should promote this to a top-level field of the result.
        #       Meta should be reserved for user-defined information.
        if run_id:
            results.meta["run_id"] = run_id
            results.meta["validation_time"] = run_id.run_time
        if batch_parameters:
            batch_parameters_copy = {k: v for k, v in batch_parameters.items()}
            if "dataframe" in batch_parameters_copy:
                batch_parameters_copy["dataframe"] = DATAFRAME_REPLACEMENT_STR
            results.meta["batch_parameters"] = batch_parameters_copy
        else:
            results.meta["batch_parameters"] = None

        (
            expectation_suite_identifier,
            validation_result_id,
        ) = self._get_expectation_suite_and_validation_result_ids(
            validator=validator, run_id=run_id
        )

        ref = self._validation_results_store.store_validation_results(
            suite_validation_result=results,
            suite_validation_result_identifier=validation_result_id,
            expectation_suite_identifier=expectation_suite_identifier,
        )

        if isinstance(ref, GXCloudResourceRef):
            results.id = ref.id
            # FIXME(cdkini): There is currently a bug in GX Cloud where the result_url is None
            results.result_url = self._validation_results_store.parse_result_url_from_gx_cloud_ref(
                ref
            )

        return results

    def _get_expectation_suite_and_validation_result_ids(
        self,
        validator: Validator,
        run_id: RunIdentifier | None = None,
    ) -> (
        tuple[GXCloudIdentifier, GXCloudIdentifier]
        | tuple[ExpectationSuiteIdentifier, ValidationResultIdentifier]
    ):
        expectation_suite_identifier: GXCloudIdentifier | ExpectationSuiteIdentifier
        validation_result_id: GXCloudIdentifier | ValidationResultIdentifier
        if self._validation_results_store.cloud_mode:
            expectation_suite_identifier = GXCloudIdentifier(
                resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
                id=self.suite.id,
            )
            validation_result_id = GXCloudIdentifier(
                resource_type=GXCloudRESTResource.VALIDATION_RESULT
            )
            return expectation_suite_identifier, validation_result_id
        else:
            run_id = run_id or RunIdentifier(
                run_time=datetime.datetime.now(tz=datetime.timezone.utc)
            )
            expectation_suite_identifier = ExpectationSuiteIdentifier(name=self.suite.name)
            validation_result_id = ValidationResultIdentifier(
                batch_identifier=validator.active_batch_id,
                expectation_suite_identifier=expectation_suite_identifier,
                run_id=run_id,
            )
            return expectation_suite_identifier, validation_result_id

    def identifier_bundle(self) -> _IdentifierBundle:
        # Utilized as a custom json_encoder
        diagnostics = self.is_fresh()
        diagnostics.raise_for_error()

        return _IdentifierBundle(name=self.name, id=self.id)

    @public_api
    def save(self) -> None:
        store = project_manager.get_validation_definition_store()
        key = store.get_key(name=self.name, id=self.id)

        store.update(key=key, value=self)

    def _add_to_store(self) -> None:
        """This is used to persist a validation_definition before we run it.

        We need to persist a validation_definition before it can be run. If user calls runs but
        hasn't persisted it we add it for them."""
        store = project_manager.get_validation_definition_store()
        key = store.get_key(name=self.name, id=self.id)

        store.add(key=key, value=self)
