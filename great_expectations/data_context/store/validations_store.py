from great_expectations.core import ExpectationSuiteValidationResultSchema
from great_expectations.data_context.store.database_store_backend import (
    DatabaseStoreBackend,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import load_class
from great_expectations.util import verify_dynamic_loading_support


class ValidationsStore(Store):
    """
A ValidationsStore manages Validation Results to ensure they are accessible via a Data Context for review and rendering into Data Docs.

--ge-feature-maturity-info--

    id: validations_store_filesystem
    title: Validations Store - Filesystem
    icon:
    short_description: Filesystem
    description: Use a locally-mounted filesystem to store validation results.
    how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_metadata_stores/how_to_configure_a_validation_result_store_on_a_filesystem.html
    maturity: Production
    maturity_details:
        api_stability: Stable
        implementation_completeness: Complete
        unit_test_coverage: Complete
        integration_infrastructure_test_coverage: N/A
        documentation_completeness: Complete
        bug_risk: Low

    id: validations_store_s3
    title: Validations Store - S3
    icon:
    short_description: S3
    description: Use an Amazon Web Services S3 bucket to store validation results.
    how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_s3.html
    maturity: Beta
    maturity_details:
        api_stability: Stable
        implementation_completeness: Complete
        unit_test_coverage: Complete
        integration_infrastructure_test_coverage: Minimal
        documentation_completeness: Complete
        bug_risk: Low

    id: validations_store_gcs
    title: Validations Store - GCS
    icon:
    short_description:
    description: # What it does  <br /> Store validation results in a Google Cloud Storage bucket. You may optionally specify a key to use. <br /> <br /> See the GCS Store backend [module docs](https://docs.greatexpectations.io/en/latest/autoapi/great_expectations/data_context/store/tuple_store_backend/index.html#great_expectations.data_context.store.tuple_store_backend.TupleGCSStoreBackend) for more information."
    how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.html
    maturity: Beta
    maturity_details:
        api_stability: Stable
        implementation_completeness: Complete
        unit_test_coverage: Complete
        integration_infrastructure_test_coverage: Minimal
        documentation_completeness: Partial
        bug_risk: Low

    id: validations_store_azure_blob_storage
    title: Validations Store - Azure
    icon:
    short_description: Azure Blob Storage
    description: Use Microsoft Azure Blob Storage to store validation results.
    how_to_guide_url:
    maturity: N/A
    maturity_details:
        api_stability: Stable
        implementation_completeness: Minimal
        unit_test_coverage: Minimal
        integration_infrastructure_test_coverage: Minimal
        documentation_completeness: Minimal
        bug_risk: Moderate

--ge-feature-maturity-info--
    """

    _key_class = ValidationResultIdentifier

    def __init__(self, store_backend=None, runtime_environment=None):
        self._expectationSuiteValidationResultSchema = (
            ExpectationSuiteValidationResultSchema()
        )

        if store_backend is not None:
            store_backend_module_name = store_backend.get(
                "module_name", "great_expectations.data_context.store"
            )
            store_backend_class_name = store_backend.get(
                "class_name", "InMemoryStoreBackend"
            )
            verify_dynamic_loading_support(module_name=store_backend_module_name)
            store_backend_class = load_class(
                store_backend_class_name, store_backend_module_name
            )

            # Store Backend Class was loaded successfully; verify that it is of a correct subclass.
            if issubclass(store_backend_class, TupleStoreBackend):
                # Provide defaults for this common case
                store_backend["filepath_suffix"] = store_backend.get(
                    "filepath_suffix", ".json"
                )
            elif issubclass(store_backend_class, DatabaseStoreBackend):
                # Provide defaults for this common case
                store_backend["table_name"] = store_backend.get(
                    "table_name", "ge_validations_store"
                )
                store_backend["key_columns"] = store_backend.get(
                    "key_columns",
                    [
                        "expectation_suite_name",
                        "run_name",
                        "run_time",
                        "batch_identifier",
                    ],
                )
        super().__init__(
            store_backend=store_backend, runtime_environment=runtime_environment
        )

    def serialize(self, key, value):
        return self._expectationSuiteValidationResultSchema.dumps(value)

    def deserialize(self, key, value):
        return self._expectationSuiteValidationResultSchema.loads(value)
