from __future__ import annotations

import datetime
import hashlib
import keyword
import logging
import pickle
from functools import partial
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Union,
    cast,
    overload,
)

import pandas as pd
from packaging.version import Version

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import aws, azure, google
from great_expectations.compatibility.sqlalchemy_and_pandas import (
    execute_pandas_reader_fn,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch import BatchMarkers
from great_expectations.core.batch_spec import (
    AzureBatchSpec,
    BatchSpec,
    FabricBatchSpec,
    GCSBatchSpec,
    PandasBatchSpec,
    PandasBatchSpecProtocol,
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.core.metric_domain_types import (
    MetricDomainTypes,  # noqa: TC001 # FIXME CoP
)
from great_expectations.core.util import AzureUrl, GCSUrl, S3Url, sniff_s3_compression
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.execution_engine import (
    PartitionDomainKwargs,  # noqa: TC001 # FIXME CoP
)
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.execution_engine.partition_and_sample.pandas_data_partitioner import (
    PandasDataPartitioner,
)
from great_expectations.execution_engine.partition_and_sample.pandas_data_sampler import (
    PandasDataSampler,
)
from great_expectations.expectations.model_field_types import CONDITION_PARSER_PANDAS
from great_expectations.expectations.row_conditions import (
    AndCondition,
    ComparisonCondition,
    Condition,
    NullityCondition,
    Operator,
    OrCondition,
    PassThroughCondition,
    deserialize_row_condition,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient
    from typing_extensions import TypeAlias

    from great_expectations.validator.metric_configuration import MetricConfigurationID

logger = logging.getLogger(__name__)


HASH_THRESHOLD = 1e9

DataFrameFactoryFn: TypeAlias = Callable[..., pd.DataFrame]


def query_column_reference(name: str) -> str:
    """Render *name* so ``DataFrame.query`` reads it as one column rather than as an expression.

    pandas parses the expression it is given, so a column that is not a plain Python identifier
    (``"Total Amount"``, ``"a.b"``) or that collides with a Python keyword (``"from"``) has to be
    wrapped in backticks, pandas' own escape for this. Names that already parse as themselves are
    left untouched.
    """
    if name.isidentifier() and not keyword.iskeyword(name):
        return name
    return f"`{name}`"


class PandasExecutionEngine(ExecutionEngine[str]):
    """PandasExecutionEngine instantiates the ExecutionEngine API to support computations using Pandas.

    Constructor builds a PandasExecutionEngine, using provided configuration options.

    Args:
        *args: Positional arguments for configuring PandasExecutionEngine
        **kwargs: Keyword arguments for configuring PandasExecutionEngine

    For example:
    ```python
        execution_engine: ExecutionEngine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    ```

    --ge-feature-maturity-info--

        id: validation_engine_pandas
        title: Validation Engine - Pandas
        icon:
        short_description: Use Pandas DataFrame to validate data
        description: Use Pandas DataFrame to validate data
        how_to_guide_url:
        maturity: Production
        maturity_details:
            api_stability: Stable
            implementation_completeness: Complete
            unit_test_coverage: Complete
            integration_infrastructure_test_coverage: N/A -> see relevant Datasource evaluation
            documentation_completeness: Complete
            bug_risk: Low
            expectation_completeness: Complete

    --ge-feature-maturity-info--
    """  # noqa: E501 # FIXME CoP

    recognized_batch_spec_defaults = {
        "reader_method",
        "reader_options",
    }

    def __init__(self, *args, **kwargs) -> None:
        self.discard_subset_failing_expectations = kwargs.pop(
            "discard_subset_failing_expectations", False
        )
        boto3_options: Dict[str, dict] = kwargs.pop("boto3_options", {})
        azure_options: Dict[str, dict] = kwargs.pop("azure_options", {})
        gcs_options: Dict[str, dict] = kwargs.pop("gcs_options", {})
        s3_client = kwargs.pop("s3_client", None)

        # Instantiate cloud provider clients as None at first.
        # They will be instantiated if/when passed cloud-specific in BatchSpec is passed in
        self._s3: BaseClient | None = None
        self._azure: azure.BlobServiceClient | None = None
        self._gcs: google.Client | None = None

        super().__init__(*args, **kwargs)

        self._config.update(
            {
                "discard_subset_failing_expectations": self.discard_subset_failing_expectations,
                "boto3_options": boto3_options,
                "azure_options": azure_options,
                "gcs_options": gcs_options,
                "s3_client": s3_client,
            }
        )

        self._data_partitioner = PandasDataPartitioner()
        self._data_sampler = PandasDataSampler()

    def _instantiate_azure_client(self) -> None:
        self._azure = None
        if azure.BlobServiceClient:  # type: ignore[truthy-function] # False if NotImported
            azure_options = self.config.get("azure_options", {})
            try:
                if "conn_str" in azure_options:
                    self._azure = azure.BlobServiceClient.from_connection_string(**azure_options)
                else:
                    self._azure = azure.BlobServiceClient(**azure_options)
            except (TypeError, AttributeError):
                # If exception occurs, then "self._azure = None" remains in effect.
                pass

    def _instantiate_s3_client(self) -> None:
        # If s3_client was passed in (from data source) use it, otherwise create our own
        self._s3 = self._config.get("s3_client") or aws.boto3.client(
            "s3",
            **aws.get_s3_boto3_options(self.config.get("boto3_options", {})),
        )

    def _instantiate_gcs_client(self) -> None:
        """
        Helper method for instantiating GCS client when GCSBatchSpec is passed in.

        The method accounts for 3 ways that a GCS connection can be configured:
            1. setting an environment variable, which is typically GOOGLE_APPLICATION_CREDENTIALS
            2. passing in explicit credentials via gcs_options
            3. running Great Expectations from within a GCP container, at which you would be able to create a Client
                without passing in an additional environment variable or explicit credentials
        """  # noqa: E501 # FIXME CoP
        gcs_options = self.config.get("gcs_options", {})
        try:
            credentials = None  # If configured with gcloud CLI / env vars
            if "filename" in gcs_options:
                filename = gcs_options.pop("filename")
                credentials = google.service_account.Credentials.from_service_account_file(
                    filename=filename
                )
            elif "info" in gcs_options:
                info = gcs_options.pop("info")
                credentials = google.service_account.Credentials.from_service_account_info(
                    info=info
                )
            self._gcs = google.storage.Client(credentials=credentials, **gcs_options)
        # This exception handling causes a TypeError if google dependency not installed
        except (TypeError, AttributeError, google.DefaultCredentialsError):
            self._gcs = None

    @override
    def configure_validator(self, validator) -> None:
        super().configure_validator(validator)
        validator.expose_dataframe_methods = True

    @override
    def load_batch_data(
        self,
        batch_id: str,
        batch_data: Union[PandasBatchData, pd.DataFrame],  # type: ignore[override] # FIXME CoP
    ) -> None:
        if isinstance(batch_data, pd.DataFrame):
            batch_data = PandasBatchData(self, batch_data)
        elif not isinstance(batch_data, PandasBatchData):
            raise gx_exceptions.GreatExpectationsError(  # noqa: TRY003 # FIXME CoP
                "PandasExecutionEngine requires batch data that is either a DataFrame or a PandasBatchData object"  # noqa: E501 # FIXME CoP
            )

        super().load_batch_data(batch_id=batch_id, batch_data=batch_data)

    @override
    def get_batch_data_and_markers(  # noqa: C901, PLR0912, PLR0915 # FIXME CoP
        self, batch_spec: BatchSpec | PandasBatchSpecProtocol
    ) -> Tuple[PandasBatchData, BatchMarkers]:  # batch_data
        # We need to build a batch_markers to be used in the dataframe
        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        batch_data: Any
        if isinstance(batch_spec, RuntimeDataBatchSpec):
            # batch_data != None is already checked when RuntimeDataBatchSpec is instantiated
            batch_data = batch_spec.batch_data
            if isinstance(batch_data, str):
                raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                    f"""PandasExecutionEngine has been passed a string type batch_data, "{batch_data}", which is illegal.  Please check your config.
"""  # noqa: E501 # FIXME CoP
                )

            if isinstance(batch_spec.batch_data, pd.DataFrame):
                df = batch_spec.batch_data
            elif isinstance(batch_spec.batch_data, PandasBatchData):
                df = batch_spec.batch_data.dataframe
            else:
                raise ValueError(  # noqa: TRY003, TRY004 # FIXME CoP
                    "RuntimeDataBatchSpec must provide a Pandas DataFrame or PandasBatchData object."  # noqa: E501 # FIXME CoP
                )

            batch_spec.batch_data = "PandasDataFrame"

        elif isinstance(batch_spec, S3BatchSpec):
            if self._s3 is None:
                self._instantiate_s3_client()
            s3_engine = self._s3
            try:
                reader_method: str = batch_spec.reader_method
                reader_options: dict = batch_spec.reader_options or {}
                path: str = batch_spec.path
                s3_url = S3Url(path)
                if "compression" not in reader_options:
                    inferred_compression_param = sniff_s3_compression(s3_url)
                    if inferred_compression_param is not None:
                        reader_options["compression"] = inferred_compression_param
                if s3_engine:
                    s3_object: dict = s3_engine.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            except (
                aws.exceptions.ParamValidationError,
                aws.exceptions.ClientError,
            ) as error:
                raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                    f"""PandasExecutionEngine encountered the following error while trying to read data from S3 Bucket: {error}"""  # noqa: E501 # FIXME CoP
                )
            logger.debug(f"Fetching s3 object. Bucket: {s3_url.bucket} Key: {s3_url.key}")
            reader_fn: DataFrameFactoryFn = self._get_reader_fn(reader_method, s3_url.key)
            buf = BytesIO(s3_object["Body"].read())  # type: ignore[possibly-undefined] # FIXME
            buf.seek(0)
            df = reader_fn(buf, **reader_options)

        elif isinstance(batch_spec, AzureBatchSpec):
            if self._azure is None:
                self._instantiate_azure_client()
            # if we were not able to instantiate Azure client, then raise error
            if self._azure is None:
                raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                    """PandasExecutionEngine has been passed a AzureBatchSpec,
                        but the ExecutionEngine does not have an Azure client configured. Please check your config."""  # noqa: E501 # FIXME CoP
                )
            azure_engine = self._azure
            reader_method = batch_spec.reader_method
            reader_options = batch_spec.reader_options or {}
            path = batch_spec.path
            azure_url = AzureUrl(path)
            blob_client = azure_engine.get_blob_client(
                container=azure_url.container, blob=azure_url.blob
            )
            azure_object = blob_client.download_blob()
            logger.debug(
                f"Fetching Azure blob. Container: {azure_url.container} Blob: {azure_url.blob}"
            )
            reader_fn = self._get_reader_fn(reader_method, azure_url.blob)
            buf = BytesIO(azure_object.readall())
            buf.seek(0)
            df = reader_fn(buf, **reader_options)

        elif isinstance(batch_spec, GCSBatchSpec):
            if self._gcs is None:
                self._instantiate_gcs_client()
            # if we were not able to instantiate GCS client, then raise error
            if self._gcs is None:
                raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                    """PandasExecutionEngine has been passed a GCSBatchSpec,
                        but the ExecutionEngine does not have an GCS client configured. Please check your config."""  # noqa: E501 # FIXME CoP
                )
            gcs_engine = self._gcs
            gcs_url = GCSUrl(batch_spec.path)
            reader_method = batch_spec.reader_method
            reader_options = batch_spec.reader_options or {}
            try:
                gcs_bucket = gcs_engine.get_bucket(gcs_url.bucket)
                gcs_blob = gcs_bucket.blob(gcs_url.blob)
                logger.debug(f"Fetching GCS blob. Bucket: {gcs_url.bucket} Blob: {gcs_url.blob}")
            except google.GoogleAPIError as error:
                raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                    f"""PandasExecutionEngine encountered the following error while trying to read data from GCS \
Bucket: {error}"""  # noqa: E501 # FIXME CoP
                )
            reader_fn = self._get_reader_fn(reader_method, gcs_url.blob)
            buf = BytesIO(gcs_blob.download_as_bytes())
            buf.seek(0)
            df = reader_fn(buf, **reader_options)

        # Experimental datasources will go down this code path
        elif isinstance(batch_spec, PathBatchSpec):
            reader_method = batch_spec.reader_method
            reader_options = batch_spec.reader_options
            path = batch_spec.path
            reader_fn = self._get_reader_fn(reader_method, path)
            df = reader_fn(path, **reader_options)

        elif isinstance(batch_spec, PandasBatchSpec):
            reader_method = batch_spec.reader_method
            reader_options = batch_spec.reader_options
            reader_fn = self._get_reader_fn(reader_method)
            reader_fn_result: pd.DataFrame | list[pd.DataFrame] = execute_pandas_reader_fn(
                reader_fn, reader_options
            )
            if isinstance(reader_fn_result, list):
                if len(reader_fn_result) > 1:
                    raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                        "Pandas reader method must return a single DataFrame, "
                        f'but "{reader_method}" returned {len(reader_fn_result)} DataFrames.'
                    )
                else:
                    df = reader_fn_result[0]
            else:
                df = reader_fn_result

        elif isinstance(batch_spec, FabricBatchSpec):
            reader_fn = batch_spec.get_reader_function()
            df = reader_fn(**batch_spec.reader_options)

        else:
            raise gx_exceptions.BatchSpecError(  # noqa: TRY003 # FIXME CoP
                f"""batch_spec must be of type RuntimeDataBatchSpec, PandasBatchSpec, PathBatchSpec, S3BatchSpec, AzureBatchSpec or FabricBatchSpec \
not {batch_spec.__class__.__name__}"""  # noqa: E501 # FIXME CoP
            )

        df = self._apply_partitioning_and_sampling_methods(batch_spec, df)  # type: ignore[arg-type] # FIXME CoP
        if df.memory_usage().sum() < HASH_THRESHOLD:
            batch_markers["pandas_data_fingerprint"] = hash_pandas_dataframe(df)

        typed_batch_data = PandasBatchData(execution_engine=self, dataframe=df)

        return typed_batch_data, batch_markers

    def _apply_partitioning_and_sampling_methods(
        self,
        batch_spec: BatchSpec | PandasBatchSpecProtocol,
        batch_data: PandasBatchData,
    ):
        # partitioning and sampling not supported for FabricBatchSpec
        if isinstance(batch_spec, BatchSpec):
            partitioner_method_name: Optional[str] = batch_spec.get("partitioner_method")
            if partitioner_method_name:
                partitioner_fn: Callable = self._data_partitioner.get_partitioner_method(
                    partitioner_method_name
                )
                partitioner_kwargs: dict = batch_spec.get("partitioner_kwargs") or {}
                batch_data = partitioner_fn(batch_data, **partitioner_kwargs)

            sampler_method_name: Optional[str] = batch_spec.get("sampling_method")
            if sampler_method_name:
                sampling_fn: Callable = self._data_sampler.get_sampler_method(sampler_method_name)
                batch_data = sampling_fn(batch_data, batch_spec)

        return batch_data

    @property
    def dataframe(self) -> pd.DataFrame:
        """Tests whether or not a Batch has been loaded. If the loaded batch does not exist, raises a
        ValueError Exception
        """  # noqa: E501 # FIXME CoP
        # Changed to is None because was breaking prior
        if self.batch_manager.active_batch_data is None:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "Batch has not been loaded - please run load_batch_data() to load a batch."
            )

        return cast("PandasBatchData", self.batch_manager.active_batch_data).dataframe

    # NOTE Abe 20201105: Any reason this shouldn't be a private method?
    @staticmethod
    def guess_reader_method_from_path(path: str):  # noqa: C901, PLR0911 # FIXME CoP
        """Helper method for deciding which reader to use to read in a certain path.

        Args:
            path (str): the to use to guess

        Returns:
            ReaderMethod to use for the filepath

        """
        path = path.lower()
        if path.endswith(".csv") or path.endswith(".tsv"):
            return {"reader_method": "read_csv"}
        elif path.endswith(".parquet") or path.endswith(".parq") or path.endswith(".pqt"):
            return {"reader_method": "read_parquet"}
        elif path.endswith(".xlsx") or path.endswith(".xls"):
            return {"reader_method": "read_excel"}
        elif path.endswith(".json"):
            return {"reader_method": "read_json"}
        elif path.endswith(".pkl"):
            return {"reader_method": "read_pickle"}
        elif path.endswith(".feather"):
            return {"reader_method": "read_feather"}
        elif path.endswith(".csv.gz") or path.endswith(".tsv.gz"):
            return {
                "reader_method": "read_csv",
                "reader_options": {"compression": "gzip"},
            }
        elif path.endswith(".sas7bdat") or path.endswith(".xpt"):
            return {"reader_method": "read_sas"}

        else:
            raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                f'Unable to determine reader method from path: "{path}".'
            )

    @overload
    def _get_reader_fn(
        self, reader_method: str = ..., path: Optional[str] = ...
    ) -> DataFrameFactoryFn: ...

    @overload
    def _get_reader_fn(self, reader_method: None = ..., path: str = ...) -> DataFrameFactoryFn: ...

    def _get_reader_fn(
        self, reader_method: Optional[str] = None, path: Optional[str] = None
    ) -> DataFrameFactoryFn:
        """Static helper for parsing reader types. If reader_method is not provided, path will be used to guess the
        correct reader_method.

        Args:
            reader_method (str): the name of the reader method to use, if available.
            path (str): the path used to guess

        Returns:
            ReaderMethod to use for the filepath

        """  # noqa: E501 # FIXME CoP
        if reader_method is None and path is None:
            raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                "Unable to determine pandas reader function without reader_method or path."
            )

        reader_options = {}
        if reader_method is None:
            path_guess = self.guess_reader_method_from_path(path)  # type: ignore[arg-type] # see overload
            reader_method = path_guess["reader_method"]
            reader_options = path_guess.get(
                "reader_options"
            )  # This may not be there; use None in that case

        # Handle read_gbq which was removed from pandas 3.0.0+
        # Use pandas_gbq.read_gbq instead
        if reader_method == "read_gbq":
            pandas_version = Version(pd.__version__)
            if pandas_version >= Version("3.0.0"):
                try:
                    import pandas_gbq  # type: ignore[import-not-found] # Import  is only available when installing BigQuery Dependencies

                    reader_fn = pandas_gbq.read_gbq
                except ImportError:
                    raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                        "pandas.read_gbq was removed in pandas 3.0.0. "
                        "Please install pandas-gbq, `pip install pandas-gbq`"
                        "See https://pandas-gbq.readthedocs.io/ for more information."
                    )
            else:
                reader_fn = getattr(pd, reader_method)
        else:
            try:
                reader_fn = getattr(pd, reader_method)
            except AttributeError:
                raise gx_exceptions.ExecutionEngineError(  # noqa: TRY003 # FIXME CoP
                    f'Unable to find reader_method "{reader_method}" in pandas.'
                )

        if reader_options:
            reader_fn = partial(reader_fn, **reader_options)
        return reader_fn

    @override
    def resolve_metric_bundle(self, metric_fn_bundle) -> dict[MetricConfigurationID, Any]:
        """Resolve a bundle of metrics with the same compute Domain as part of a single trip to the compute engine."""  # noqa: E501 # FIXME CoP
        return {}  # This is NO-OP for "PandasExecutionEngine" (no bundling for direct execution computational backend).  # noqa: E501 # FIXME CoP

    def _apply_row_condition_filter(
        self, data: pd.DataFrame, row_condition: Any, domain_kwargs: dict
    ) -> pd.DataFrame:
        """Apply row condition filter to DataFrame.

        Args:
            data: The DataFrame to filter
            row_condition: The condition to apply (can be Condition object, dict, or string)
            domain_kwargs: Domain kwargs containing condition_parser if needed

        Returns:
            Filtered DataFrame
        """
        # Convert dict to Condition object if needed
        if isinstance(row_condition, dict):
            row_condition = deserialize_row_condition(row_condition)

        if isinstance(row_condition, PassThroughCondition):
            # Use pass_through_filter for pandas query syntax
            # Uses DataFrame.query() directly with the pass-through string
            return data.query(row_condition.pass_through_filter)
        elif isinstance(row_condition, Condition):
            # Handle other Condition objects using condition_to_filter_clause
            return data.query(self.condition_to_filter_clause(row_condition))
        else:
            # Legacy string-based conditions
            condition_parser = domain_kwargs.get("condition_parser", None)
            if (
                condition_parser
                and condition_parser == CONDITION_PARSER_PANDAS
                and isinstance(row_condition, str)
            ):
                return data.query(row_condition, parser=condition_parser)
            else:
                raise gx_exceptions.ValidationError(  # noqa: TRY003 # FIXME CoP
                    "condition_parser for Pandas is required when setting a row_condition."
                )

    @override
    def get_domain_records(  # noqa: C901, PLR0912 # FIXME CoP
        self,
        domain_kwargs: dict,
    ) -> pd.DataFrame:
        """Uses the given Domain kwargs (which include row_condition, condition_parser, and ignore_row_if directives) to obtain and/or query a Batch of data.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the Domain kwargs specifying which data to obtain

        Returns:
            A DataFrame (the data on which to compute returned in the format of a Pandas DataFrame)
        """  # noqa: E501 # FIXME CoP
        table = domain_kwargs.get("table", None)
        if table:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "PandasExecutionEngine does not currently support multiple named tables."
            )

        batch_id = domain_kwargs.get("batch_id")
        if batch_id is None:
            # We allow no batch id specified if there is only one batch
            if self.batch_manager.active_batch_data_id is not None:
                data = cast("PandasBatchData", self.batch_manager.active_batch_data).dataframe
            else:
                raise gx_exceptions.ValidationError(  # noqa: TRY003 # FIXME CoP
                    "No batch is specified, but could not identify a loaded batch."
                )
        else:  # noqa: PLR5501 # FIXME CoP
            if batch_id in self.batch_manager.batch_data_cache:
                data = cast(
                    "PandasBatchData", self.batch_manager.batch_data_cache[batch_id]
                ).dataframe
            else:
                raise gx_exceptions.ValidationError(  # noqa: TRY003 # FIXME CoP
                    f"Unable to find batch with batch_id {batch_id}"
                )

        # Filtering by row condition.
        row_condition = domain_kwargs.get("row_condition", None)
        if row_condition:
            data = self._apply_row_condition_filter(data, row_condition, domain_kwargs)

        if "column" in domain_kwargs:
            return data

        if (
            "column_A" in domain_kwargs
            and "column_B" in domain_kwargs
            and "ignore_row_if" in domain_kwargs
        ):
            # noinspection PyPep8Naming
            column_A_name = domain_kwargs["column_A"]
            # noinspection PyPep8Naming
            column_B_name = domain_kwargs["column_B"]

            ignore_row_if = domain_kwargs["ignore_row_if"]
            if ignore_row_if == "both_values_are_missing":
                data = data.dropna(
                    axis=0,
                    how="all",
                    subset=[column_A_name, column_B_name],
                )
            elif ignore_row_if == "either_value_is_missing":
                data = data.dropna(
                    axis=0,
                    how="any",
                    subset=[column_A_name, column_B_name],
                )
            else:  # noqa: PLR5501 # FIXME CoP
                if ignore_row_if != "neither":
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')  # noqa: TRY003 # FIXME CoP

            return data

        if "column_list" in domain_kwargs and "ignore_row_if" in domain_kwargs:
            column_list = domain_kwargs["column_list"]

            ignore_row_if = domain_kwargs["ignore_row_if"]
            if ignore_row_if == "all_values_are_missing":
                data = data.dropna(
                    axis=0,
                    how="all",
                    subset=column_list,
                )
            elif ignore_row_if == "any_value_is_missing":
                data = data.dropna(
                    axis=0,
                    how="any",
                    subset=column_list,
                )
            else:  # noqa: PLR5501 # FIXME CoP
                if ignore_row_if != "never":
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')  # noqa: TRY003 # FIXME CoP

            return data

        return data

    @override
    def get_compute_domain(
        self,
        domain_kwargs: dict,
        domain_type: Union[str, MetricDomainTypes],
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> Tuple[pd.DataFrame, dict, dict]:
        """Uses the given Domain kwargs (which include row_condition, condition_parser, and ignore_row_if directives) to obtain and/or query a batch.

        Returns in the format of a Pandas DataFrame along with Domain arguments required for computing.  If the Domain \
        is a single column, this is added to 'accessor Domain kwargs' and used for later access.

        Args:
            domain_kwargs (dict): a dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type (str or MetricDomainTypes): an Enum value indicating which metric Domain the user would like \
            to be using, or a corresponding string value representing it.  String types include "column", \
            "column_pair", "table", and "other".  Enum types include capitalized versions of these from the class \
            MetricDomainTypes.
            accessor_keys (str iterable): keys that are part of the compute Domain but should be ignored when \
            describing the Domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            A tuple including:
              - a DataFrame (the data on which to compute)
              - a dictionary of compute_domain_kwargs, describing the DataFrame
              - a dictionary of accessor_domain_kwargs, describing any accessors needed to
                identify the Domain within the compute domain
        """  # noqa: E501 # FIXME CoP
        table: Optional[str] = domain_kwargs.get("table", None)
        if table:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "PandasExecutionEngine does not currently support multiple named tables."
            )

        data: pd.DataFrame = self.get_domain_records(domain_kwargs=domain_kwargs)

        partition_domain_kwargs: PartitionDomainKwargs = self._partition_domain_kwargs(
            domain_kwargs, domain_type, accessor_keys
        )

        return data, partition_domain_kwargs.compute, partition_domain_kwargs.accessor

    @override
    def _comparison_condition_to_filter_clause(self, condition: ComparisonCondition) -> str:
        col = query_column_reference(condition.column.name)
        op, val = condition.operator, condition.parameter
        if op in (Operator.IN, Operator.NOT_IN):
            values = ", ".join(map(repr, val))
            connector = "in" if op == Operator.IN else "not in"
            return f"{col} {connector} [{values}]"
        return f"{col} {op} {val!r}"

    @override
    def _nullity_condition_to_filter_clause(self, condition: NullityCondition) -> str:
        col = query_column_reference(condition.column.name)
        return f"{col}.isnull()" if condition.is_null else f"~{col}.isnull()"

    @override
    def _and_condition_to_filter_clause(self, condition: AndCondition) -> str:
        parts = [self.condition_to_filter_clause(c) for c in condition.conditions]
        return "(" + " and ".join(parts) + ")"

    @override
    def _or_condition_to_filter_clause(self, condition: OrCondition) -> str:
        parts = [self.condition_to_filter_clause(c) for c in condition.conditions]
        return "(" + " or ".join(parts) + ")"


def hash_pandas_dataframe(df):
    try:
        obj = pd.util.hash_pandas_object(df, index=True).values
    except TypeError:
        # In case of facing unhashable objects (like dict), use pickle
        obj = pickle.dumps(df, pickle.HIGHEST_PROTOCOL)

    # not used for security: this is a batch fingerprint, not a cryptographic digest.
    # usedforsecurity=False lets this run on hosts with FIPS-mode OpenSSL.
    return hashlib.md5(obj, usedforsecurity=False).hexdigest()
