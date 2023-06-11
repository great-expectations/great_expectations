from __future__ import annotations

import copy
import datetime
import logging
from functools import reduce
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
    overload,
)

from dateutil.parser import parse

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import (
    functions as F,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import BatchMarkers
from great_expectations.core.batch_spec import (
    AzureBatchSpec,
    BatchSpec,
    GlueDataCatalogBatchSpec,
    PathBatchSpec,
    RuntimeDataBatchSpec,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.core.metric_domain_types import (
    MetricDomainTypes,  # noqa: TCH001
)
from great_expectations.core.util import (
    AzureUrl,
    convert_to_json_serializable,
    get_or_create_spark_application,
)
from great_expectations.exceptions import (
    BatchSpecError,
    ExecutionEngineError,
    GreatExpectationsError,
    ValidationError,
)
from great_expectations.exceptions import exceptions as gx_exceptions
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.execution_engine import (
    MetricComputationConfiguration,  # noqa: TCH001
    SplitDomainKwargs,  # noqa: TCH001
)
from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
from great_expectations.execution_engine.split_and_sample.sparkdf_data_sampler import (
    SparkDataSampler,
)
from great_expectations.execution_engine.split_and_sample.sparkdf_data_splitter import (
    SparkDataSplitter,
)
from great_expectations.expectations.row_conditions import (
    RowCondition,
    RowConditionParserType,
    parse_condition_to_spark,
)
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,  # noqa: TCH001
)

logger = logging.getLogger(__name__)


# noinspection SpellCheckingInspection
def apply_dateutil_parse(column):
    assert len(column.columns) == 1, "Expected DataFrame with 1 column"
    col_name = column.columns[0]
    _udf = F.udf(parse, pyspark.types.TimestampType())
    return column.withColumn(col_name, _udf(col_name))


@public_api
class SparkDFExecutionEngine(ExecutionEngine):
    """SparkDFExecutionEngine instantiates the ExecutionEngine API to support computations using Spark platform.

    This class holds an attribute `spark_df` which is a spark.sql.DataFrame.

    Constructor builds a SparkDFExecutionEngine, using provided configuration parameters.

    Args:
        *args: Positional arguments for configuring SparkDFExecutionEngine
        persist: If True (default), then creation of the Spark DataFrame is done outside this class
        spark_config: Dictionary of Spark configuration options
        force_reuse_spark_context: If True then utilize existing SparkSession if it exists and is active
        **kwargs: Keyword arguments for configuring SparkDFExecutionEngine

    For example:
    ```python
        name: str = "great_expectations-ee-config"
        spark_config: Dict[str, str] = {
        "spark.app.name": name,
        "spark.sql.catalogImplementation": "hive",
        "spark.executor.memory": "512m",
        }
        execution_engine = SparkDFExecutionEngine(spark_config=spark_config)
        spark_session: SparkSession = execution_engine.spark
    ```

    --ge-feature-maturity-info--

        id: validation_engine_pyspark_self_managed
        title: Validation Engine - pyspark - Self-Managed
        icon:
        short_description: Use Spark DataFrame to validate data
        description: Use Spark DataFrame to validate data
        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html
        maturity: Production
        maturity_details:
            api_stability: Stable
            implementation_completeness: Moderate
            unit_test_coverage: Complete
            integration_infrastructure_test_coverage: N/A -> see relevant Datasource evaluation
            documentation_completeness: Complete
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

        id: validation_engine_databricks
        title: Validation Engine - Databricks
        icon:
        short_description: Use Spark DataFrame in a Databricks cluster to validate data
        description: Use Spark DataFrame in a Databricks cluster to validate data
        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html
        maturity: Beta
        maturity_details:
            api_stability: Stable
            implementation_completeness: Low (dbfs-specific handling)
            unit_test_coverage: N/A -> implementation not different
            integration_infrastructure_test_coverage: Minimal (we've tested a bit, know others have used it)
            documentation_completeness: Moderate (need docs on managing project configuration via dbfs/etc.)
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

        id: validation_engine_emr_spark
        title: Validation Engine - EMR - Spark
        icon:
        short_description: Use Spark DataFrame in an EMR cluster to validate data
        description: Use Spark DataFrame in an EMR cluster to validate data
        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html
        maturity: Experimental
        maturity_details:
            api_stability: Stable
            implementation_completeness: Low (need to provide guidance on "known good" paths, and we know there are many "knobs" to tune that we have not explored/tested)
            unit_test_coverage: N/A -> implementation not different
            integration_infrastructure_test_coverage: Unknown
            documentation_completeness: Low (must install specific/latest version but do not have docs to that effect or of known useful paths)
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

        id: validation_engine_spark_other
        title: Validation Engine - Spark - Other
        icon:
        short_description: Use Spark DataFrame to validate data
        description: Use Spark DataFrame to validate data
        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html
        maturity: Experimental
        maturity_details:
            api_stability: Stable
            implementation_completeness: Other (we haven't tested possibility, known glue deployment)
            unit_test_coverage: N/A -> implementation not different
            integration_infrastructure_test_coverage: Unknown
            documentation_completeness: Low (must install specific/latest version but do not have docs to that effect or of known useful paths)
            bug_risk: Low/Moderate
            expectation_completeness: Moderate

    --ge-feature-maturity-info--
    """

    recognized_batch_definition_keys = {"limit"}

    recognized_batch_spec_defaults = {
        "reader_method",
        "reader_options",
    }

    def __init__(
        self,
        *args,
        persist=True,
        spark_config=None,
        force_reuse_spark_context=True,
        **kwargs,
    ) -> None:
        self._persist = persist

        if spark_config is None:
            spark_config = {}

        spark: pyspark.SparkSession = get_or_create_spark_application(
            spark_config=spark_config,
            force_reuse_spark_context=force_reuse_spark_context,
        )

        spark_config.update({k: v for (k, v) in spark.sparkContext.getConf().getAll()})

        self.spark = spark

        azure_options: dict = kwargs.pop("azure_options", {})
        self._azure_options = azure_options

        super().__init__(*args, **kwargs)

        self._config.update(
            {
                "persist": self._persist,
                "spark_config": spark_config,
                "azure_options": azure_options,
            }
        )

        self._data_splitter = SparkDataSplitter()
        self._data_sampler = SparkDataSampler()

    @property
    def dataframe(self) -> pyspark.DataFrame:
        """If a batch has been loaded, returns a Spark Dataframe containing the data within the loaded batch"""
        if self.batch_manager.active_batch_data is None:
            raise ValueError(
                "Batch has not been loaded - please run load_batch() to load a batch."
            )

        return cast(SparkDFBatchData, self.batch_manager.active_batch_data).dataframe

    def load_batch_data(  # type: ignore[override]
        self, batch_id: str, batch_data: Union[SparkDFBatchData, pyspark.DataFrame]
    ) -> None:
        if pyspark.DataFrame and isinstance(batch_data, pyspark.DataFrame):  # type: ignore[truthy-function]
            batch_data = SparkDFBatchData(self, batch_data)
        elif not isinstance(batch_data, SparkDFBatchData):
            raise GreatExpectationsError(
                "SparkDFExecutionEngine requires batch data that is either a DataFrame or a SparkDFBatchData object"
            )

        if self._persist:
            batch_data.dataframe.persist()

        super().load_batch_data(batch_id=batch_id, batch_data=batch_data)  # type: ignore[arg-type] # got SparkDFBatchData

    def get_batch_data_and_markers(  # noqa: PLR0912, PLR0915
        self, batch_spec: BatchSpec
    ) -> Tuple[Any, BatchMarkers]:  # batch_data
        # We need to build a batch_markers to be used in the dataframe
        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        """
        As documented in Azure DataConnector implementations, Pandas and Spark execution engines utilize separate path
        formats for accessing Azure Blob Storage service.  However, Pandas and Spark execution engines utilize identical
        path formats for accessing all other supported cloud storage services (AWS S3 and Google Cloud Storage).
        Moreover, these formats (encapsulated in S3BatchSpec and GCSBatchSpec) extend PathBatchSpec (common to them).
        Therefore, at the present time, all cases with the exception of Azure Blob Storage, are handled generically.
        """

        batch_data: Any
        reader_method: str
        reader_options: dict
        path: str
        schema: Optional[Union[pyspark.types.StructType, dict, str]]
        reader: pyspark.DataFrameReader
        reader_fn: Callable
        if isinstance(batch_spec, RuntimeDataBatchSpec):
            # batch_data != None is already checked when RuntimeDataBatchSpec is instantiated
            batch_data = batch_spec.batch_data
            if isinstance(batch_data, str):
                raise gx_exceptions.ExecutionEngineError(
                    f"""SparkDFExecutionEngine has been passed a string type batch_data, "{batch_data}", which is \
illegal.  Please check your config."""
                )
            batch_spec.batch_data = "SparkDataFrame"

        elif isinstance(batch_spec, AzureBatchSpec):
            reader_method = batch_spec.reader_method
            reader_options = batch_spec.reader_options or {}
            path = batch_spec.path
            azure_url = AzureUrl(path)
            # TODO <WILL> 202209 - Add `schema` definition to Azure like PathBatchSpec below (GREAT-1224)
            try:
                credential = self._azure_options.get("credential")
                storage_account_url = azure_url.account_url
                if credential:
                    self.spark.conf.set(
                        "fs.wasb.impl",
                        "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
                    )
                    self.spark.conf.set(
                        f"fs.azure.account.key.{storage_account_url}", credential
                    )
                reader = self.spark.read.options(**reader_options)
                reader_fn = self._get_reader_fn(
                    reader=reader,
                    reader_method=reader_method,
                    path=path,
                )
                batch_data = reader_fn(path)
            except AttributeError:
                raise ExecutionEngineError(
                    """
                    Unable to load pyspark. Pyspark is required for SparkDFExecutionEngine.
                    """
                )

        elif isinstance(batch_spec, (PathBatchSpec, GlueDataCatalogBatchSpec)):
            reader_method = batch_spec.reader_method
            reader_options = batch_spec.reader_options or {}
            path = batch_spec.path
            schema = reader_options.get("schema")

            # schema can be a dict if it has been through serialization step,
            # either as part of the datasource configuration, or checkpoint config
            if isinstance(schema, dict):
                schema = pyspark.types.StructType.fromJson(schema)

            # this can happen if we have not converted schema into json at Datasource-config level
            elif isinstance(schema, str):
                raise gx_exceptions.ExecutionEngineError(
                    """
                    Spark schema was not properly serialized.
                    Please run the .jsonValue() method on the schema object before loading into GX.
                    schema: your_schema.jsonValue()
                    """
                )
            # noinspection PyUnresolvedReferences
            try:
                if schema:
                    reader = self.spark.read.schema(schema).options(**reader_options)
                else:
                    reader = self.spark.read.options(**reader_options)

                reader_fn = self._get_reader_fn(
                    reader=reader,
                    reader_method=reader_method,
                    path=path,
                )
                batch_data = reader_fn(path)
            except AttributeError:
                raise ExecutionEngineError(
                    """
                    Unable to load pyspark. Pyspark is required for SparkDFExecutionEngine.
                    """
                )
            # pyspark will raise an AnalysisException error if path is incorrect
            except pyspark.AnalysisException:
                raise ExecutionEngineError(
                    f"""Unable to read in batch from the following path: {path}. Please check your configuration."""
                )

        else:
            raise BatchSpecError(
                """
                Invalid batch_spec: batch_data is required for a SparkDFExecutionEngine to operate.
                """
            )

        batch_data = self._apply_splitting_and_sampling_methods(batch_spec, batch_data)
        typed_batch_data = SparkDFBatchData(execution_engine=self, dataframe=batch_data)

        return typed_batch_data, batch_markers

    def _apply_splitting_and_sampling_methods(self, batch_spec, batch_data):
        # Note this is to get a batch from tables in AWS Glue Data Catalog by its partitions
        partitions: Optional[List[str]] = batch_spec.get("partitions")
        if partitions:
            batch_data = self._data_splitter.split_on_multi_column_values(
                df=batch_data,
                column_names=partitions,
                batch_identifiers=batch_spec.get("batch_identifiers"),
            )

        splitter_method_name: Optional[str] = batch_spec.get("splitter_method")
        if splitter_method_name:
            splitter_fn: Callable = self._data_splitter.get_splitter_method(
                splitter_method_name
            )
            splitter_kwargs: dict = batch_spec.get("splitter_kwargs") or {}
            batch_data = splitter_fn(batch_data, **splitter_kwargs)

        sampler_method_name: Optional[str] = batch_spec.get("sampling_method")
        if sampler_method_name:
            sampling_fn: Callable = self._data_sampler.get_sampler_method(
                sampler_method_name
            )
            batch_data = sampling_fn(batch_data, batch_spec)

        return batch_data

    # TODO: <Alex>Similar to Abe's note in PandasExecutionEngine: Any reason this shouldn't be a private method?</Alex>
    @staticmethod
    def guess_reader_method_from_path(path: str):
        """
        Based on a given filepath, decides a reader method. Currently supports tsv, csv, and parquet. If none of these
        file extensions are used, returns ExecutionEngineError stating that it is unable to determine the current path.

        Args:
            path - A given file path

        Returns:
            A dictionary entry of format {'reader_method': reader_method}

        """
        path = path.lower()
        if path.endswith(".csv") or path.endswith(".tsv"):
            return "csv"
        elif (
            path.endswith(".parquet") or path.endswith(".parq") or path.endswith(".pqt")
        ):
            return "parquet"

        raise ExecutionEngineError(
            f"Unable to determine reader method from path: {path}"
        )

    @overload
    def _get_reader_fn(
        self, reader, reader_method: str = ..., path: Optional[str] = ...
    ) -> Callable:
        ...

    @overload
    def _get_reader_fn(
        self, reader, reader_method: None = ..., path: str = ...
    ) -> Callable:
        ...

    def _get_reader_fn(self, reader, reader_method=None, path=None) -> Callable:
        """Static helper for providing reader_fn

        Args:
            reader: the base spark reader to use; this should have had reader_options applied already
            reader_method: the name of the reader_method to use, if specified
            path (str): the path to use to guess reader_method if it was not specified

        Returns:
            ReaderMethod to use for the filepath

        """
        if reader_method is None and path is None:
            raise ExecutionEngineError(
                "Unable to determine spark reader function without reader_method or path"
            )

        if reader_method is None:
            reader_method = self.guess_reader_method_from_path(path=path)

        reader_method_op: str = reader_method.lower()
        try:
            if reader_method_op == "delta":
                return reader.format(reader_method_op).load
            return getattr(reader, reader_method_op)
        except AttributeError:
            raise ExecutionEngineError(
                f"Unable to find reader_method {reader_method} in spark.",
            )

    @public_api
    def get_domain_records(  # noqa: C901, PLR0912, PLR0915
        self,
        domain_kwargs: dict,
    ) -> "pyspark.DataFrame":  # noqa F821
        """Uses the given Domain kwargs (which include row_condition, condition_parser, and ignore_row_if directives) to obtain and/or query a batch.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the Domain kwargs specifying which data to obtain

        Returns:
            A DataFrame (the data on which to compute returned in the format of a Spark DataFrame)
        """
        """
        # TODO: <Alex>Docusaurus run fails, unless "pyspark.DataFrame" type hint above is enclosed in quotes.
        This may be caused by it becoming great_expectations.compatibility.not_imported.NotImported when pyspark is not installed.
        </Alex>
        """
        table = domain_kwargs.get("table", None)
        if table:
            raise ValueError(
                "SparkDFExecutionEngine does not currently support multiple named tables."
            )

        batch_id = domain_kwargs.get("batch_id")
        if batch_id is None:
            # We allow no batch id specified if there is only one batch
            if self.batch_manager.active_batch_data:
                data = cast(
                    SparkDFBatchData, self.batch_manager.active_batch_data
                ).dataframe
            else:
                raise ValidationError(
                    "No batch is specified, but could not identify a loaded batch."
                )
        else:
            if batch_id in self.batch_manager.batch_data_cache:  # noqa: PLR5501
                data = cast(
                    SparkDFBatchData, self.batch_manager.batch_data_cache[batch_id]
                ).dataframe
            else:
                raise ValidationError(f"Unable to find batch with batch_id {batch_id}")

        # Filtering by row condition.
        row_condition = domain_kwargs.get("row_condition", None)
        if row_condition:
            condition_parser = domain_kwargs.get("condition_parser", None)
            if condition_parser == "spark":
                data = data.filter(row_condition)
            elif condition_parser == "great_expectations__experimental__":
                parsed_condition = parse_condition_to_spark(row_condition)
                data = data.filter(parsed_condition)
            else:
                raise GreatExpectationsError(
                    f"unrecognized condition_parser {str(condition_parser)} for Spark execution engine"
                )

        # Filtering by filter_conditions
        filter_conditions: List[RowCondition] = domain_kwargs.get(
            "filter_conditions", []
        )
        if len(filter_conditions) > 0:
            filter_condition = self._combine_row_conditions(filter_conditions)
            data = data.filter(filter_condition.condition)

        if "column" in domain_kwargs:
            return data

        # Filtering by ignore_row_if directive
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
                ignore_condition = (
                    F.col(column_A_name).isNull() & F.col(column_B_name).isNull()
                )
                data = data.filter(~ignore_condition)
            elif ignore_row_if == "either_value_is_missing":
                ignore_condition = (
                    F.col(column_A_name).isNull() | F.col(column_B_name).isNull()
                )
                data = data.filter(~ignore_condition)
            else:
                if ignore_row_if != "neither":  # noqa: PLR5501
                    raise ValueError(
                        f'Unrecognized value of ignore_row_if ("{ignore_row_if}").'
                    )

            return data

        if "column_list" in domain_kwargs and "ignore_row_if" in domain_kwargs:
            column_list = domain_kwargs["column_list"]
            ignore_row_if = domain_kwargs["ignore_row_if"]
            if ignore_row_if == "all_values_are_missing":
                conditions = [
                    F.col(column_name).isNull() for column_name in column_list
                ]
                ignore_condition = reduce(lambda a, b: a & b, conditions)
                data = data.filter(~ignore_condition)
            elif ignore_row_if == "any_value_is_missing":
                conditions = [
                    F.col(column_name).isNull() for column_name in column_list
                ]
                ignore_condition = reduce(lambda a, b: a | b, conditions)
                data = data.filter(~ignore_condition)
            else:
                if ignore_row_if != "never":  # noqa: PLR5501
                    raise ValueError(
                        f'Unrecognized value of ignore_row_if ("{ignore_row_if}").'
                    )

            return data

        return data

    @staticmethod
    def _combine_row_conditions(row_conditions: List[RowCondition]) -> RowCondition:
        """Combine row conditions using AND if condition_type is SPARK_SQL

        Note, although this method does not currently use `self` internally we
        are not marking as @staticmethod since it is meant to only be called
        internally in this class.

        Args:
            row_conditions: Row conditions of type Spark

        Returns:
            Single Row Condition combined
        """
        assert all(
            condition.condition_type == RowConditionParserType.SPARK_SQL
            for condition in row_conditions
        ), "All row conditions must have type SPARK_SQL"
        conditions: List[str] = [
            row_condition.condition for row_condition in row_conditions
        ]
        joined_condition: str = " AND ".join(conditions)
        return RowCondition(
            condition=joined_condition, condition_type=RowConditionParserType.SPARK_SQL
        )

    @public_api
    def get_compute_domain(
        self,
        domain_kwargs: dict,
        domain_type: Union[str, MetricDomainTypes],
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> Tuple["pyspark.DataFrame", dict, dict]:  # noqa F821
        """Uses a DataFrame and Domain kwargs (which include a row condition and a condition parser) to obtain and/or query a Batch of data.

        Returns in the format of a Spark DataFrame along with Domain arguments required for computing.  If the Domain \
        is a single column, this is added to 'accessor Domain kwargs' and used for later access.

        Args:
            domain_kwargs (dict): a dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type (str or MetricDomainTypes): an Enum value indicating which metric Domain the user would like \
            to be using, or a corresponding string value representing it.  String types include "identity", "column", \
            "column_pair", "table" and "other".  Enum types include capitalized versions of these from the class \
            MetricDomainTypes.
            accessor_keys (str iterable): keys that are part of the compute Domain but should be ignored when \
            describing the Domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            A tuple including:
              - a DataFrame (the data on which to compute)
              - a dictionary of compute_domain_kwargs, describing the DataFrame
              - a dictionary of accessor_domain_kwargs, describing any accessors needed to
                identify the Domain within the compute domain
        """
        """
        # TODO: <Alex>Docusaurus run fails, unless "pyspark.DataFrame" type hint above is enclosed in quotes.
        This may be caused by it becoming great_expectations.compatibility.not_imported.NotImported when pyspark is not installed.
        </Alex>
        """
        table: str = domain_kwargs.get("table", None)
        if table:
            raise ValueError(
                "SparkDFExecutionEngine does not currently support multiple named tables."
            )

        data: pyspark.DataFrame = self.get_domain_records(domain_kwargs=domain_kwargs)

        split_domain_kwargs: SplitDomainKwargs = self._split_domain_kwargs(
            domain_kwargs, domain_type, accessor_keys
        )

        return data, split_domain_kwargs.compute, split_domain_kwargs.accessor

    def add_column_row_condition(
        self, domain_kwargs, column_name=None, filter_null=True, filter_nan=False
    ):
        # We explicitly handle filter_nan & filter_null for spark using a spark-native condition

        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        assert "column" in domain_kwargs or column_name is not None
        if column_name is not None:
            column = column_name
        else:
            column = domain_kwargs["column"]

        filter_conditions: List[RowCondition] = []
        if filter_null:
            filter_conditions.append(
                RowCondition(
                    condition=f"{column} IS NOT NULL",
                    condition_type=RowConditionParserType.SPARK_SQL,
                )
            )
        if filter_nan:
            filter_conditions.append(
                RowCondition(
                    condition=f"NOT isnan({column})",
                    condition_type=RowConditionParserType.SPARK_SQL,
                )
            )

        if not (filter_null or filter_nan):
            logger.warning(
                "add_column_row_condition called without specifying a desired row condition"
            )

        new_domain_kwargs.setdefault("filter_conditions", []).extend(filter_conditions)

        return new_domain_kwargs

    def resolve_metric_bundle(
        self,
        metric_fn_bundle: Iterable[MetricComputationConfiguration],
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """For every metric in a set of Metrics to resolve, obtains necessary metric keyword arguments and builds
        bundles of the metrics into one large query dictionary so that they are all executed simultaneously. Will fail
        if bundling the metrics together is not possible.

            Args:
                metric_fn_bundle (Iterable[MetricComputationConfiguration]): \
                    "MetricComputationConfiguration" contains MetricProvider's MetricConfiguration (its unique identifier),
                    its metric provider function (the function that actually executes the metric), and arguments to pass
                    to metric provider function (dictionary of metrics defined in registry and corresponding arguments).

            Returns:
                A dictionary of "MetricConfiguration" IDs and their corresponding fully resolved values for domains.
        """
        resolved_metrics: Dict[Tuple[str, str, str], MetricValue] = {}

        res: List[pyspark.Row]

        aggregates: Dict[Tuple[str, str, str], dict] = {}

        aggregate: dict

        domain_id: Tuple[str, str, str]

        bundled_metric_configuration: MetricComputationConfiguration
        for bundled_metric_configuration in metric_fn_bundle:
            metric_to_resolve: MetricConfiguration = (
                bundled_metric_configuration.metric_configuration
            )
            metric_fn: Any = bundled_metric_configuration.metric_fn
            compute_domain_kwargs: dict = (
                bundled_metric_configuration.compute_domain_kwargs or {}
            )
            if not isinstance(compute_domain_kwargs, IDDict):
                compute_domain_kwargs = IDDict(compute_domain_kwargs)

            domain_id = compute_domain_kwargs.to_id()
            if domain_id not in aggregates:
                aggregates[domain_id] = {
                    "column_aggregates": [],
                    "metric_ids": [],
                    "domain_kwargs": compute_domain_kwargs,
                }

            aggregates[domain_id]["column_aggregates"].append(metric_fn)
            aggregates[domain_id]["metric_ids"].append(metric_to_resolve.id)

        for aggregate in aggregates.values():
            domain_kwargs: dict = aggregate["domain_kwargs"]
            df: pyspark.DataFrame = self.get_domain_records(domain_kwargs=domain_kwargs)

            assert len(aggregate["column_aggregates"]) == len(aggregate["metric_ids"])

            res = df.agg(*aggregate["column_aggregates"]).collect()

            logger.debug(
                f"SparkDFExecutionEngine computed {len(res[0])} metrics on domain_id {IDDict(domain_kwargs).to_id()}"
            )

            assert (
                len(res) == 1
            ), "all bundle-computed metrics must be single-value statistics"
            assert len(aggregate["metric_ids"]) == len(
                res[0]
            ), "unexpected number of metrics returned"

            idx: int
            metric_id: Tuple[str, str, str]
            for idx, metric_id in enumerate(aggregate["metric_ids"]):
                # Converting DataFrame.collect() results into JSON-serializable format produces simple data types,
                # amenable for subsequent post-processing by higher-level "Metric" and "Expectation" layers.
                resolved_metrics[metric_id] = convert_to_json_serializable(
                    data=res[0][idx]
                )

        return resolved_metrics

    def head(self, n=5):
        """Returns dataframe head. Default is 5"""
        return self.dataframe.limit(n).toPandas()
