import copy
import datetime
import hashlib
import logging
import uuid
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

from great_expectations.core.batch import BatchMarkers, BatchSpec
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.types.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.exceptions import exceptions as ge_exceptions

from ..exceptions import (
    BatchKwargsError,
    BatchSpecError,
    ExecutionEngineError,
    GreatExpectationsError,
    ValidationError,
)
from ..expectations.row_conditions import parse_condition_to_spark
from ..validator.validation_graph import MetricConfiguration
from .execution_engine import ExecutionEngine, MetricDomainTypes

logger = logging.getLogger(__name__)

try:
    import pyspark
    import pyspark.sql.functions as F
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        FloatType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    class SparkDFBatchData(DataFrame):
        def __init__(self, df):
            super(self.__class__, self).__init__(df._jdf, df.sql_ctx)

        def row_count(self):
            return self.count()


except ImportError:
    pyspark = None
    SparkSession = None
    DataFrame = None
    F = None
    StructType = (None,)
    StructField = (None,)
    IntegerType = (None,)
    FloatType = (None,)
    StringType = (None,)
    DateType = (None,)
    BooleanType = (None,)

    SparkDFBatchData = None

    logger.debug(
        "Unable to load pyspark; install optional spark dependency for support."
    )


class SparkDFExecutionEngine(ExecutionEngine):
    """
    This class holds an attribute `spark_df` which is a spark.sql.DataFrame.

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

    def __init__(self, *args, **kwargs):
        # Creation of the Spark DataFrame is done outside this class
        self._persist = kwargs.pop("persist", True)
        self._spark_config = kwargs.pop("spark_config", {})
        try:
            builder = SparkSession.builder
            app_name: Optional[str] = self._spark_config.pop("spark.app.name", None)
            if app_name:
                builder.appName(app_name)
            for k, v in self._spark_config.items():
                builder.config(k, v)
            self.spark = builder.getOrCreate()
        except AttributeError:
            logger.error(
                "Unable to load spark context; install optional spark dependency for support."
            )
            self.spark = None

        super().__init__(*args, **kwargs)

        self._config.update(
            {
                "persist": self._persist,
                "spark_config": self._spark_config,
            }
        )

    @property
    def dataframe(self):
        """If a batch has been loaded, returns a Spark Dataframe containing the data within the loaded batch"""
        if not self.active_batch_data:
            raise ValueError(
                "Batch has not been loaded - please run load_batch() to load a batch."
            )

        return self.active_batch_data

    def get_batch_data_and_markers(
        self, batch_spec: BatchSpec
    ) -> Tuple[Any, BatchMarkers]:  # batch_data
        batch_data: DataFrame

        # We need to build a batch_markers to be used in the dataframe
        batch_markers: BatchMarkers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        if isinstance(batch_spec, RuntimeDataBatchSpec):
            # batch_data != None is already checked when RuntimeDataBatchSpec is instantiated
            batch_data = batch_spec.batch_data
            batch_spec.batch_data = "SparkDataFrame"
        elif isinstance(batch_spec, (PathBatchSpec, S3BatchSpec)):
            reader_method: str = batch_spec.get("reader_method")
            reader_options: dict = batch_spec.get("reader_options") or {}
            path: str = batch_spec.get("path") or batch_spec.get("s3")
            try:
                reader_options = self.spark.read.options(**reader_options)
                reader_fn: Callable = self._get_reader_fn(
                    reader=reader_options,
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
        else:
            raise BatchSpecError(
                """
                Invalid batch_spec: batch_data is required for a SparkDFExecutionEngine to operate.
                """
            )

        batch_data = self._apply_splitting_and_sampling_methods(batch_spec, batch_data)
        typed_batch_data = SparkDFBatchData(batch_data)

        return typed_batch_data, batch_markers

    def _apply_splitting_and_sampling_methods(self, batch_spec, batch_data):
        if batch_spec.get("splitter_method"):
            splitter_fn = getattr(self, batch_spec.get("splitter_method"))
            splitter_kwargs: dict = batch_spec.get("splitter_kwargs") or {}
            batch_data = splitter_fn(batch_data, **splitter_kwargs)

        if batch_spec.get("sampling_method"):
            sampling_fn = getattr(self, batch_spec.get("sampling_method"))
            sampling_kwargs: dict = batch_spec.get("sampling_kwargs") or {}
            batch_data = sampling_fn(batch_data, **sampling_kwargs)
        return batch_data

    @staticmethod
    def guess_reader_method_from_path(path):
        """Based on a given filepath, decides a reader method. Currently supports tsv, csv, and parquet. If none of these
        file extensions are used, returns BatchKwargsError stating that it is unable to determine the current path.

        Args:
            path - A given file path

        Returns:
            A dictionary entry of format {'reader_method': reader_method}

        """
        if path.endswith(".csv") or path.endswith(".tsv"):
            return "csv"
        elif path.endswith(".parquet"):
            return "parquet"

        raise BatchKwargsError(
            "Unable to determine reader method from path: %s" % path, {"path": path}
        )

    def _get_reader_fn(self, reader, reader_method=None, path=None):
        """Static helper for providing reader_fn

        Args:
            reader: the base spark reader to use; this should have had reader_options applied already
            reader_method: the name of the reader_method to use, if specified
            path (str): the path to use to guess reader_method if it was not specified

        Returns:
            ReaderMethod to use for the filepath

        """
        if reader_method is None and path is None:
            raise BatchKwargsError(
                "Unable to determine spark reader function without reader_method or path.",
                {"reader_method": reader_method},
            )

        if reader_method is None:
            reader_method = self.guess_reader_method_from_path(path=path)

        reader_method_op: str = reader_method.lower()
        try:
            if reader_method_op == "delta":
                return reader.format(reader_method_op).load
            return getattr(reader, reader_method_op)
        except AttributeError:
            raise BatchKwargsError(
                "Unable to find reader_method %s in spark." % reader_method,
                {"reader_method": reader_method},
            )

    def get_compute_domain(
        self,
        domain_kwargs: dict,
        domain_type: Union[str, "MetricDomainTypes"],
        accessor_keys: Optional[Iterable[str]] = [],
    ) -> Tuple["pyspark.sql.DataFrame", dict, dict]:
        """Uses a given batch dictionary and domain kwargs (which include a row condition and a condition parser)
        to obtain and/or query a batch. Returns in the format of a Pandas Series if only a single column is desired,
        or otherwise a Data Frame.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain
            domain_type (str or "MetricDomainTypes") - an Enum value indicating which metric domain the user would
            like to be using, or a corresponding string value representing it. String types include "identity", "column",
            "column_pair", "table" and "other". Enum types include capitalized versions of these from the class
            MetricDomainTypes.
            accessor_keys (str iterable) - keys that are part of the compute domain but should be ignored when describing
            the domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            A tuple including:
              - a DataFrame (the data on which to compute)
              - a dictionary of compute_domain_kwargs, describing the DataFrame
              - a dictionary of accessor_domain_kwargs, describing any accessors needed to
                identify the domain within the compute domain
        """
        # Extracting value from enum if it is given for future computation
        domain_type = MetricDomainTypes(domain_type)

        batch_id = domain_kwargs.get("batch_id")
        if batch_id is None:
            # We allow no batch id specified if there is only one batch
            if self.active_batch_data:
                data = self.active_batch_data
            else:
                raise ValidationError(
                    "No batch is specified, but could not identify a loaded batch."
                )
        else:
            if batch_id in self.loaded_batch_data_dict:
                data = self.loaded_batch_data_dict[batch_id]
            else:
                raise ValidationError(f"Unable to find batch with batch_id {batch_id}")

        compute_domain_kwargs = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs = dict()
        table = domain_kwargs.get("table", None)
        if table:
            raise ValueError(
                "SparkExecutionEngine does not currently support multiple named tables."
            )

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
                    f"unrecognized condition_parser {str(condition_parser)}for Spark execution engine"
                )

        # Warning user if accessor keys are in any domain that is not of type table, will be ignored
        if (
            domain_type != MetricDomainTypes.TABLE
            and accessor_keys is not None
            and len(accessor_keys) > 0
        ):
            logger.warning(
                "Accessor keys ignored since Metric Domain Type is not 'table"
            )

        if domain_type == MetricDomainTypes.TABLE:
            if accessor_keys is not None and len(accessor_keys) > 0:
                for key in accessor_keys:
                    accessor_domain_kwargs[key] = compute_domain_kwargs.pop(key)
            if len(compute_domain_kwargs.keys()) > 0:
                for key in compute_domain_kwargs.keys():
                    # Warning user if kwarg not "normal"
                    if key not in [
                        "batch_id",
                        "table",
                        "row_condition",
                        "condition_parser",
                    ]:
                        logger.warning(
                            f"Unexpected key {key} found in domain_kwargs for domain type {domain_type.value}"
                        )
            return data, compute_domain_kwargs, accessor_domain_kwargs

        # If user has stated they want a column, checking if one is provided, and
        elif domain_type == MetricDomainTypes.COLUMN:
            if "column" in compute_domain_kwargs:
                accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")
            else:
                # If column not given
                raise GreatExpectationsError(
                    "Column not provided in compute_domain_kwargs"
                )

        # Else, if column pair values requested
        elif domain_type == MetricDomainTypes.COLUMN_PAIR:
            # Ensuring column_A and column_B parameters provided
            if (
                "column_A" in compute_domain_kwargs
                and "column_B" in compute_domain_kwargs
            ):
                accessor_domain_kwargs["column_A"] = compute_domain_kwargs.pop(
                    "column_A"
                )
                accessor_domain_kwargs["column_B"] = compute_domain_kwargs.pop(
                    "column_B"
                )
            else:
                raise GreatExpectationsError(
                    "column_A or column_B not found within compute_domain_kwargs"
                )

        # Checking if table or identity or other provided, column is not specified. If it is, warning the user
        elif domain_type == MetricDomainTypes.MULTICOLUMN:
            if "columns" in compute_domain_kwargs:
                # If columns exist
                accessor_domain_kwargs["columns"] = compute_domain_kwargs.pop("columns")

        # Filtering if identity
        elif domain_type == MetricDomainTypes.IDENTITY:

            # If we would like our data to become a single column
            if "column" in compute_domain_kwargs:
                data = data.select(compute_domain_kwargs["column"])

            # If we would like our data to now become a column pair
            elif ("column_A" in compute_domain_kwargs) and (
                "column_B" in compute_domain_kwargs
            ):
                data = data.select(
                    compute_domain_kwargs["column_A"], compute_domain_kwargs["column_B"]
                )
            else:

                # If we would like our data to become a multicolumn
                if "columns" in compute_domain_kwargs:
                    data = data.select(compute_domain_kwargs["columns"])

        return data, compute_domain_kwargs, accessor_domain_kwargs

    def add_column_row_condition(
        self, domain_kwargs, column_name=None, filter_null=True, filter_nan=False
    ):
        if filter_nan is False:
            return super().add_column_row_condition(
                domain_kwargs=domain_kwargs,
                column_name=column_name,
                filter_null=filter_null,
                filter_nan=filter_nan,
            )

        # We explicitly handle filter_nan for spark using a spark-native condition
        if "row_condition" in domain_kwargs and domain_kwargs["row_condition"]:
            raise GreatExpectationsError(
                "ExecutionEngine does not support updating existing row_conditions."
            )

        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        assert "column" in domain_kwargs or column_name is not None
        if column_name is not None:
            column = column_name
        else:
            column = domain_kwargs["column"]
        if filter_null and filter_nan:
            new_domain_kwargs[
                "row_condition"
            ] = f"NOT isnan({column}) AND {column} IS NOT NULL"
        elif filter_null:
            new_domain_kwargs["row_condition"] = f"{column} IS NOT NULL"
        elif filter_nan:
            new_domain_kwargs["row_condition"] = f"NOT isnan({column})"
        else:
            logger.warning(
                "add_column_row_condition called without specifying a desired row condition"
            )

        new_domain_kwargs["condition_parser"] = "spark"
        return new_domain_kwargs

    def resolve_metric_bundle(
        self,
        metric_fn_bundle: Iterable[Tuple[MetricConfiguration, Callable, dict]],
    ) -> dict:
        """For each metric name in the given metric_fn_bundle, finds the domain of the metric and calculates it using a
        metric function from the given provider class.

                Args:
                    metric_fn_bundle - A batch containing MetricEdgeKeys and their corresponding functions
                    metrics (dict) - A dictionary containing metrics and corresponding parameters

                Returns:
                    A dictionary of the collected metrics over their respective domains
        """

        resolved_metrics = dict()
        aggregates: Dict[Tuple, dict] = dict()
        for (
            metric_to_resolve,
            engine_fn,
            compute_domain_kwargs,
            accessor_domain_kwargs,
            metric_provider_kwargs,
        ) in metric_fn_bundle:
            if not isinstance(compute_domain_kwargs, IDDict):
                compute_domain_kwargs = IDDict(compute_domain_kwargs)
            domain_id = compute_domain_kwargs.to_id()
            if domain_id not in aggregates:
                aggregates[domain_id] = {
                    "column_aggregates": [],
                    "ids": [],
                    "domain_kwargs": compute_domain_kwargs,
                }
            aggregates[domain_id]["column_aggregates"].append(engine_fn)
            aggregates[domain_id]["ids"].append(metric_to_resolve.id)
        for aggregate in aggregates.values():
            compute_domain_kwargs = aggregate["domain_kwargs"]
            df, _, _ = self.get_compute_domain(
                compute_domain_kwargs, domain_type="identity"
            )
            assert len(aggregate["column_aggregates"]) == len(aggregate["ids"])
            condition_ids = []
            aggregate_cols = []
            for idx in range(len(aggregate["column_aggregates"])):
                column_aggregate = aggregate["column_aggregates"][idx]
                aggregate_id = str(uuid.uuid4())
                condition_ids.append(aggregate_id)
                aggregate_cols.append(column_aggregate)
            res = df.agg(*aggregate_cols).collect()
            assert (
                len(res) == 1
            ), "all bundle-computed metrics must be single-value statistics"
            assert len(aggregate["ids"]) == len(
                res[0]
            ), "unexpected number of metrics returned"
            logger.debug(
                f"SparkDFExecutionEngine computed {len(res[0])} metrics on domain_id {IDDict(compute_domain_kwargs).to_id()}"
            )
            for idx, id in enumerate(aggregate["ids"]):
                resolved_metrics[id] = res[0][idx]

        return resolved_metrics

    def head(self, n=5):
        """Returns dataframe head. Default is 5"""
        return self.dataframe.limit(n).toPandas()

    @staticmethod
    def _split_on_whole_table(
        df,
    ):
        return df

    @staticmethod
    def _split_on_column_value(
        df,
        column_name: str,
        partition_definition: dict,
    ):
        return df.filter(F.col(column_name) == partition_definition[column_name])

    @staticmethod
    def _split_on_converted_datetime(
        df,
        column_name: str,
        partition_definition: dict,
        date_format_string: str = "yyyy-MM-dd",
    ):
        matching_string = partition_definition[column_name]
        res = (
            df.withColumn(
                "date_time_tmp", F.from_unixtime(F.col(column_name), date_format_string)
            )
            .filter(F.col("date_time_tmp") == matching_string)
            .drop("date_time_tmp")
        )
        return res

    @staticmethod
    def _split_on_divided_integer(
        df,
        column_name: str,
        divisor: int,
        partition_definition: dict,
    ):
        """Divide the values in the named column by `divisor`, and split on that"""
        matching_divisor = partition_definition[column_name]
        res = (
            df.withColumn(
                "div_temp", (F.col(column_name) / divisor).cast(IntegerType())
            )
            .filter(F.col("div_temp") == matching_divisor)
            .drop("div_temp")
        )
        return res

    @staticmethod
    def _split_on_mod_integer(
        df,
        column_name: str,
        mod: int,
        partition_definition: dict,
    ):
        """Divide the values in the named column by `divisor`, and split on that"""
        matching_mod_value = partition_definition[column_name]
        res = (
            df.withColumn("mod_temp", (F.col(column_name) % mod).cast(IntegerType()))
            .filter(F.col("mod_temp") == matching_mod_value)
            .drop("mod_temp")
        )
        return res

    @staticmethod
    def _split_on_multi_column_values(
        df,
        column_names: list,
        partition_definition: dict,
    ):
        """Split on the joint values in the named columns"""
        for column_name in column_names:
            value = partition_definition.get(column_name)
            if not value:
                raise ValueError(
                    f"In order for SparkExecutionEngine to `_split_on_multi_column_values`, "
                    f"all values in  column_names must also exist in partition_definition. "
                    f"{column_name} was not found in partition_definition."
                )
            df = df.filter(F.col(column_name) == value)
        return df

    @staticmethod
    def _split_on_hashed_column(
        df,
        column_name: str,
        hash_digits: int,
        partition_definition: dict,
        hash_function_name: str = "sha256",
    ):
        """Split on the hashed value of the named column"""
        try:
            getattr(hashlib, hash_function_name)
        except (TypeError, AttributeError) as e:
            raise (
                ge_exceptions.ExecutionEngineError(
                    f"""The splitting method used with SparkDFExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found."""
                )
            )

        def _encrypt_value(to_encode):
            hash_func = getattr(hashlib, hash_function_name)
            hashed_value = hash_func(to_encode.encode()).hexdigest()[-1 * hash_digits :]
            return hashed_value

        encrypt_udf = F.udf(_encrypt_value, StringType())
        res = (
            df.withColumn("encrypted_value", encrypt_udf(column_name))
            .filter(F.col("encrypted_value") == partition_definition["hash_value"])
            .drop("encrypted_value")
        )
        return res

    ### Sampling methods ###
    @staticmethod
    def _sample_using_random(df, p: float = 0.1, seed: int = 1):
        """Take a random sample of rows, retaining proportion p"""
        res = (
            df.withColumn("rand", F.rand(seed=seed))
            .filter(F.col("rand") < p)
            .drop("rand")
        )
        return res

    @staticmethod
    def _sample_using_mod(
        df,
        column_name: str,
        mod: int,
        value: int,
    ):
        """Take the mod of named column, and only keep rows that match the given value"""
        res = (
            df.withColumn("mod_temp", (F.col(column_name) % mod).cast(IntegerType()))
            .filter(F.col("mod_temp") == value)
            .drop("mod_temp")
        )
        return res

    @staticmethod
    def _sample_using_a_list(
        df,
        column_name: str,
        value_list: list,
    ):
        """Match the values in the named column against value_list, and only keep the matches"""
        return df.where(F.col(column_name).isin(value_list))

    @staticmethod
    def _sample_using_hash(
        df,
        column_name: str,
        hash_digits: int = 1,
        hash_value: str = "f",
        hash_function_name: str = "md5",
    ):
        try:
            getattr(hashlib, str(hash_function_name))
        except (TypeError, AttributeError) as e:
            raise (
                ge_exceptions.ExecutionEngineError(
                    f"""The sampling method used with SparkDFExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found."""
                )
            )

        def _encrypt_value(to_encode):
            to_encode_str = str(to_encode)
            hash_func = getattr(hashlib, hash_function_name)
            hashed_value = hash_func(to_encode_str.encode()).hexdigest()[
                -1 * hash_digits :
            ]
            return hashed_value

        encrypt_udf = F.udf(_encrypt_value, StringType())
        res = (
            df.withColumn("encrypted_value", encrypt_udf(column_name))
            .filter(F.col("encrypted_value") == hash_value)
            .drop("encrypted_value")
        )
        return res
