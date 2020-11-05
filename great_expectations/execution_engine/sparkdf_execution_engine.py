import copy
import datetime
import logging
import uuid
from typing import Any, Callable, Dict, Iterable, Tuple, Union

try:
    import pyspark.sql.functions as F
except ImportError:
    F = None

from great_expectations.core.id_dict import IDDict

from ..exceptions import BatchKwargsError, GreatExpectationsError, ValidationError
from ..expectations.row_conditions import parse_condition_to_spark
from ..validator.validation_graph import MetricConfiguration
from .execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None
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

        self._spark_config = kwargs.pop("spark_config ", {})
        try:
            builder = SparkSession.builder
            app_name: Union[str, None] = self._spark_config.pop("spark.app.name", None)
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

    def load_batch(self, batch_spec: BatchSpec = None) -> Batch:
        """
        Utilizes the provided batch spec to load a batch using the appropriate file reader and the given file path.
        :arg batch_spec the parameters used to build the batch
        :returns Batch
        """
        batch_spec._id_ignore_keys = {"dataset"}
        batch_id = batch_spec.to_id()

        # We need to build a batch_markers to be used in the dataframe
        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        if isinstance(batch_spec, InMemoryBatchSpec):
            # We do not want to store the actual dataframe in batch_spec (mark that this is SparkDFRef instead).
            batch_data = batch_spec.pop("batch_data")
            batch_spec["SparkInMemoryDF"] = True
            if batch_data is not None:
                if batch_spec.get("data_asset_name"):
                    df = batch_data
                else:
                    raise ValueError("To pass an batch_data, you must also a data_asset_name as well.")
        else:
            reader = self.spark.read
            reader_method = batch_spec.get("reader_method")
            reader_options = batch_spec.get("reader_options") or {}
            for option in reader_options.items():
                reader = reader.option(*option)
            if isinstance(batch_spec, PathBatchSpec):
                path = batch_spec["path"]
                reader_fn = self._get_reader_fn(reader, reader_method, path)
                df = reader_fn(path)
            elif isinstance(batch_spec, S3BatchSpec):
                # TODO: <Alex>The job of S3DataConnector is to supply the URL and the S3_OBJECT (like FilesystemDataConnector supplies the PATH).</Alex>
                # TODO: <Alex>Move the code below to S3DataConnector (which will update batch_spec with URL and S3_OBJECT values.</Alex>
                # url, s3_object = data_connector.get_s3_object(batch_spec=batch_spec)
                # reader_fn = self._get_reader_fn(reader, reader_method, url.key)
                # df = reader_fn(
                #     StringIO(
                #         s3_object["Body"]
                #         .read()
                #         .decode(s3_object.get("ContentEncoding", "utf-8"))
                #     ),
                #     **reader_options,
                # )
                pass
            else:
                raise BatchSpecError(
                    "Invalid batch_spec: file path, s3 path, or df is required for a SparkDFExecutionEngine to operate."
                )

        limit = batch_spec.get("limit")
        if limit:
            df = df.limit(limit)

        if self._persist:
            df.persist()

        if not self.batches.get(batch_id) or self.batches.get(batch_id).batch_spec != batch_spec:
            batch = Batch(
                data=df,
                batch_spec=batch_spec,
                batch_markers=batch_markers,
            )
            self.batches[batch_id] = batch
        else:
            batch = self.batches.get(batch_id)

        self._loaded_batch_id = batch_id
        return batch

    @property
    def dataframe(self):
        """If a batch has been loaded, returns a Spark Dataframe containing the data within the loaded batch"""
        if not self.active_batch_data:
            raise ValueError(
                "Batch has not been loaded - please run load_batch() to load a batch."
            )

        return self.active_batch_data

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
            return {"reader_method": "csv"}
        elif path.endswith(".parquet"):
            return {"reader_method": "parquet"}

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
            reader_method = self.guess_reader_method_from_path(path=path)[
                "reader_method"
            ]

        try:
            if reader_method.lower() == "delta":
                return reader.format("delta").load

            return getattr(reader, reader_method)
        except AttributeError:
            raise BatchKwargsError(
                "Unable to find reader_method %s in spark." % reader_method,
                {"reader_method": reader_method},
            )

    def process_batch_definition(self, batch_definition, batch_spec):
        """Given that the batch definition has a limit state, transfers the limit dictionary entry from the batch_definition
        to the batch_spec.
        Args:
            batch_definition: The batch definition to use in configuring the batch spec's limit
            batch_spec: a batch_spec dictionary whose limit needs to be configured
        Returns:
            ReaderMethod to use for the filepath
        """
        limit = batch_definition.get("limit")
        if limit is not None:
            if not batch_spec.get("limit"):
                batch_spec["limit"] = limit
        return batch_spec

    def get_compute_domain(
        self, domain_kwargs: dict
    ) -> Tuple["pyspark.sql.DataFrame", dict, dict]:
        """Uses a given batch dictionary and domain kwargs (which include a row condition and a condition parser)
        to obtain and/or query a batch. Returns in the format of a Pandas Series if only a single column is desired,
        or otherwise a Data Frame.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain
            batches (dict) - A dictionary specifying batch id and which batches to obtain

        Returns:
            A tuple including:
              - a DataFrame (the data on which to compute)
              - a dictionary of compute_domain_kwargs, describing the DataFrame
              - a dictionary of accessor_domain_kwargs, describing any accessors needed to
                identify the domain within the compute domain
        """
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
            if batch_id in self.loaded_batch_data:
                data = self.loaded_batch_data[batch_id]
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

        if "column" in compute_domain_kwargs:
            accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")

        return data, compute_domain_kwargs, accessor_domain_kwargs

    def _get_eval_column_name(self, column):
        """Given the name of a column (string), returns the name of the corresponding eval column"""
        return "__eval_col_" + column.replace(".", "__").replace("`", "_")

    def resolve_metric_bundle(
        self, metric_fn_bundle: Iterable[Tuple[MetricConfiguration, Callable, dict]],
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
            metric_provider,
            metric_provider_kwargs,
        ) in metric_fn_bundle:
            assert (
                metric_provider.metric_fn_type == "aggregate_fn"
            ), "resolve_metric_bundle only supports aggregate metrics"
            # batch_id and table are the only determining factors for bundled metrics
            column_aggregate, domain_kwargs = metric_provider(**metric_provider_kwargs)
            if not isinstance(domain_kwargs, IDDict):
                domain_kwargs = IDDict(domain_kwargs)
            domain_id = domain_kwargs.to_id()
            if domain_id not in aggregates:
                aggregates[domain_id] = {
                    "column_aggregates": [],
                    "ids": [],
                    "domain_kwargs": domain_kwargs,
                }
            aggregates[domain_id]["column_aggregates"].append(column_aggregate)
            aggregates[domain_id]["ids"].append(metric_to_resolve.id)
        for aggregate in aggregates.values():
            df, compute_domain_kwargs, _ = self.get_compute_domain(
                aggregate["domain_kwargs"]
            )
            assert (
                compute_domain_kwargs == aggregate["domain_kwargs"]
            ), "Invalid compute domain returned from a bundled metric. Verify that its target compute domain is a valid compute domain."
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
            for idx, id in enumerate(aggregate["ids"]):
                resolved_metrics[id] = res[0][idx]

        return resolved_metrics

    def head(self, n=5):
        """Returns dataframe head. Default is 5"""
        return self.dataframe.limit(n).toPandas()
