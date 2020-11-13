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
from .execution_engine import ExecutionEngine, MetricDomainTypes

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import DataFrame, SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None
    logger.debug(
        "Unable to load pyspark; install optional spark dependency for support."
    )


class SparkDFBatchData:
    def __init__(
        self,
        dataframe: DataFrame = None,
        dataframe_dict: Dict[str, DataFrame] = None,
        default_table_name=None,
    ):
        assert (
            dataframe is not None or dataframe_dict is not None
        ), "dataframe or dataframe_dict is required"
        assert (
            not dataframe and dataframe_dict
        ), "dataframe and dataframe_dict may not both be specified"

        if dataframe is not None:
            dataframe_dict = {"": dataframe}
            default_table_name = ""

        self._dataframe_dict = dataframe_dict
        self._default_table_name = default_table_name

    @property
    def default(self):
        if self._default_table_name in self._dataframe_dict:
            return self._dataframe_dict[self._default_table_name]
        return None

    def __getattr__(self, item):
        return self._dataframe_dict.get(item)

    def __getitem__(self, item):
        return self._dataframe_dict.get(item)


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
        self, domain_kwargs: dict, domain_type: Union[str, "MetricDomainTypes"]
    ) -> Tuple["pyspark.sql.DataFrame", dict, dict]:
        """Uses a given batch dictionary and domain kwargs (which include a row condition and a condition parser)
        to obtain and/or query a batch. Returns in the format of a Pandas Series if only a single column is desired,
        or otherwise a Data Frame.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain
            batches (dict) - A dictionary specifying batch id and which batches to obtain
            domain_type (str or "MetricDomainTypes") - an Enum value indicating which metric domain the user would
            like to be using, or a corresponding string value representing it. String types include "identity", "column",
            "column_pair", "table" and "other".

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

        # If user has stated they want a column, checking if one is provided, and
        if domain_type == "column" or domain_type == MetricDomainTypes.COLUMN:
            if "column" in compute_domain_kwargs:
                accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")
            else:
                # If column not given
                raise GreatExpectationsError("Column not provided in compute_domain_kwargs")
        else:
            # If column pair requested
            if domain_type == "column_pair" or domain_type == MetricDomainTypes.COLUMN_PAIR:

                # Ensuring column_A and column_B parameters provided
                if "column_A" in compute_domain_kwargs and "column_B" in compute_domain_kwargs:
                    accessor_domain_kwargs["column_A"] = compute_domain_kwargs.pop("column_A")
                    accessor_domain_kwargs["column_B"] = compute_domain_kwargs.pop("column_B")
                else:
                    raise GreatExpectationsError("column_A or column_B not found within compute_domain_kwargs")

        return data, compute_domain_kwargs, accessor_domain_kwargs

    def add_column_row_condition(
        self, domain_kwargs, filter_null=True, filter_nan=False
    ):
        if filter_nan is False:
            return super().add_column_row_condition(
                domain_kwargs=domain_kwargs,
                filter_null=filter_null,
                filter_nan=filter_nan,
            )

        # We explicitly handle filter_nan for spark using a spark-native condition
        if "row_condition" in domain_kwargs and domain_kwargs["row_condition"]:
            raise GreatExpectationsError(
                "ExecutionEngine does not support updating existing row_conditions."
            )

        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        assert "column" in domain_kwargs
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
