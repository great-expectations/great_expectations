import copy
import datetime
import logging
from functools import partial
from io import StringIO
from typing import Any, Callable, Dict, Iterable, Tuple

import pandas as pd

from great_expectations.execution_environment.types import (
    InMemoryBatchSpec,
    PathBatchSpec,
    S3BatchSpec,
)

from ..core.batch import Batch, BatchMarkers, BatchRequest
from ..core.id_dict import BatchSpec
from ..exceptions import BatchSpecError, ValidationError
from ..execution_environment.util import hash_pandas_dataframe
from ..validator.validation_graph import MetricConfiguration
from .execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)

HASH_THRESHOLD = 1e9


class PandasBatchData:
    def __init__(
        self,
        dataframe: pd.DataFrame = None,
        dataframe_dict: Dict[str, pd.DataFrame] = None,
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
    def default_dataframe(self):
        if self._default_table_name in self._dataframe_dict:
            return self._dataframe_dict[self._default_table_name]

        return None


class PandasExecutionEngine(ExecutionEngine):
    """
PandasExecutionEngine instantiates the great_expectations Expectations API as a subclass of a pandas.DataFrame.

For the full API reference, please see :func:`Dataset <great_expectations.data_asset.dataset.Dataset>`

Notes:
    1. Samples and Subsets of PandaDataSet have ALL the expectations of the original \
       data frame unless the user specifies the ``discard_subset_failing_expectations = True`` \
       property on the original data frame.
    2. Concatenations, joins, and merges of PandaDataSets contain NO expectations (since no autoinspection
       is performed by default).

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
    """

    recognized_batch_spec_defaults = {
        "reader_method",
        "reader_options",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.discard_subset_failing_expectations = kwargs.get(
            "discard_subset_failing_expectations", False
        )

    def configure_validator(self, validator):
        super().configure_validator(validator)
        validator.expose_dataframe_methods = True

    def get_batch_data_and_markers(
        self, batch_spec: BatchSpec
    ) -> Tuple[Any, BatchMarkers]:  # batch_data
        batch_data: Any = None

        # We need to build a batch_markers to be used in the dataframe
        batch_markers: BatchMarkers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        if isinstance(batch_spec, InMemoryBatchSpec):
            raise BatchSpecError(
                """The batch_spec argument must not have the type InMemoryBatchSpec when calling
"get_batch_data_and_markers()"."
                """
            )

        reader_method: str = batch_spec.get("reader_method")
        reader_options: dict = batch_spec.get("reader_options") or {}
        if isinstance(batch_spec, PathBatchSpec):
            path: str = batch_spec["path"]
            reader_fn: Callable = self._get_reader_fn(reader_method, path)
            batch_data = reader_fn(path, **reader_options)
        elif isinstance(batch_spec, S3BatchSpec):
            # TODO: <Alex>The job of S3DataConnector is to supply the URL and the S3_OBJECT (like FilesystemDataConnector supplies the PATH).</Alex>
            # TODO: <Alex>Move the code below to S3DataConnector (which will update batch_spec with URL and S3_OBJECT values.</Alex>
            # url, s3_object = data_connector.get_s3_object(batch_spec=batch_spec)
            # reader_method = batch_spec.get("reader_method")
            # reader_fn = self._get_reader_fn(reader_method, url.key)
            # batch_data = reader_fn(
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
                """Invalid batch_spec: file path, s3 path, or batch_data is required for a PandasExecutionEngine to
operate.
                """
            )

        if batch_data is not None:
            if batch_data.memory_usage().sum() < HASH_THRESHOLD:
                batch_markers["pandas_data_fingerprint"] = hash_pandas_dataframe(
                    batch_data
                )

        return batch_data, batch_markers

    @staticmethod
    def get_batch_markers_and_update_batch_spec_for_batch_data(
        batch_data: pd.DataFrame, batch_spec: BatchSpec
    ) -> BatchMarkers:
        """
        Computes batch_markers in the case of user-provided batch_data (e.g., in the case of a data pipeline).

        :param batch_data -- user-provided dataframe
        :param batch_spec -- BatchSpec (must be previously instantiated/initialized by PipelineDataConnector)
        :returns computed batch_markers specific to this execution engine
        """
        batch_markers: BatchMarkers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )
        if batch_data is not None:
            if batch_data.memory_usage().sum() < HASH_THRESHOLD:
                batch_markers["pandas_data_fingerprint"] = hash_pandas_dataframe(
                    batch_data
                )
            # we do not want to store the actual dataframe in batch_spec
            # hence, marking that this is a PandasInMemoryDF instead
            batch_spec["PandasInMemoryDF"] = True
        return batch_markers

    @property
    def dataframe(self):
        """Tests whether or not a Batch has been loaded. If the loaded batch does not exist, raises a
        ValueError Exception
        """
        # Changed to is None because was breaking prior
        if self.active_batch_data is None:
            raise ValueError(
                "Batch has not been loaded - please run load_batch_data() to load a batch."
            )

        return self.active_batch_data

    def _get_reader_fn(self, reader_method=None, path=None):
        """Static helper for parsing reader types. If reader_method is not provided, path will be used to guess the
        correct reader_method.

        Args:
            reader_method (str): the name of the reader method to use, if available.
            path (str): the path used to guess

        Returns:
            ReaderMethod to use for the filepath

        """
        if reader_method is None and path is None:
            raise BatchSpecError(
                "Unable to determine pandas reader function without reader_method or path."
            )

        reader_options = dict()
        if reader_method is None:
            path_guess = self.guess_reader_method_from_path(path)
            reader_method = path_guess["reader_method"]
            reader_options = path_guess.get(
                "reader_options"
            )  # This may not be there; use None in that case

        try:
            reader_fn = getattr(pd, reader_method)
            if reader_options:
                reader_fn = partial(reader_fn, **reader_options)
            return reader_fn
        except AttributeError:
            raise BatchSpecError(
                f'Unable to find reader_method "{reader_method}" in pandas.'
            )

    @staticmethod
    def guess_reader_method_from_path(path):
        """Helper method for deciding which reader to use to read in a certain path.

               Args:
                   path (str): the to use to guess

               Returns:
                   ReaderMethod to use for the filepath

               """
        if path.endswith(".csv") or path.endswith(".tsv"):
            return {"reader_method": "read_csv"}
        elif path.endswith(".parquet"):
            return {"reader_method": "read_parquet"}
        elif path.endswith(".xlsx") or path.endswith(".xls"):
            return {"reader_method": "read_excel"}
        elif path.endswith(".json"):
            return {"reader_method": "read_json"}
        elif path.endswith(".pkl"):
            return {"reader_method": "read_pickle"}
        elif path.endswith(".feather"):
            return {"reader_method": "read_feather"}
        elif path.endswith(".csv.gz") or path.endswith(".csv.gz"):
            return {
                "reader_method": "read_csv",
                "reader_options": {"compression": "gzip"},
            }

        raise BatchSpecError(f'Unable to determine reader method from path: "{path}".')

    # TODO: <Alex>Is this method still needed?  The DataConnector subclasses seem to accoplish the needed functionality.</Alex>
    def process_batch_request(self, batch_request: BatchRequest, batch_spec: BatchSpec):
        """Takes in a batch request and batch spec. If the batch request has a limit, uses it to initialize the
        number of rows to process for the batch spec in obtaining a batch
        Args:
            batch_definition (dict) - The batch definition as defined by the user
            batch_spec (dict) - The batch spec used to query the backend
        Returns:
             batch_spec (dict) - The batch spec used to query the backend, with the added row limit
        """
        limit = batch_request.get("limit")
        if limit is not None:
            if not batch_spec.get("reader_options"):
                batch_spec["reader_options"] = {}
            batch_spec["reader_options"]["nrows"] = limit

        # TODO: <Alex>Is this still relevant?</Alex>
        # TODO: Make sure dataset_options are accounted for in __init__ of ExecutionEngine
        # if dataset_options is not None:
        #     # Then update with any locally-specified reader options
        #     if not batch_parameters.get("dataset_options"):
        #         batch_parameters["dataset_options"] = dict()
        #     batch_parameters["dataset_options"].update(dataset_options)

        return batch_spec

    def get_compute_domain(
        self, domain_kwargs: Dict,
    ) -> Tuple[pd.DataFrame, dict, dict]:
        """Uses a given batch dictionary and domain kwargs (which include a row condition and a condition parser)
        to obtain and/or query a batch. Returns in the format of a Pandas DataFrame. If the domain is a single column,
        this is added to 'accessor domain kwargs' and used for later access

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
            if self.active_batch_data_id is not None:
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
                "PandasExecutionEngine does not currently support multiple named tables."
            )

        row_condition = domain_kwargs.get("row_condition", None)
        if row_condition:
            condition_parser = domain_kwargs.get("condition_parser", None)
            if condition_parser not in ["python", "pandas"]:
                raise ValueError(
                    "condition_parser is required when setting a row_condition,"
                    " and must be 'python' or 'pandas'"
                )
            else:
                data = data.query(row_condition, parser=condition_parser).reset_index(
                    drop=True
                )

        if "column" in compute_domain_kwargs:
            accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")

        return data, compute_domain_kwargs, accessor_domain_kwargs

    def resolve_metric_bundle(
        self, metric_fn_bundle: Iterable[Tuple[MetricConfiguration, Callable, dict]],
    ) -> dict:
        """This engine simply evaluates metrics one at a time."""
        resolved_metrics = dict()
        for (
            metric_to_resolve,
            metric_provider,
            metric_provider_kwargs,
        ) in metric_fn_bundle:
            resolved_metrics[metric_to_resolve.id] = metric_provider(
                **metric_provider_kwargs
            )
        return resolved_metrics
