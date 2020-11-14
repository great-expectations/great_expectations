import copy
import datetime
import logging
from functools import partial
import hashlib
import random
from typing import Any, Callable, Dict, Iterable, Tuple, List

import boto3
from ruamel.yaml.compat import StringIO

import pandas as pd

import great_expectations.exceptions.exceptions as ge_exceptions

from great_expectations.execution_environment.util import S3Url

from great_expectations.execution_environment.data_connector import ConfiguredAssetS3DataConnector, InferredAssetS3DataConnector

from great_expectations.execution_environment.types import (
    PathBatchSpec,
    S3BatchSpec,
    RuntimeDataBatchSpec,
)

from ..core.batch import BatchMarkers
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
        self,
        batch_spec: BatchSpec
    ) -> Tuple[
        Any,  # batch_data
        BatchMarkers
    ]:
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

        elif isinstance(batch_spec, PathBatchSpec):
            reader_method: str = batch_spec.get("reader_method")
            reader_options: dict = batch_spec.get("reader_options") or {}

            path: str = batch_spec["path"]
            reader_fn: Callable = self._get_reader_fn(reader_method, path)

            batch_data = reader_fn(path, **reader_options)

        elif isinstance(batch_spec, S3BatchSpec):
            # TODO: <Alex>The job of S3DataConnector is to supply the URL and the S3_OBJECT (like FilesystemDataConnector supplies the PATH).</Alex>
            # TODO: <Alex>Move the code below to S3DataConnector (which will update batch_spec with URL and S3_OBJECT values.</Alex>
            #
            # if self.data_connector is None:
            #     raise ge_exceptions.ExecutionEngineError(f'''
            #         S3BatchSpec requires that a data_connector is configured for PandasExecutionEngine. Please add appropriate DataConnector and try again
            #         You can either use a ConfiguredAssetS3DataConnector or InferredAssetS3DataConnector.
            #         Please check documentation for more information
            #         ''')
            # if not isinstance(self.data_connector, ConfiguredAssetS3DataConnector) or not isinstance(self.data_connector, InferredAssetS3DataConnector):
            #     raise ge_exceptions.ExecutionEngineError(f'''
            #                         S3BatchSpec requires a connection to ConfiguredAssetS3DataConnector or InferredAssetS3DataConnector.
            #                         The current data_connector is of type {type(self.data_connector)}.
            #                         Please check documentation for more information
            #                         ''')

            #url, s3_object = self.data_connector.get_s3_object(batch_spec=batch_spec)
            reader_method = batch_spec.get("reader_method")
            reader_options: dict = batch_spec.get("reader_options") or {}
            url = S3Url(batch_spec.get("s3"))
            print(
                "Fetching s3 object. Bucket: {} Key: {}".format(url.bucket, url.key)
            )
            boto3_options = {}
            s3 = boto3.client("s3", **boto3_options)
            s3_object = s3.get_object(Bucket=url.bucket, Key=url.key)
            reader_fn = self._get_reader_fn(reader_method, url.key)
            batch_data = reader_fn(
                StringIO(
                    s3_object["Body"]
                    .read()
                    .decode(s3_object.get("ContentEncoding", "utf-8"))
                ),
                **reader_options,
            )
        else:
            raise BatchSpecError(
                f"batch_spec must be of type RuntimeDataBatchSpec, PathBatchSpec, or S3BatchSpec, not {batch_spec.__class__.__name__}"
            )
        batch_data = self._apply_splitting_and_sampling_methods(batch_spec, batch_data)
        if batch_data.memory_usage().sum() < HASH_THRESHOLD:
            batch_markers["pandas_data_fingerprint"] = hash_pandas_dataframe(batch_data)
        return batch_data, batch_markers

    def _apply_splitting_and_sampling_methods(self, batch_spec, batch_data):
        if batch_spec.get("splitter_method"):
            splitter_fn = getattr(self, batch_spec.get("splitter_method"))
            splitter_kwargs: str = batch_spec.get("splitter_kwargs") or {}
            batch_data = splitter_fn(batch_data, **splitter_kwargs)

        if batch_spec.get("sampling_method"):
            sampling_fn = getattr(self, batch_spec.get("sampling_method"))
            sampling_kwargs: str = batch_spec.get("sampling_kwargs") or {}
            batch_data = sampling_fn(batch_data, **sampling_kwargs)
        return batch_data

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

    # NOTE Abe 20201105: Any reason this shouldn't be a private method?
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

    ### Splitter methods for partitioning dataframes ###
    @staticmethod
    def _split_on_whole_table(
        df,
    ) -> pd.DataFrame:
        return df

    @staticmethod
    def _split_on_column_value(
        df,
        column_name: str,
        partition_definition: dict,
    ) -> pd.DataFrame:

        return df[df[column_name]==partition_definition[column_name]]

    @staticmethod
    def _split_on_converted_datetime(
        df,
        column_name: str,
        partition_definition: dict,
        date_format_string: str = "%Y-%m-%d",
    ):
        """Convert the values in the named column to the given date_format, and split on that"""
        stringified_datetime_series = df[column_name].map(lambda x: x.strftime(date_format_string))
        matching_string = partition_definition[column_name]
        return df[stringified_datetime_series == matching_string]

    @staticmethod
    def _split_on_divided_integer(
        df,
        column_name: str,
        divisor:int,
        partition_definition: dict,
    ):
        """Divide the values in the named column by `divisor`, and split on that"""

        matching_divisor = partition_definition[column_name]
        matching_rows = df[column_name].map(lambda x: int(x/divisor)==matching_divisor)

        return df[matching_rows]

    @staticmethod
    def _split_on_mod_integer(
        df,
        column_name: str,
        mod:int,
        partition_definition: dict,
    ):
        """Divide the values in the named column by `divisor`, and split on that"""

        matching_mod_value = partition_definition[column_name]
        matching_rows = df[column_name].map(lambda x: x % mod == matching_mod_value)

        return df[matching_rows]

    @staticmethod
    def _split_on_multi_column_values(
        df,
        column_names: List[str],
        partition_definition: dict,
    ):
        """Split on the joint values in the named columns"""

        subset_df = df.copy()
        for column_name in column_names:
            value = partition_definition.get(column_name)
            if not value:
                raise ValueError(f"In order for PandasExecution to `_split_on_multi_column_values`, "
                                 f"all values in column_names must also exist in partition_definition. "
                                 f"{column_name} was not found in partition_definition.")
            subset_df = subset_df[subset_df[column_name]==value]
        return subset_df

    @staticmethod
    def _split_on_hashed_column(
        df,
        column_name: str,
        hash_digits: int,
        partition_definition: dict,
        hash_function_name: str = "md5",

    ):
        """Split on the hashed value of the named column"""
        try:
            hash_method = getattr(hashlib, hash_function_name)
        except (TypeError, AttributeError) as e:
            raise (ge_exceptions.ExecutionEngineError(
                f'''The splitting method used with SparkDFExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found.'''))
        matching_rows = df[column_name].map(
            lambda x: hash_method(str(x).encode()).hexdigest()[-1*hash_digits:] == partition_definition["hash_value"]
        )
        return df[matching_rows]

    ### Sampling methods ###

    @staticmethod
    def _sample_using_random(
        df,
        p: float = .1,
    ):
        """Take a random sample of rows, retaining proportion p
        
        Note: the Random function behaves differently on different dialects of SQL
        """
        return df[df.index.map( lambda x: random.random() < p )]

    @staticmethod
    def _sample_using_mod(
        df,
        column_name: str,
        mod: int,
        value: int,
    ):
        """Take the mod of named column, and only keep rows that match the given value"""
        return df[df[column_name].map( lambda x: x % mod == value)]

    @staticmethod
    def _sample_using_a_list(
        df,
        column_name: str,
        value_list: list,
    ):
        """Match the values in the named column against value_list, and only keep the matches"""
        return df[df[column_name].isin(value_list)]

    @staticmethod
    def _sample_using_hash(
        df,
        column_name: str,
        hash_digits: int = 1,
        hash_value: str = 'f',
        hash_function_name: str = "md5",
    ):
        """Hash the values in the named column, and split on that"""
        try:
            hash_func = getattr(hashlib, hash_function_name)
        except (TypeError, AttributeError) as e:
            raise (ge_exceptions.ExecutionEngineError(
                f'''The sampling method used with PandasExecutionEngine has a reference to an invalid hash_function_name.  
                    Reference to {hash_function_name} cannot be found.'''))

        matches = df[column_name].map(
            lambda x: hash_func(str(x).encode()).hexdigest()[-1*hash_digits:] == hash_value
        )
        return df[matches]
