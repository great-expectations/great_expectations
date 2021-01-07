import copy
import datetime
import hashlib
import logging
import random
from functools import partial
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

import pandas as pd
from ruamel.yaml.compat import StringIO

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.datasource.types import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.datasource.util import S3Url

try:
    import boto3
except ImportError:
    boto3 = None

from ..core.batch import BatchMarkers
from ..core.id_dict import BatchSpec
from ..datasource.util import hash_pandas_dataframe
from ..exceptions import BatchSpecError, GreatExpectationsError, ValidationError
from .execution_engine import ExecutionEngine, MetricDomainTypes

logger = logging.getLogger(__name__)

HASH_THRESHOLD = 1e9


class PandasBatchData(pd.DataFrame):
    # @property
    def row_count(self):
        return self.shape[0]


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
        self.discard_subset_failing_expectations = kwargs.get(
            "discard_subset_failing_expectations", False
        )
        boto3_options: dict = kwargs.get("boto3_options", {})

        # Try initializing boto3 client. If unsuccessful, we'll catch it when/if a S3BatchSpec is passed in.
        try:
            self._s3 = boto3.client("s3", **boto3_options)
        except (TypeError, AttributeError):
            self._s3 = None

        super().__init__(*args, **kwargs)

        self._config.update(
            {
                "discard_subset_failing_expectations": self.discard_subset_failing_expectations,
                "boto3_options": boto3_options,
            }
        )

    def configure_validator(self, validator):
        super().configure_validator(validator)
        validator.expose_dataframe_methods = True

    def get_batch_data_and_markers(
        self, batch_spec: BatchSpec
    ) -> Tuple[Any, BatchMarkers]:  # batch_data
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
            batch_spec.batch_data = "PandasDataFrame"

        elif isinstance(batch_spec, PathBatchSpec):
            reader_method: str = batch_spec.get("reader_method")
            reader_options: dict = batch_spec.get("reader_options") or {}

            path: str = batch_spec["path"]
            reader_fn: Callable = self._get_reader_fn(reader_method, path)

            batch_data = reader_fn(path, **reader_options)

        elif isinstance(batch_spec, S3BatchSpec):
            if self._s3 is None:
                raise ge_exceptions.ExecutionEngineError(
                    f"""PandasExecutionEngine has been passed a S3BatchSpec,
                        but the ExecutionEngine does not have a boto3 client configured. Please check your config."""
                )
            s3_engine = self._s3
            s3_url = S3Url(batch_spec.get("s3"))
            reader_method: str = batch_spec.get("reader_method")
            reader_options: dict = batch_spec.get("reader_options") or {}

            s3_object = s3_engine.get_object(Bucket=s3_url.bucket, Key=s3_url.key)

            logger.debug(
                "Fetching s3 object. Bucket: {} Key: {}".format(
                    s3_url.bucket, s3_url.key
                )
            )
            reader_fn = self._get_reader_fn(reader_method, s3_url.key)
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

        typed_batch_data = self._get_typed_batch_data(batch_data)

        return typed_batch_data, batch_markers

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

    def _get_typed_batch_data(self, batch_data):
        typed_batch_data = PandasBatchData(batch_data)
        return typed_batch_data

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
        elif path.endswith(".csv.gz") or path.endswith(".tsv.gz"):
            return {
                "reader_method": "read_csv",
                "reader_options": {"compression": "gzip"},
            }

        raise BatchSpecError(f'Unable to determine reader method from path: "{path}".')

    def get_compute_domain(
        self,
        domain_kwargs: dict,
        domain_type: Union[str, "MetricDomainTypes"],
        accessor_keys: Optional[Iterable[str]] = [],
    ) -> Tuple[pd.DataFrame, dict, dict]:
        """Uses a given batch dictionary and domain kwargs (which include a row condition and a condition parser)
        to obtain and/or query a batch. Returns in the format of a Pandas DataFrame. If the domain is a single column,
        this is added to 'accessor domain kwargs' and used for later access

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
            if self.active_batch_data_id is not None:
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
                "PandasExecutionEngine does not currently support multiple named tables."
            )

        # Filtering by row condition
        row_condition = domain_kwargs.get("row_condition", None)
        if row_condition:
            condition_parser = domain_kwargs.get("condition_parser", None)

            # Ensuring proper condition parser has been provided
            if condition_parser not in ["python", "pandas"]:
                raise ValueError(
                    "condition_parser is required when setting a row_condition,"
                    " and must be 'python' or 'pandas'"
                )
            else:
                # Querying row condition
                data = data.query(row_condition, parser=condition_parser).reset_index(
                    drop=True
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

        # If given table (this is default), get all unexpected accessor_keys (an optional parameters allowing us to
        # modify domain access)
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
                accessor_domain_kwargs["columns"] = compute_domain_kwargs.pop("columns")

        # Filtering if identity
        elif domain_type == MetricDomainTypes.IDENTITY:

            # If we would like our data to become a single column
            if "column" in compute_domain_kwargs:
                data = pd.DataFrame(data[compute_domain_kwargs["column"]])

            # If we would like our data to now become a column pair
            elif ("column_A" in compute_domain_kwargs) and (
                "column_B" in compute_domain_kwargs
            ):

                # Dropping all not needed columns
                column_a, column_b = (
                    compute_domain_kwargs["column_A"],
                    compute_domain_kwargs["column_B"],
                )
                data = pd.DataFrame(
                    {column_a: data[column_a], column_b: data[column_b]}
                )

            else:
                # If we would like our data to become a multicolumn
                if "columns" in compute_domain_kwargs:
                    data = data[compute_domain_kwargs["columns"]]

        return data, compute_domain_kwargs, accessor_domain_kwargs

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

        return df[df[column_name] == partition_definition[column_name]]

    @staticmethod
    def _split_on_converted_datetime(
        df,
        column_name: str,
        partition_definition: dict,
        date_format_string: str = "%Y-%m-%d",
    ):
        """Convert the values in the named column to the given date_format, and split on that"""
        stringified_datetime_series = df[column_name].map(
            lambda x: x.strftime(date_format_string)
        )
        matching_string = partition_definition[column_name]
        return df[stringified_datetime_series == matching_string]

    @staticmethod
    def _split_on_divided_integer(
        df,
        column_name: str,
        divisor: int,
        partition_definition: dict,
    ):
        """Divide the values in the named column by `divisor`, and split on that"""

        matching_divisor = partition_definition[column_name]
        matching_rows = df[column_name].map(
            lambda x: int(x / divisor) == matching_divisor
        )

        return df[matching_rows]

    @staticmethod
    def _split_on_mod_integer(
        df,
        column_name: str,
        mod: int,
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
                raise ValueError(
                    f"In order for PandasExecution to `_split_on_multi_column_values`, "
                    f"all values in column_names must also exist in partition_definition. "
                    f"{column_name} was not found in partition_definition."
                )
            subset_df = subset_df[subset_df[column_name] == value]
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
            raise (
                ge_exceptions.ExecutionEngineError(
                    f"""The splitting method used with SparkDFExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found."""
                )
            )
        matching_rows = df[column_name].map(
            lambda x: hash_method(str(x).encode()).hexdigest()[-1 * hash_digits :]
            == partition_definition["hash_value"]
        )
        return df[matching_rows]

    ### Sampling methods ###

    @staticmethod
    def _sample_using_random(
        df,
        p: float = 0.1,
    ):
        """Take a random sample of rows, retaining proportion p

        Note: the Random function behaves differently on different dialects of SQL
        """
        return df[df.index.map(lambda x: random.random() < p)]

    @staticmethod
    def _sample_using_mod(
        df,
        column_name: str,
        mod: int,
        value: int,
    ):
        """Take the mod of named column, and only keep rows that match the given value"""
        return df[df[column_name].map(lambda x: x % mod == value)]

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
        hash_value: str = "f",
        hash_function_name: str = "md5",
    ):
        """Hash the values in the named column, and split on that"""
        try:
            hash_func = getattr(hashlib, hash_function_name)
        except (TypeError, AttributeError) as e:
            raise (
                ge_exceptions.ExecutionEngineError(
                    f"""The sampling method used with PandasExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found."""
                )
            )

        matches = df[column_name].map(
            lambda x: hash_func(str(x).encode()).hexdigest()[-1 * hash_digits :]
            == hash_value
        )
        return df[matches]
