
import copy
import datetime
import hashlib
import logging
import uuid
import warnings
from functools import reduce
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from dateutil.parser import parse
from great_expectations.core.batch import BatchMarkers
from great_expectations.core.batch_spec import AzureBatchSpec, BatchSpec, PathBatchSpec, RuntimeDataBatchSpec
from great_expectations.core.id_dict import IDDict
from great_expectations.core.util import AzureUrl, get_or_create_spark_application
from great_expectations.exceptions import BatchSpecError, ExecutionEngineError, GreatExpectationsError, ValidationError
from great_expectations.exceptions import exceptions as ge_exceptions
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
from great_expectations.execution_engine.split_and_sample.sparkdf_data_splitter import SparkDataSplitter
from great_expectations.expectations.row_conditions import RowCondition, RowConditionParserType, parse_condition_to_spark
from great_expectations.validator.metric_configuration import MetricConfiguration
logger = logging.getLogger(__name__)
try:
    import pyspark
    import pyspark.sql.functions as F
    import pyspark.sql.types as sparktypes
    from pyspark import SparkContext
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.readwriter import DataFrameReader
except ImportError:
    pyspark = None
    SparkContext = None
    SparkSession = None
    DataFrame = None
    DataFrameReader = None
    F = None
    sparktypes = None
    logger.debug('Unable to load pyspark; install optional spark dependency for support.')

def apply_dateutil_parse(column):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    assert (len(column.columns) == 1), 'Expected DataFrame with 1 column'
    col_name = column.columns[0]
    _udf = F.udf(parse, sparktypes.TimestampType())
    return column.withColumn(col_name, _udf(col_name))

class SparkDFExecutionEngine(ExecutionEngine):
    '\n    This class holds an attribute `spark_df` which is a spark.sql.DataFrame.\n\n    --ge-feature-maturity-info--\n\n        id: validation_engine_pyspark_self_managed\n        title: Validation Engine - pyspark - Self-Managed\n        icon:\n        short_description: Use Spark DataFrame to validate data\n        description: Use Spark DataFrame to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Production\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Moderate\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A -> see relevant Datasource evaluation\n            documentation_completeness: Complete\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n        id: validation_engine_databricks\n        title: Validation Engine - Databricks\n        icon:\n        short_description: Use Spark DataFrame in a Databricks cluster to validate data\n        description: Use Spark DataFrame in a Databricks cluster to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Beta\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Low (dbfs-specific handling)\n            unit_test_coverage: N/A -> implementation not different\n            integration_infrastructure_test_coverage: Minimal (we\'ve tested a bit, know others have used it)\n            documentation_completeness: Moderate (need docs on managing project configuration via dbfs/etc.)\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n        id: validation_engine_emr_spark\n        title: Validation Engine - EMR - Spark\n        icon:\n        short_description: Use Spark DataFrame in an EMR cluster to validate data\n        description: Use Spark DataFrame in an EMR cluster to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Experimental\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Low (need to provide guidance on "known good" paths, and we know there are many "knobs" to tune that we have not explored/tested)\n            unit_test_coverage: N/A -> implementation not different\n            integration_infrastructure_test_coverage: Unknown\n            documentation_completeness: Low (must install specific/latest version but do not have docs to that effect or of known useful paths)\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n        id: validation_engine_spark_other\n        title: Validation Engine - Spark - Other\n        icon:\n        short_description: Use Spark DataFrame to validate data\n        description: Use Spark DataFrame to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Experimental\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Other (we haven\'t tested possibility, known glue deployment)\n            unit_test_coverage: N/A -> implementation not different\n            integration_infrastructure_test_coverage: Unknown\n            documentation_completeness: Low (must install specific/latest version but do not have docs to that effect or of known useful paths)\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n    --ge-feature-maturity-info--\n    '
    recognized_batch_definition_keys = {'limit'}
    recognized_batch_spec_defaults = {'reader_method', 'reader_options'}

    def __init__(self, *args, persist=True, spark_config=None, force_reuse_spark_context=False, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._persist = persist
        if (spark_config is None):
            spark_config = {}
        spark: SparkSession = get_or_create_spark_application(spark_config=spark_config, force_reuse_spark_context=force_reuse_spark_context)
        spark_config = dict(spark_config)
        spark_config.update({k: v for (k, v) in spark.sparkContext.getConf().getAll()})
        self._spark_config = spark_config
        self.spark = spark
        azure_options: dict = kwargs.pop('azure_options', {})
        self._azure_options = azure_options
        super().__init__(*args, **kwargs)
        self._config.update({'persist': self._persist, 'spark_config': spark_config, 'azure_options': azure_options})
        self._data_splitter = SparkDataSplitter()

    @property
    def dataframe(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'If a batch has been loaded, returns a Spark Dataframe containing the data within the loaded batch'
        if (not self.active_batch_data):
            raise ValueError('Batch has not been loaded - please run load_batch() to load a batch.')
        return self.active_batch_data.dataframe

    def load_batch_data(self, batch_id: str, batch_data: Any) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if isinstance(batch_data, DataFrame):
            batch_data = SparkDFBatchData(self, batch_data)
        elif isinstance(batch_data, SparkDFBatchData):
            pass
        else:
            raise GreatExpectationsError('SparkDFExecutionEngine requires batch data that is either a DataFrame or a SparkDFBatchData object')
        super().load_batch_data(batch_id=batch_id, batch_data=batch_data)

    def get_batch_data_and_markers(self, batch_spec: BatchSpec) -> Tuple[(Any, BatchMarkers)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        batch_markers: BatchMarkers = BatchMarkers({'ge_load_time': datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')})
        '\n        As documented in Azure DataConnector implementations, Pandas and Spark execution engines utilize separate path\n        formats for accessing Azure Blob Storage service.  However, Pandas and Spark execution engines utilize identical\n        path formats for accessing all other supported cloud storage services (AWS S3 and Google Cloud Storage).\n        Moreover, these formats (encapsulated in S3BatchSpec and GCSBatchSpec) extend PathBatchSpec (common to them).\n        Therefore, at the present time, all cases with the exception of Azure Blob Storage , are handled generically.\n        '
        batch_data: Any
        if isinstance(batch_spec, RuntimeDataBatchSpec):
            batch_data = batch_spec.batch_data
            if isinstance(batch_data, str):
                raise ge_exceptions.ExecutionEngineError(f'''SparkDFExecutionEngine has been passed a string type batch_data, "{batch_data}", which is illegal.
Please check your config.''')
            batch_spec.batch_data = 'SparkDataFrame'
        elif isinstance(batch_spec, AzureBatchSpec):
            reader_method: str = batch_spec.reader_method
            reader_options: dict = (batch_spec.reader_options or {})
            path: str = batch_spec.path
            azure_url = AzureUrl(path)
            try:
                credential = self._azure_options.get('credential')
                storage_account_url = azure_url.account_url
                if credential:
                    self.spark.conf.set('fs.wasb.impl', 'org.apache.hadoop.fs.azure.NativeAzureFileSystem')
                    self.spark.conf.set(f'fs.azure.account.key.{storage_account_url}', credential)
                reader: DataFrameReader = self.spark.read.options(**reader_options)
                reader_fn: Callable = self._get_reader_fn(reader=reader, reader_method=reader_method, path=path)
                batch_data = reader_fn(path)
            except AttributeError:
                raise ExecutionEngineError('\n                    Unable to load pyspark. Pyspark is required for SparkDFExecutionEngine.\n                    ')
        elif isinstance(batch_spec, PathBatchSpec):
            reader_method: str = batch_spec.reader_method
            reader_options: dict = (batch_spec.reader_options or {})
            path: str = batch_spec.path
            try:
                reader: DataFrameReader = self.spark.read.options(**reader_options)
                reader_fn: Callable = self._get_reader_fn(reader=reader, reader_method=reader_method, path=path)
                batch_data = reader_fn(path)
            except AttributeError:
                raise ExecutionEngineError('\n                    Unable to load pyspark. Pyspark is required for SparkDFExecutionEngine.\n                    ')
            except pyspark.sql.utils.AnalysisException:
                raise ExecutionEngineError(f'Unable to read in batch from the following path: {path}. Please check your configuration.')
        else:
            raise BatchSpecError('\n                Invalid batch_spec: batch_data is required for a SparkDFExecutionEngine to operate.\n                ')
        batch_data = self._apply_splitting_and_sampling_methods(batch_spec, batch_data)
        typed_batch_data = SparkDFBatchData(execution_engine=self, dataframe=batch_data)
        return (typed_batch_data, batch_markers)

    def _apply_splitting_and_sampling_methods(self, batch_spec, batch_data):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        splitter_method_name: Optional[str] = batch_spec.get('splitter_method')
        if splitter_method_name:
            splitter_fn: Callable = self._data_splitter.get_splitter_method(splitter_method_name)
            splitter_kwargs: dict = (batch_spec.get('splitter_kwargs') or {})
            batch_data = splitter_fn(batch_data, **splitter_kwargs)
        if batch_spec.get('sampling_method'):
            sampling_fn = getattr(self, batch_spec.get('sampling_method'))
            sampling_kwargs: dict = (batch_spec.get('sampling_kwargs') or {})
            batch_data = sampling_fn(batch_data, **sampling_kwargs)
        return batch_data

    @staticmethod
    def guess_reader_method_from_path(path):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Based on a given filepath, decides a reader method. Currently supports tsv, csv, and parquet. If none of these\n        file extensions are used, returns ExecutionEngineError stating that it is unable to determine the current path.\n\n        Args:\n            path - A given file path\n\n        Returns:\n            A dictionary entry of format {'reader_method': reader_method}\n\n        "
        if (path.endswith('.csv') or path.endswith('.tsv')):
            return 'csv'
        elif path.endswith('.parquet'):
            return 'parquet'
        raise ExecutionEngineError(f'Unable to determine reader method from path: {path}')

    def _get_reader_fn(self, reader, reader_method=None, path=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Static helper for providing reader_fn\n\n        Args:\n            reader: the base spark reader to use; this should have had reader_options applied already\n            reader_method: the name of the reader_method to use, if specified\n            path (str): the path to use to guess reader_method if it was not specified\n\n        Returns:\n            ReaderMethod to use for the filepath\n\n        '
        if ((reader_method is None) and (path is None)):
            raise ExecutionEngineError('Unable to determine spark reader function without reader_method or path')
        if (reader_method is None):
            reader_method = self.guess_reader_method_from_path(path=path)
        reader_method_op: str = reader_method.lower()
        try:
            if (reader_method_op == 'delta'):
                return reader.format(reader_method_op).load
            return getattr(reader, reader_method_op)
        except AttributeError:
            raise ExecutionEngineError(f'Unable to find reader_method {reader_method} in spark.')

    def get_domain_records(self, domain_kwargs: dict) -> DataFrame:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Uses the given domain kwargs (which include row_condition, condition_parser, and ignore_row_if directives) to\n        obtain and/or query a batch. Returns in the format of a Spark DataFrame.\n\n        Args:\n            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain\n\n        Returns:\n            A DataFrame (the data on which to compute)\n        '
        table = domain_kwargs.get('table', None)
        if table:
            raise ValueError('SparkDFExecutionEngine does not currently support multiple named tables.')
        batch_id = domain_kwargs.get('batch_id')
        if (batch_id is None):
            if self.active_batch_data:
                data = self.active_batch_data.dataframe
            else:
                raise ValidationError('No batch is specified, but could not identify a loaded batch.')
        elif (batch_id in self.loaded_batch_data_dict):
            data = self.loaded_batch_data_dict[batch_id].dataframe
        else:
            raise ValidationError(f'Unable to find batch with batch_id {batch_id}')
        row_condition = domain_kwargs.get('row_condition', None)
        if row_condition:
            condition_parser = domain_kwargs.get('condition_parser', None)
            if (condition_parser == 'spark'):
                data = data.filter(row_condition)
            elif (condition_parser == 'great_expectations__experimental__'):
                parsed_condition = parse_condition_to_spark(row_condition)
                data = data.filter(parsed_condition)
            else:
                raise GreatExpectationsError(f'unrecognized condition_parser {str(condition_parser)} for Spark execution engine')
        filter_conditions: List[RowCondition] = domain_kwargs.get('filter_conditions', [])
        if (len(filter_conditions) > 0):
            filter_condition = self._combine_row_conditions(filter_conditions)
            data = data.filter(filter_condition.condition)
        if ('column' in domain_kwargs):
            return data
        if (('column_A' in domain_kwargs) and ('column_B' in domain_kwargs) and ('ignore_row_if' in domain_kwargs)):
            column_A_name = domain_kwargs['column_A']
            column_B_name = domain_kwargs['column_B']
            ignore_row_if = domain_kwargs['ignore_row_if']
            if (ignore_row_if == 'both_values_are_missing'):
                ignore_condition = (F.col(column_A_name).isNull() & F.col(column_B_name).isNull())
                data = data.filter((~ ignore_condition))
            elif (ignore_row_if == 'either_value_is_missing'):
                ignore_condition = (F.col(column_A_name).isNull() | F.col(column_B_name).isNull())
                data = data.filter((~ ignore_condition))
            else:
                if (ignore_row_if not in ['neither', 'never']):
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')
                if (ignore_row_if == 'never'):
                    warnings.warn(f'''The correct "no-action" value of the "ignore_row_if" directive for the column pair case is "neither" (the use of "{ignore_row_if}" is deprecated as of v0.13.29 and will be removed in v0.16).  Please use "neither" moving forward.
''', DeprecationWarning)
            return data
        if (('column_list' in domain_kwargs) and ('ignore_row_if' in domain_kwargs)):
            column_list = domain_kwargs['column_list']
            ignore_row_if = domain_kwargs['ignore_row_if']
            if (ignore_row_if == 'all_values_are_missing'):
                conditions = [F.col(column_name).isNull() for column_name in column_list]
                ignore_condition = reduce((lambda a, b: (a & b)), conditions)
                data = data.filter((~ ignore_condition))
            elif (ignore_row_if == 'any_value_is_missing'):
                conditions = [F.col(column_name).isNull() for column_name in column_list]
                ignore_condition = reduce((lambda a, b: (a | b)), conditions)
                data = data.filter((~ ignore_condition))
            elif (ignore_row_if != 'never'):
                raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')
            return data
        return data

    def _combine_row_conditions(self, row_conditions: List[RowCondition]) -> RowCondition:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Combine row conditions using AND if condition_type is SPARK_SQL\n\n        Note, although this method does not currently use `self` internally we\n        are not marking as @staticmethod since it is meant to only be called\n        internally in this class.\n\n        Args:\n            row_conditions: Row conditions of type Spark\n\n        Returns:\n            Single Row Condition combined\n        '
        assert all(((condition.condition_type == RowConditionParserType.SPARK_SQL) for condition in row_conditions)), 'All row conditions must have type SPARK_SQL'
        conditions: List[str] = [row_condition.condition for row_condition in row_conditions]
        joined_condition: str = ' AND '.join(conditions)
        return RowCondition(condition=joined_condition, condition_type=RowConditionParserType.SPARK_SQL)

    def get_compute_domain(self, domain_kwargs: dict, domain_type: Union[(str, MetricDomainTypes)], accessor_keys: Optional[Iterable[str]]=None) -> Tuple[(DataFrame, dict, dict)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Uses a given batch dictionary and domain kwargs (which include a row condition and a condition parser)\n        to obtain and/or query a batch. Returns in the format of a Spark DataFrame.\n\n        Args:\n            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type (str or MetricDomainTypes) - an Enum value indicating which metric domain the user would\n            like to be using, or a corresponding string value representing it. String types include "identity",\n            "column", "column_pair", "table" and "other". Enum types include capitalized versions of these from the\n            class MetricDomainTypes.\n            accessor_keys (str iterable) - keys that are part of the compute domain but should be ignored when\n            describing the domain and simply transferred with their associated values into accessor_domain_kwargs.\n\n        Returns:\n            A tuple including:\n              - a DataFrame (the data on which to compute)\n              - a dictionary of compute_domain_kwargs, describing the DataFrame\n              - a dictionary of accessor_domain_kwargs, describing any accessors needed to\n                identify the domain within the compute domain\n        '
        data = self.get_domain_records(domain_kwargs)
        table = domain_kwargs.get('table', None)
        if table:
            raise ValueError('SparkDFExecutionEngine does not currently support multiple named tables.')
        split_domain_kwargs = self._split_domain_kwargs(domain_kwargs, domain_type, accessor_keys)
        return (data, split_domain_kwargs.compute, split_domain_kwargs.accessor)

    def add_column_row_condition(self, domain_kwargs, column_name=None, filter_null=True, filter_nan=False):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        assert (('column' in domain_kwargs) or (column_name is not None))
        if (column_name is not None):
            column = column_name
        else:
            column = domain_kwargs['column']
        filter_conditions: List[RowCondition] = []
        if filter_null:
            filter_conditions.append(RowCondition(condition=f'{column} IS NOT NULL', condition_type=RowConditionParserType.SPARK_SQL))
        if filter_nan:
            filter_conditions.append(RowCondition(condition=f'NOT isnan({column})', condition_type=RowConditionParserType.SPARK_SQL))
        if (not (filter_null or filter_nan)):
            logger.warning('add_column_row_condition called without specifying a desired row condition')
        new_domain_kwargs.setdefault('filter_conditions', []).extend(filter_conditions)
        return new_domain_kwargs

    def resolve_metric_bundle(self, metric_fn_bundle: Iterable[Tuple[(MetricConfiguration, Callable, dict)]]) -> Dict[(Tuple[(str, str, str)], Any)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'For each metric name in the given metric_fn_bundle, finds the domain of the metric and calculates it using a\n        metric function from the given provider class.\n\n                Args:\n                    metric_fn_bundle - A batch containing MetricEdgeKeys and their corresponding functions\n\n                Returns:\n                    A dictionary of the collected metrics over their respective domains\n        '
        resolved_metrics = {}
        aggregates: Dict[(Tuple, dict)] = {}
        for (metric_to_resolve, engine_fn, compute_domain_kwargs, accessor_domain_kwargs, metric_provider_kwargs) in metric_fn_bundle:
            if (not isinstance(compute_domain_kwargs, IDDict)):
                compute_domain_kwargs = IDDict(compute_domain_kwargs)
            domain_id = compute_domain_kwargs.to_id()
            if (domain_id not in aggregates):
                aggregates[domain_id] = {'column_aggregates': [], 'ids': [], 'domain_kwargs': compute_domain_kwargs}
            aggregates[domain_id]['column_aggregates'].append(engine_fn)
            aggregates[domain_id]['ids'].append(metric_to_resolve.id)
        for aggregate in aggregates.values():
            compute_domain_kwargs = aggregate['domain_kwargs']
            df = self.get_domain_records(domain_kwargs=compute_domain_kwargs)
            assert (len(aggregate['column_aggregates']) == len(aggregate['ids']))
            condition_ids = []
            aggregate_cols = []
            for idx in range(len(aggregate['column_aggregates'])):
                column_aggregate = aggregate['column_aggregates'][idx]
                aggregate_id = str(uuid.uuid4())
                condition_ids.append(aggregate_id)
                aggregate_cols.append(column_aggregate)
            res = df.agg(*aggregate_cols).collect()
            assert (len(res) == 1), 'all bundle-computed metrics must be single-value statistics'
            assert (len(aggregate['ids']) == len(res[0])), 'unexpected number of metrics returned'
            logger.debug(f'SparkDFExecutionEngine computed {len(res[0])} metrics on domain_id {IDDict(compute_domain_kwargs).to_id()}')
            for (idx, id) in enumerate(aggregate['ids']):
                resolved_metrics[id] = res[0][idx]
        return resolved_metrics

    def head(self, n=5):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns dataframe head. Default is 5'
        return self.dataframe.limit(n).toPandas()

    @staticmethod
    def _sample_using_random(df, p: float=0.1, seed: int=1):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Take a random sample of rows, retaining proportion p'
        res = df.withColumn('rand', F.rand(seed=seed)).filter((F.col('rand') < p)).drop('rand')
        return res

    @staticmethod
    def _sample_using_mod(df, column_name: str, mod: int, value: int):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Take the mod of named column, and only keep rows that match the given value'
        res = df.withColumn('mod_temp', (F.col(column_name) % mod).cast(sparktypes.IntegerType())).filter((F.col('mod_temp') == value)).drop('mod_temp')
        return res

    @staticmethod
    def _sample_using_a_list(df, column_name: str, value_list: list):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Match the values in the named column against value_list, and only keep the matches'
        return df.where(F.col(column_name).isin(value_list))

    @staticmethod
    def _sample_using_hash(df, column_name: str, hash_digits: int=1, hash_value: str='f', hash_function_name: str='md5'):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        try:
            getattr(hashlib, str(hash_function_name))
        except (TypeError, AttributeError):
            raise ge_exceptions.ExecutionEngineError(f'''The sampling method used with SparkDFExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found.''')

        def _encrypt_value(to_encode):
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            to_encode_str = str(to_encode)
            hash_func = getattr(hashlib, hash_function_name)
            hashed_value = hash_func(to_encode_str.encode()).hexdigest()[((- 1) * hash_digits):]
            return hashed_value
        encrypt_udf = F.udf(_encrypt_value, sparktypes.StringType())
        res = df.withColumn('encrypted_value', encrypt_udf(column_name)).filter((F.col('encrypted_value') == hash_value)).drop('encrypted_value')
        return res
