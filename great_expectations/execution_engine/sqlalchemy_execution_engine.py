import datetime
import logging
from urllib.parse import urlparse

try:
    import sqlalchemy as sa
except ImportError:
    sa = None

from great_expectations.core.batch import BatchMarkers
from great_expectations.exceptions import InvalidConfigError
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class SqlAlchemyExecutionEngine(ExecutionEngine):

    def __init__(self, credentials=None, data_context=None, engine=None, connection_string=None, url=None, **kwargs):
        super().__init__(data_context=data_context)
        if engine is not None:
            if credentials is not None:
                logger.warning(
                    "Both credentials and engine were provided during initialization of SqlAlchemyExecutionEngine. "
                    "Ignoring credentials.")
            self.engine = engine
        elif credentials is not None:
            self.engine = self._build_engine(credentials=credentials, **kwargs)
        elif connection_string is not None:
            self.engine = sa.create_engine(connection_string, **kwargs)
        elif url is not None:
            self.drivername = urlparse(url).scheme
            self.engine = sa.create_engine(url, **kwargs)
        else:
            raise InvalidConfigError("Credentials or an engine are required for a SqlAlchemyExecutionEngine.")
        self.engine.connect()

        # Send a connect event to provide dialect type
        if data_context is not None and getattr(
            data_context, "_usage_statistics_handler", None
        ):
            handler = data_context._usage_statistics_handler
            handler.send_usage_message(
                # TODO: 20200916 - JPC: update event and anonymizer
                event="datasource.sqlalchemy.connect",
                event_payload={
                    "anonymized_name": handler._datasource_anonymizer.anonymize(
                        self.name
                    ),
                    "sqlalchemy_dialect": self.engine.name,
                },
                success=True,
            )

    def _connect(self, credentials) -> sa.engine.Engine:
        self._execution_engine_config = {}

        else:
        (
            options,
            create_engine_kwargs,
            drivername,
        ) = self._get_sqlalchemy_connection_options(**kwargs)
        self.drivername = drivername
        self.engine = create_engine(options, **create_engine_kwargs)
        self.engine.connect()

    def load_batch(self, batch_definition, in_memory_dataset=None):
        """
        With the help of the execution environment and data connector specified within the batch definition,
        builds a batch spec and utilizes it to load a batch using the appropriate file reader and the given file path.

       Args:
           batch_definition (dict): A dictionary specifying the parameters used to build the batch
           in_memory_dataset (A Pandas DataFrame or None): Optional specification of an in memory Dataset used
                                                            to load a batch. A Data Asset name and partition ID
                                                            must still be passed via batch definition.

       """
        execution_environment_name = batch_definition.get("execution_environment")
        execution_environment = self._data_context.get_execution_environment(
            execution_environment_name
        )

        # We need to build a batch_markers to be used in the dataframe
        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        data_connector_name = batch_definition.get("data_connector")
        assert data_connector_name, "Batch definition must specify a data_connector"

        data_connector = execution_environment.get_data_connector(data_connector_name)
        batch_spec = data_connector.build_batch_spec(batch_definition=batch_definition)
        batch_id = batch_spec.to_id()

        if in_memory_dataset is not None:
            if batch_definition.get("data_asset_name") and batch_definition.get(
                "partition_id"
            ):
                df = in_memory_dataset
            else:
                raise ValueError(
                    "To pass an in_memory_dataset, you must also pass a data_asset_name "
                    "and partition_id"
                )
        else:
            if data_connector.get_config().get("class_name") == "DataConnector":
                raise ValueError(
                    "No in_memory_dataset found. To use a data_connector with class DataConnector, please ensure that "
                    "you are passing a dataset to load_batch()"
                )

            # We will use and manipulate reader_options along the way
            reader_options = batch_spec.get("reader_options", {})

            if isinstance(batch_spec, PathBatchSpec):
                path = batch_spec["path"]
                reader_method = batch_spec.get("reader_method")
                reader_fn = self._get_reader_fn(reader_method, path)
                df = reader_fn(path, **reader_options)
            elif isinstance(batch_spec, S3BatchSpec):
                url, s3_object = data_connector.get_s3_object(batch_spec=batch_spec)
                reader_method = batch_spec.get("reader_method")
                reader_fn = self._get_reader_fn(reader_method, url.key)
                df = reader_fn(
                    StringIO(
                        s3_object["Body"]
                        .read()
                        .decode(s3_object.get("ContentEncoding", "utf-8"))
                    ),
                    **reader_options,
                )
            else:
                raise BatchSpecError(
                    "Invalid batch_spec: path, s3, or df is required for a PandasDatasource",
                    batch_spec,
                )

        if df.memory_usage().sum() < HASH_THRESHOLD:
            batch_markers["pandas_data_fingerprint"] = hash_pandas_dataframe(df)

        if not self.batches.get(batch_id):
            batch = Batch(
                execution_environment_name=batch_definition.get(
                    "execution_environment"
                ),
                batch_spec=batch_spec,
                data=df,
                batch_definition=batch_definition,
                batch_markers=batch_markers,
                data_context=self._data_context,
            )
            self.batches[batch_id] = batch

        self._loaded_batch_id = batch_id
