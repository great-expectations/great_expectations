
import datetime
import logging
import warnings
from pathlib import Path
from string import Template
from great_expectations.core.batch import Batch, BatchMarkers
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.util import nested_update
from great_expectations.data_context.types.base import ConcurrencyConfig
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyBatchReference
from great_expectations.datasource import LegacyDatasource
from great_expectations.exceptions import DatasourceInitializationError, DatasourceKeyPairAuthBadPassphraseError
from great_expectations.types import ClassConfig
from great_expectations.types.configurations import classConfigSchema
from great_expectations.util import get_sqlalchemy_url, import_make_url
logger = logging.getLogger(__name__)
try:
    import sqlalchemy
    from sqlalchemy import create_engine
    from sqlalchemy.sql.elements import quoted_name
    make_url = import_make_url()
except ImportError:
    sqlalchemy = None
    create_engine = None
    logger.debug('Unable to import sqlalchemy.')
if (sqlalchemy != None):
    try:
        import google.auth
        datasource_initialization_exceptions = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError, sqlalchemy.exc.ArgumentError, google.auth.exceptions.GoogleAuthError)
    except ImportError:
        datasource_initialization_exceptions = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError, sqlalchemy.exc.ArgumentError)

class SqlAlchemyDatasource(LegacyDatasource):
    '\n    A SqlAlchemyDatasource will provide data_assets converting batch_kwargs using the following rules:\n        - if the batch_kwargs include a table key, the datasource will provide a dataset object connected to that table\n        - if the batch_kwargs include a query key, the datasource will create a temporary table usingthat query. The query can be parameterized according to the standard python Template engine, which uses $parameter, with additional kwargs passed to the get_batch method.\n\n    --ge-feature-maturity-info--\n        id: datasource_postgresql\n        title: Datasource - PostgreSQL\n        icon:\n        short_description: Postgres\n        description: Support for using the open source PostgresQL database as an external datasource and execution engine.\n        how_to_guide_url:\n        maturity: Production\n        maturity_details:\n            api_stability: High\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: Complete\n            documentation_completeness: Medium (does not have a specific how-to, but easy to use overall)\n            bug_risk: Low\n            expectation_completeness: Moderate\n\n        id: datasource_bigquery\n        title: Datasource - BigQuery\n        icon:\n        short_description: BigQuery\n        description: Use Google BigQuery as an execution engine and external datasource to validate data.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_datasources/how_to_configure_a_bigquery_datasource.html\n        maturity: Beta\n        maturity_details:\n            api_stability: Unstable (table generator inability to work with triple-dotted, temp table usability, init flow calls setup "other")\n            implementation_completeness: Moderate\n            unit_test_coverage: Partial (no test coverage for temp table creation)\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Partial (how-to does not cover all cases)\n            bug_risk: High (we *know* of several bugs, including inability to list tables, SQLAlchemy URL incomplete)\n            expectation_completeness: Moderate\n\n        id: datasource_redshift\n        title: Datasource - Amazon Redshift\n        icon:\n        short_description: Redshift\n        description: Use Amazon Redshift as an execution engine and external datasource to validate data.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_datasources/how_to_configure_a_redshift_datasource.html\n        maturity: Beta\n        maturity_details:\n            api_stability: Moderate (potential metadata/introspection method special handling for performance)\n            implementation_completeness: Complete\n            unit_test_coverage: Minimal\n            integration_infrastructure_test_coverage: Minimal (none automated)\n            documentation_completeness: Moderate\n            bug_risk: Moderate\n            expectation_completeness: Moderate\n\n        id: datasource_snowflake\n        title: Datasource - Snowflake\n        icon:\n        short_description: Snowflake\n        description: Use Snowflake Computing as an execution engine and external datasource to validate data.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_datasources/how_to_configure_a_snowflake_datasource.html\n        maturity: Production\n        maturity_details:\n            api_stability: High\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: Minimal (manual only)\n            documentation_completeness: Complete\n            bug_risk: Low\n            expectation_completeness: Complete\n\n        id: datasource_mssql\n        title: Datasource - Microsoft SQL Server\n        icon:\n        short_description: Microsoft SQL Server\n        description: Use Microsoft SQL Server as an execution engine and external datasource to validate data.\n        how_to_guide_url:\n        maturity: Experimental\n        maturity_details:\n            api_stability: High\n            implementation_completeness: Moderate\n            unit_test_coverage: Minimal (none)\n            integration_infrastructure_test_coverage: Minimal (none)\n            documentation_completeness: Minimal\n            bug_risk: High\n            expectation_completeness: Low (some required queries do not generate properly, such as related to nullity)\n\n        id: datasource_mysql\n        title: Datasource - MySQL\n        icon:\n        short_description: MySQL\n        description: Use MySQL as an execution engine and external datasource to validate data.\n        how_to_guide_url:\n        maturity: Experimental\n        maturity_details:\n            api_stability: Low (no consideration for temp tables)\n            implementation_completeness: Low (no consideration for temp tables)\n            unit_test_coverage: Minimal (none)\n            integration_infrastructure_test_coverage: Minimal (none)\n            documentation_completeness:  Minimal (none)\n            bug_risk: Unknown\n            expectation_completeness: Unknown\n\n        id: datasource_mariadb\n        title: Datasource - MariaDB\n        icon:\n        short_description: MariaDB\n        description: Use MariaDB as an execution engine and external datasource to validate data.\n        how_to_guide_url:\n        maturity: Experimental\n        maturity_details:\n            api_stability: Low (no consideration for temp tables)\n            implementation_completeness: Low (no consideration for temp tables)\n            unit_test_coverage: Minimal (none)\n            integration_infrastructure_test_coverage: Minimal (none)\n            documentation_completeness:  Minimal (none)\n            bug_risk: Unknown\n            expectation_completeness: Unknown\n    '
    recognized_batch_parameters = {'query_parameters', 'limit', 'dataset_options'}

    @classmethod
    def build_configuration(cls, data_asset_type=None, batch_kwargs_generators=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "\n        Build a full configuration object for a datasource, potentially including generators with defaults.\n\n        Args:\n            data_asset_type: A ClassConfig dictionary\n            batch_kwargs_generators: Generator configuration dictionary\n            **kwargs: Additional kwargs to be part of the datasource constructor's initialization\n\n        Returns:\n            A complete datasource configuration.\n\n        "
        if (data_asset_type is None):
            data_asset_type = {'class_name': 'SqlAlchemyDataset', 'module_name': 'great_expectations.dataset'}
        else:
            data_asset_type = classConfigSchema.dump(ClassConfig(**data_asset_type))
        configuration = kwargs
        configuration['data_asset_type'] = data_asset_type
        if (batch_kwargs_generators is not None):
            configuration['batch_kwargs_generators'] = batch_kwargs_generators
        return configuration

    def __init__(self, name='default', data_context=None, data_asset_type=None, credentials=None, batch_kwargs_generators=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not sqlalchemy):
            raise DatasourceInitializationError(name, "ModuleNotFoundError: No module named 'sqlalchemy'")
        configuration_with_defaults = SqlAlchemyDatasource.build_configuration(data_asset_type, batch_kwargs_generators, **kwargs)
        data_asset_type = configuration_with_defaults.pop('data_asset_type')
        batch_kwargs_generators = configuration_with_defaults.pop('batch_kwargs_generators', None)
        super().__init__(name, data_context=data_context, data_asset_type=data_asset_type, batch_kwargs_generators=batch_kwargs_generators, **configuration_with_defaults)
        if (credentials is not None):
            self._datasource_config.update({'credentials': credentials})
        else:
            credentials = {}
        try:
            if ('engine' in kwargs):
                self.engine = kwargs.pop('engine')
            else:
                concurrency: ConcurrencyConfig
                if ((data_context is None) or (data_context.concurrency is None)):
                    concurrency = ConcurrencyConfig()
                else:
                    concurrency = data_context.concurrency
                concurrency.add_sqlalchemy_create_engine_parameters(kwargs)
                if ('connection_string' in kwargs):
                    connection_string = kwargs.pop('connection_string')
                    self.engine = create_engine(connection_string, **kwargs)
                    connection = self.engine.connect()
                    connection.close()
                elif ('url' in credentials):
                    url = credentials.pop('url')
                    parsed_url = make_url(url)
                    self.drivername = parsed_url.drivername
                    self.engine = create_engine(url, **kwargs)
                    connection = self.engine.connect()
                    connection.close()
                else:
                    (options, create_engine_kwargs, drivername) = self._get_sqlalchemy_connection_options(**kwargs)
                    self.drivername = drivername
                    self.engine = create_engine(options, **create_engine_kwargs)
                    connection = self.engine.connect()
                    connection.close()
            if ((data_context is not None) and getattr(data_context, '_usage_statistics_handler', None)):
                handler = data_context._usage_statistics_handler
                handler.send_usage_message(event=UsageStatsEvents.DATASOURCE_SQLALCHEMY_CONNECT.value, event_payload={'anonymized_name': handler.anonymizer.anonymize(obj=self.name), 'sqlalchemy_dialect': self.engine.name}, success=True)
        except datasource_initialization_exceptions as sqlalchemy_error:
            raise DatasourceInitializationError(self._name, str(sqlalchemy_error))
        self._build_generators()

    def _get_sqlalchemy_connection_options(self, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        drivername = None
        if ('credentials' in self._datasource_config):
            credentials = self._datasource_config['credentials']
        else:
            credentials = {}
        create_engine_kwargs = {}
        connect_args = credentials.pop('connect_args', None)
        if connect_args:
            create_engine_kwargs['connect_args'] = connect_args
        if ('connection_string' in credentials):
            options = credentials['connection_string']
        elif ('url' in credentials):
            options = credentials['url']
        else:
            drivername = credentials.pop('drivername')
            schema_name = credentials.pop('schema_name', None)
            if (schema_name is not None):
                logger.warning('schema_name specified creating a URL with schema is not supported. Set a default schema on the user connecting to your database.')
            if ('private_key_path' in credentials):
                (options, create_engine_kwargs) = self._get_sqlalchemy_key_pair_auth_url(drivername, credentials)
            else:
                options = get_sqlalchemy_url(drivername, **credentials)
        return (options, create_engine_kwargs, drivername)

    def _get_sqlalchemy_key_pair_auth_url(self, drivername, credentials):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        private_key_path = credentials.pop('private_key_path')
        private_key_passphrase = credentials.pop('private_key_passphrase')
        with Path(private_key_path).expanduser().resolve().open(mode='rb') as key:
            try:
                p_key = serialization.load_pem_private_key(key.read(), password=(private_key_passphrase.encode() if private_key_passphrase else None), backend=default_backend())
            except ValueError as e:
                if ('incorrect password' in str(e).lower()):
                    raise DatasourceKeyPairAuthBadPassphraseError(datasource_name='SqlAlchemyDatasource', message='Decryption of key failed, was the passphrase incorrect?') from e
                else:
                    raise e
        pkb = p_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8, encryption_algorithm=serialization.NoEncryption())
        credentials_driver_name = credentials.pop('drivername', None)
        create_engine_kwargs = {'connect_args': {'private_key': pkb}}
        return (get_sqlalchemy_url((drivername or credentials_driver_name), **credentials), create_engine_kwargs)

    def get_batch(self, batch_kwargs, batch_parameters=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        batch_markers = BatchMarkers({'ge_load_time': datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')})
        if ('bigquery_temp_table' in batch_kwargs):
            warnings.warn('BigQuery tables that are created as the result of a query are no longer created as permanent tables. Thus, a named permanent table through the `bigquery_temp_table`parameter is not required. The `bigquery_temp_table` parameter is deprecated as ofv0.15.3 and will be removed in v0.18.', DeprecationWarning)
        if ('snowflake_transient_table' in batch_kwargs):
            query_support_table_name = batch_kwargs.get('snowflake_transient_table')
        else:
            query_support_table_name = None
        if ('query' in batch_kwargs):
            if (('limit' in batch_kwargs) or ('offset' in batch_kwargs)):
                logger.warning('Limit and offset parameters are ignored when using query-based batch_kwargs; consider adding limit and offset directly to the generated query.')
            if ('query_parameters' in batch_kwargs):
                query = Template(batch_kwargs['query']).safe_substitute(batch_kwargs['query_parameters'])
            else:
                query = batch_kwargs['query']
            batch_reference = SqlAlchemyBatchReference(engine=self.engine, query=query, table_name=query_support_table_name, schema=batch_kwargs.get('schema'))
        elif ('table' in batch_kwargs):
            table = batch_kwargs['table']
            if batch_kwargs.get('use_quoted_name'):
                table = quoted_name(table, quote=True)
            limit = batch_kwargs.get('limit')
            offset = batch_kwargs.get('offset')
            if ((limit is not None) or (offset is not None)):
                logger.info('Generating query from table batch_kwargs based on limit and offset')
                if (self.engine.dialect.name.lower() == 'bigquery'):
                    schema = None
                else:
                    schema = batch_kwargs.get('schema')
                if (self.engine.dialect.name.lower() == 'oracle'):
                    raw_query = sqlalchemy.select([sqlalchemy.text('*')]).select_from(sqlalchemy.schema.Table(table, sqlalchemy.MetaData(), schema=schema))
                else:
                    raw_query = sqlalchemy.select([sqlalchemy.text('*')]).select_from(sqlalchemy.schema.Table(table, sqlalchemy.MetaData(), schema=schema)).offset(offset).limit(limit)
                query = str(raw_query.compile(self.engine, compile_kwargs={'literal_binds': True}))
                if (self.engine.dialect.name.lower() == 'oracle'):
                    query += ('\nWHERE ROWNUM <= %d' % limit)
                batch_reference = SqlAlchemyBatchReference(engine=self.engine, query=query, table_name=query_support_table_name, schema=batch_kwargs.get('schema'))
            else:
                batch_reference = SqlAlchemyBatchReference(engine=self.engine, table_name=table, schema=batch_kwargs.get('schema'))
        else:
            raise ValueError("Invalid batch_kwargs: exactly one of 'table' or 'query' must be specified")
        return Batch(datasource_name=self.name, batch_kwargs=batch_kwargs, data=batch_reference, batch_parameters=batch_parameters, batch_markers=batch_markers, data_context=self._data_context)

    def process_batch_parameters(self, query_parameters=None, limit=None, dataset_options=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        batch_kwargs = super().process_batch_parameters(limit=limit, dataset_options=dataset_options)
        nested_update(batch_kwargs, {'query_parameters': query_parameters})
        return batch_kwargs
