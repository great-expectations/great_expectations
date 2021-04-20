import locale
import logging
import os
from typing import Optional

import great_expectations as ge
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations.datasource.new_datasource import Datasource
from great_expectations.util import is_library_loadable
from ruamel.yaml import YAML

yaml = YAML()

###
#
# NOTE: THE en_US.UTF-8 LOCALE IS THE DEFAULT FOR STRING FORMATTING
#
###

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

logger = logging.getLogger(__name__)

try:
    import pyspark
except ImportError:
    pyspark = None
    logger.warning(
        "Unable to load pyspark; install optional spark dependency for support."
    )

try:
    import sqlalchemy as sa
except ImportError:
    sa = None
    logger.warning(
        "Unable to load sqlalchemy; install optional sqlalchemy dependency for support."
    )

try:
    import boto3
except ImportError:
    boto3 = None
    logger.warning(
        "Unable to load boto3; install optional boto3 dependency for support."
    )

class TaxiDataContext:
    # noinspection PyShadowingNames
    @staticmethod
    def create_yellow_tripdata_data_context_with_multiple_datasources_and_multiple_execution_engines(
        project_root_dir: Optional[str] = None,
    ):
        context: ge.data_context.DataContext = ge.data_context.DataContext.create(
            project_root_dir=project_root_dir,
            usage_statistics_enabled=True,
            runtime_environment=None,
        )

        datasource_name: str
        bucket: str
        prefix: str = ""
        datasource_config: str
        datasource: Datasource

        data_path: str = os.path.join("..", "test_data", "reports")

        datasource_name = "yellow_tripdata_pandas_filesystem_datasource"

        datasource_config = f"""
            name: {datasource_name}
            class_name: Datasource
            execution_engine:
                class_name: PandasExecutionEngine
            data_connectors:
                yellow_tripdata_filepath_stem_inferred_asset_data_connector:
                    class_name: InferredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.*)\\.csv
                        group_names:
                            - data_asset_name
                yellow_tripdata_filepath_stem_configured_asset_data_connector:
                    class_name: ConfiguredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata: {{}}
                yellow_tripdata_date_string_configured_asset_data_connector:
                    class_name: ConfiguredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_date_string:
                            base_directory: {data_path}
                            pattern: (.+)_(.+)\\.csv
                            group_names:
                                - name
                                - date_string
                yellow_tripdata_year_month_configured_asset_data_connector:
                    class_name: ConfiguredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_year_month:
                            base_directory: {data_path}
                            pattern: (.+)_(\\d{4})-(\\d{2})\\.csv
                            group_names:
                                - name
                                - year
                                - month
                runtime_data_connector:
                    class_name: RuntimeDataConnector
                    batch_identifiers:
                        - pipeline_stage_name
                        - airflow_run_id
        """

        # noinspection PyUnusedLocal
        datasource = context.test_yaml_config(
            name=datasource_name, yaml_config=datasource_config, pretty_print=False
        )
        sanitize_yaml_and_save_datasource(
            context=context, datasource_yaml=datasource_config, overwrite_existing=True
        )

        datasource_name = "yellow_tripdata_pandas_s3_datasource"

        bucket = "great-expectations-test-taxi-data"

        datasource_config = f"""
            name: {datasource_name}
            class_name: Datasource
            execution_engine:
                class_name: PandasExecutionEngine
            data_connectors:
                yellow_tripdata_filepath_stem_inferred_asset_data_connector:
                    class_name: InferredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.*)\\.csv
                        group_names:
                            - data_asset_name
                yellow_tripdata_filepath_stem_configured_asset_data_connector:
                    class_name: ConfiguredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata: {{}}
                yellow_tripdata_date_string_configured_asset_data_connector:
                    class_name: ConfiguredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_date_string:
                            bucket: {bucket}
                            prefix: {prefix}
                            pattern: (.+)_(.+)\\.csv
                            group_names:
                                - name
                                - date_string
                yellow_tripdata_year_month_configured_asset_data_connector:
                    class_name: ConfiguredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_year_month:
                            bucket: {bucket}
                            prefix: {prefix}
                            pattern: (.+)_(\\d{4})-(\\d{2})\\.csv
                            group_names:
                                - name
                                - year
                                - month
                runtime_data_connector:
                    class_name: RuntimeDataConnector
                    batch_identifiers:
                        - pipeline_stage_name
                        - airflow_run_id
        """

        if (boto3 is not None) and is_library_loadable(library_name="boto3"):
            # noinspection PyUnusedLocal,PyRedeclaration
            datasource = context.test_yaml_config(
                name=datasource_name, yaml_config=datasource_config, pretty_print=False
            )
            sanitize_yaml_and_save_datasource(
                context=context,
                datasource_yaml=datasource_config,
                overwrite_existing=True,
            )

        datasource_name = "yellow_tripdata_spark_filesystem_datasource"

        datasource_config = f"""
            name: {datasource_name}
            class_name: Datasource
            execution_engine:
                class_name: SparkDFExecutionEngine
            data_connectors:
                yellow_tripdata_filepath_stem_inferred_asset_data_connector:
                    class_name: InferredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.*)\\.csv
                        group_names:
                            - data_asset_name
                yellow_tripdata_filepath_stem_configured_asset_data_connector:
                    class_name: ConfiguredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata: {{}}
                yellow_tripdata_date_string_configured_asset_data_connector:
                    class_name: ConfiguredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_date_string:
                            base_directory: {data_path}
                            pattern: (.+)_(.+)\\.csv
                            group_names:
                                - name
                                - date_string
                yellow_tripdata_year_month_configured_asset_data_connector:
                    class_name: ConfiguredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_year_month:
                            base_directory: {data_path}
                            pattern: (.+)_(\\d{4})-(\\d{2})\\.csv
                            group_names:
                                - name
                                - year
                                - month
                runtime_data_connector:
                    class_name: RuntimeDataConnector
                    batch_identifiers:
                        - pipeline_stage_name
                        - airflow_run_id
        """

        if (pyspark is not None) and is_library_loadable(library_name="pyspark"):
            # noinspection PyUnusedLocal,PyRedeclaration
            datasource = context.test_yaml_config(
                name=datasource_name, yaml_config=datasource_config, pretty_print=False
            )
            sanitize_yaml_and_save_datasource(
                context=context,
                datasource_yaml=datasource_config,
                overwrite_existing=True,
            )

        datasource_name = "yellow_tripdata_spark_s3_datasource"

        bucket = "great-expectations-test-taxi-data"

        datasource_config = f"""
            name: {datasource_name}
            class_name: Datasource
            execution_engine:
                class_name: SparkDFExecutionEngine
            data_connectors:
                yellow_tripdata_filepath_stem_inferred_asset_data_connector:
                    class_name: InferredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.*)\\.csv
                        group_names:
                            - data_asset_name
                yellow_tripdata_filepath_stem_configured_asset_data_connector:
                    class_name: ConfiguredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata: {{}}
                yellow_tripdata_date_string_configured_asset_data_connector:
                    class_name: ConfiguredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_date_string:
                            bucket: {bucket}
                            prefix: {prefix}
                            pattern: (.+)_(.+)\\.csv
                            group_names:
                                - name
                                - date_string
                yellow_tripdata_year_month_configured_asset_data_connector:
                    class_name: ConfiguredAssetS3DataConnector
                    bucket: {bucket}
                    prefix: {prefix}
                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        yellow_tripdata_year_month:
                            bucket: {bucket}
                            prefix: {prefix}
                            pattern: (.+)_(\\d{4})-(\\d{2})\\.csv
                            group_names:
                                - name
                                - year
                                - month
                runtime_data_connector:
                    class_name: RuntimeDataConnector
                    batch_identifiers:
                        - pipeline_stage_name
                        - airflow_run_id
        """

        if (
            (pyspark is not None)
            and is_library_loadable(library_name="pyspark")
            and (boto3 is not None)
            and is_library_loadable(library_name="boto3")
        ):
            # noinspection PyUnusedLocal,PyRedeclaration
            datasource = context.test_yaml_config(
                name=datasource_name, yaml_config=datasource_config, pretty_print=False
            )
            sanitize_yaml_and_save_datasource(
                context=context,
                datasource_yaml=datasource_config,
                overwrite_existing=True,
            )

        db_file_path: str = os.path.join(
            "..", "test_data", "sql_test_sets", "yellow_tripdata.db"
        )

        datasource_name = "yellow_tripdata_sqlite_datasource"

        datasource_config = f"""
            name: {datasource_name}
            class_name: SimpleSqlalchemyDatasource
            connection_string: sqlite:///{db_file_path}
            introspection:
                whole_table: {{}}
        """

        if (sa is not None) and is_library_loadable(library_name="sqlalchemy"):
            # noinspection PyUnusedLocal,PyRedeclaration
            datasource = context.test_yaml_config(
                name=datasource_name, yaml_config=datasource_config, pretty_print=False
            )
            sanitize_yaml_and_save_datasource(
                context=context,
                datasource_yaml=datasource_config,
                overwrite_existing=True,
            )


if __name__ == "__main__":
    # TODO: <Alex>ALEX Enable the specification of "project_root_dir" as a command-line argument.</Alex>
    project_root_dir: str = "."
    TaxiDataContext().create_yellow_tripdata_data_context_with_multiple_datasources_and_multiple_execution_engines(
        project_root_dir=project_root_dir
    )