# TODO: <Alex>ALEX Delete this module.</Alex>
from typing import Dict, Optional

# import logging

# logger = logging.getLogger(__name__)


# # noinspection PyPep8Naming
# def get_or_create_spark_session(
#     name: str = "default_great_expectations_spark_dataframe_datasource",
# ):
#     # Due to the uniqueness of SparkContext per JVM, it is impossible to change SparkSession configuration dynamically.
#     # Attempts to circumvent this constraint cause "ValueError: Cannot run multiple SparkContexts at once" to be thrown.
#     # Hence, SparkSession with SparkConf acceptable for all tests must be established at "pytest" collection time.
#     # This is preferred to calling "return SparkSession.builder.getOrCreate()", which will result in the setting
#     # ("spark.app.name", "pyspark-shell") remaining in SparkConf statically for the entire duration of the "pytest" run.
#     try:
#         from pyspark import SparkContext
#         from pyspark.sql import SparkSession
#
#         from great_expectations.datasource.sparkdf_datasource import SparkDFDatasource
#     except ImportError:
#         SparkContext = None
#         SparkSession = None
#         SparkDFDatasource = None
#         # TODO: review logging more detail here
#         logger.debug(
#             "Unable to load pyspark; install optional spark dependency for support."
#         )
#
#     # TODO: <Alex>ALEX</Alex>
#     # # We need to stop the old session to reconfigure it
#     # logger.info("Stopping existing spark context to reconfigure.")
#     # builder = SparkSession.builder
#     # spark = builder.getOrCreate()
#     # # noinspection PyProtectedMember
#     # if not spark.sparkContext._jsc.sc().isStopped():
#     #     spark.sparkContext.stop()
#
#     # source: SparkDFDatasource = SparkDFDatasource(
#     #     name=name,
#     #     spark_config={
#     #         "spark.app.name": name,
#     #         "spark.sql.catalogImplementation": "hive",
#     #         "spark.executor.memory": "450m",
#     #     },
#     # )
#     spark_config = {
#         "spark.app.name": name,
#         "spark.sql.catalogImplementation": "hive",
#         "spark.executor.memory": "450m",
#     }
#     source: SparkDFDatasource = SparkDFDatasource.get_spark_df_datasource(
#         name=name,
#         spark_config=spark_config
#     )
#     sess: SparkSession = source.spark
#     sc: SparkContext = sess.sparkContext
#     # Calling "sc.stop()" after all tests have run is not easy under "pytest".
#     # Thus, will make sure that during testing, SparkContext is not stopped.
#     # noinspection PyProtectedMember
#     sc_stopped: bool = sc._jsc.sc().isStopped()
#     if sc_stopped:
#         raise ValueError("SparkContext stopped unexpectedly.")
#
#     return sess


# # noinspection PyPep8Naming
# def get_or_create_spark_session(
#     spark_config: Optional[Dict[str, str]] = None,
# ) -> "SparkSession":
#     # Due to the uniqueness of SparkContext per JVM, it is impossible to change SparkSession configuration dynamically.
#     # Attempts to circumvent this constraint cause "ValueError: Cannot run multiple SparkContexts at once" to be thrown.
#     # Hence, SparkSession with SparkConf acceptable for all tests must be established at "pytest" collection time.
#     # This is preferred to calling "return SparkSession.builder.getOrCreate()", which will result in the setting
#     # ("spark.app.name", "pyspark-shell") remaining in SparkConf statically for the entire duration of the "pytest" run.
#     try:
#         from pyspark import SparkContext
#         from pyspark.sql import SparkSession
#     except ImportError:
#         SparkContext = None
#         SparkSession = None
#         # TODO: review logging more detail here
#         logger.debug(
#             "Unable to load pyspark; install optional spark dependency for support."
#         )
#
#     spark_session: Optional[SparkSession]
#     try:
#         if spark_config is None:
#             spark_config = {}
#
#         builder = SparkSession.builder
#
#         if len(spark_config) > 0:
#             app_name: Optional[str] = spark_config.get("spark.app.name")
#             if app_name:
#                 builder.appName(app_name)
#             for k, v in spark_config.items():
#                 if k != "spark.app.name":
#                     builder.config(k, v)
#         spark_session = builder.getOrCreate()
#         sc: SparkContext = spark_session.sparkContext
#         # noinspection PyProtectedMember
#         if sc._jsc.sc().isStopped():
#             raise ValueError("SparkContext stopped unexpectedly.")
#     except AttributeError:
#         logger.error(
#             "Unable to load spark context; install optional spark dependency for support."
#         )
#         spark_session = None
#
#     return spark_session
