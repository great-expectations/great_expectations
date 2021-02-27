import logging

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
def get_or_create_spark_session(
    name: str = "default_great_expectations_spark_dataframe_datasource",
):
    # Due to the uniqueness of SparkContext per JVM, it is impossible to change SparkSession configuration dynamically.
    # Attempts to circumvent this constraint cause "ValueError: Cannot run multiple SparkContexts at once" to be thrown.
    # Hence, SparkSession with SparkConf acceptable for all tests must be established at "pytest" collection time.
    # This is preferred to calling "return SparkSession.builder.getOrCreate()", which will result in the setting
    # ("spark.app.name", "pyspark-shell") remaining in SparkConf statically for the entire duration of the "pytest" run.
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession

        from great_expectations.datasource.sparkdf_datasource import SparkDFDatasource
    except ImportError:
        SparkContext = None
        SparkSession = None
        SparkDFDatasource = None
        # TODO: review logging more detail here
        logger.debug(
            "Unable to load pyspark; install optional spark dependency for support."
        )

    source: SparkDFDatasource = SparkDFDatasource(
        name=name,
        spark_config={
            "spark.app.name": name,
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "450m",
        },
    )
    sess: SparkSession = source.spark
    sc: SparkContext = sess.sparkContext
    # Calling "sc.stop()" after all tests have run is not easy under "pytest".
    # Thus, will make sure that during testing, SparkContext is not stopped.
    # noinspection PyProtectedMember
    sc_stopped: bool = sc._jsc.sc().isStopped()
    if sc_stopped:
        raise ValueError("SparkContext stopped unexpectedly.")

    return sess
