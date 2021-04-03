import datetime
import json
import locale
import os
import random
import shutil
from types import ModuleType
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from ruamel.yaml import YAML

import great_expectations as ge
from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration, expectationSuiteSchema
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.util import get_or_create_spark_application
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.datasource import SqlAlchemyDatasource
from great_expectations.datasource.new_datasource import Datasource
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.self_check.util import (
    LockingConnectionCheck,
    expectationSuiteSchema,
    expectationSuiteValidationResultSchema,
    get_dataset,
)
from great_expectations.util import import_library_module, is_library_loadable
from tests.test_utils import create_files_in_directory

yaml = YAML()
###
#
# NOTE: THESE TESTS ARE WRITTEN WITH THE en_US.UTF-8 LOCALE AS DEFAULT FOR STRING FORMATTING
#
###

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "smoketest: mark test as smoketest--it does not have useful assertions but may produce side effects "
        "that require manual inspection.",
    )
    config.addinivalue_line(
        "markers",
        "rendered_output: produces rendered output that should be manually reviewed.",
    )
    config.addinivalue_line(
        "markers",
        "aws_integration: runs aws integration test that may be very slow and requires credentials",
    )


def pytest_addoption(parser):
    parser.addoption(
        "--no-spark",
        action="store_true",
        help="If set, suppress all tests against the spark test suite",
    )
    parser.addoption(
        "--no-sqlalchemy",
        action="store_true",
        help="If set, suppress all tests using sqlalchemy",
    )
    parser.addoption(
        "--no-postgresql",
        action="store_true",
        help="If set, suppress all tests against postgresql",
    )
    parser.addoption(
        "--mysql",
        action="store_true",
        help="If set, execute tests against mysql",
    )
    parser.addoption(
        "--mssql",
        action="store_true",
        help="If set, execute tests against mssql",
    )
    parser.addoption(
        "--aws-integration",
        action="store_true",
        help="If set, run aws integration tests",
    )


def build_test_backends_list(metafunc):
    test_backends = ["PandasDataset"]
    no_spark = metafunc.config.getoption("--no-spark")
    if not no_spark:
        try:
            import pyspark
            from pyspark.sql import SparkSession
        except ImportError:
            raise ValueError("spark tests are requested, but pyspark is not installed")
        test_backends += ["SparkDFDataset"]
    no_sqlalchemy = metafunc.config.getoption("--no-sqlalchemy")
    if not no_sqlalchemy:
        test_backends += ["sqlite"]

        sa: Optional[ModuleType] = import_library_module(module_name="sqlalchemy")

        no_postgresql = metafunc.config.getoption("--no-postgresql")
        if not (sa is None or no_postgresql):
            ###
            # NOTE: 20190918 - JPC: Since I've had to relearn this a few times, a note here.
            # SQLALCHEMY coerces postgres DOUBLE_PRECISION to float, which loses precision
            # round trip compared to NUMERIC, which stays as a python DECIMAL

            # Be sure to ensure that tests (and users!) understand that subtlety,
            # which can be important for distributional expectations, for example.
            ###
            db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
            connection_string = f"postgresql://postgres@{db_hostname}/test_ci"
            checker = LockingConnectionCheck(sa, connection_string)
            if checker.is_valid() is True:
                test_backends += ["postgresql"]
            else:
                raise ValueError(
                    f"backend-specific tests are requested, but unable "
                    f"to connect to the database at {connection_string}"
                )
        mysql = metafunc.config.getoption("--mysql")
        if sa and mysql:
            db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
            try:
                engine = sa.create_engine(f"mysql+pymysql://root@{db_hostname}/test_ci")
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                raise ImportError(
                    "mysql tests are requested, but unable to connect to the mysql database at "
                    f"'mysql+pymysql://root@{db_hostname}/test_ci'"
                )
            test_backends += ["mysql"]
        mssql = metafunc.config.getoption("--mssql")
        if sa and mssql:
            db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
            try:
                engine = sa.create_engine(
                    f"mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?"
                    "driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true",
                    # echo=True,
                )
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                raise ImportError(
                    "mssql tests are requested, but unable to connect to the mssql database at "
                    f"'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?"
                    "driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true'",
                )
            test_backends += ["mssql"]
    return test_backends


def build_test_backends_list_cfe(metafunc):
    test_backends = ["pandas"]
    no_spark = metafunc.config.getoption("--no-spark")
    if not no_spark:
        try:
            import pyspark
            from pyspark.sql import SparkSession
        except ImportError:
            raise ValueError("spark tests are requested, but pyspark is not installed")
        test_backends += ["spark"]
    no_sqlalchemy = metafunc.config.getoption("--no-sqlalchemy")
    if not no_sqlalchemy:
        test_backends += ["sqlite"]

        sa: Optional[ModuleType] = import_library_module(module_name="sqlalchemy")

        no_postgresql = metafunc.config.getoption("--no-postgresql")
        if not (sa is None or no_postgresql):
            ###
            # NOTE: 20190918 - JPC: Since I've had to relearn this a few times, a note here.
            # SQLALCHEMY coerces postgres DOUBLE_PRECISION to float, which loses precision
            # round trip compared to NUMERIC, which stays as a python DECIMAL

            # Be sure to ensure that tests (and users!) understand that subtlety,
            # which can be important for distributional expectations, for example.
            ###
            db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
            connection_string = f"postgresql://postgres@{db_hostname}/test_ci"
            checker = LockingConnectionCheck(sa, connection_string)
            if checker.is_valid() is True:
                test_backends += ["postgresql"]
            else:
                raise ValueError(
                    f"backend-specific tests are requested, but unable to connect to the database at "
                    f"{connection_string}"
                )
        mysql = metafunc.config.getoption("--mysql")
        if sa and mysql:
            try:
                db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
                engine = sa.create_engine(f"mysql+pymysql://root@{db_hostname}/test_ci")
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                raise ImportError(
                    "mysql tests are requested, but unable to connect to the mysql database at "
                    f"'mysql+pymysql://root@{db_hostname}/test_ci'"
                )
            test_backends += ["mysql"]
        mssql = metafunc.config.getoption("--mssql")
        if sa and mssql:
            db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
            try:
                engine = sa.create_engine(
                    f"mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?"
                    "driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true",
                    # echo=True,
                )
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                raise ImportError(
                    "mssql tests are requested, but unable to connect to the mssql database at "
                    f"'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?"
                    "driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true'",
                )
            test_backends += ["mssql"]
    return test_backends


def pytest_generate_tests(metafunc):
    test_backends = build_test_backends_list(metafunc)
    if "test_backend" in metafunc.fixturenames:
        metafunc.parametrize("test_backend", test_backends, scope="module")
    if "test_backends" in metafunc.fixturenames:
        metafunc.parametrize("test_backends", [test_backends], scope="module")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--aws-integration"):
        # --aws-integration given in cli: do not skip aws-integration tests
        return
    skip_aws_integration = pytest.mark.skip(
        reason="need --aws-integration option to run"
    )
    for item in items:
        if "aws_integration" in item.keywords:
            item.add_marker(skip_aws_integration)


@pytest.fixture(autouse=True)
def no_usage_stats(monkeypatch):
    # Do not generate usage stats from test runs
    monkeypatch.setenv("GE_USAGE_STATS", "False")


@pytest.fixture
def sa(test_backends):
    if not any(
        [dbms in test_backends for dbms in ["postgresql", "sqlite", "mysql", "mssql"]]
    ):
        pytest.skip("No recognized sqlalchemy backend selected.")
    else:
        try:
            import sqlalchemy as sa

            return sa
        except ImportError:
            raise ValueError("SQL Database tests require sqlalchemy to be installed.")


@pytest.mark.order(index=2)
@pytest.fixture
def spark_session(test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    try:
        import pyspark
        from pyspark.sql import SparkSession

        return get_or_create_spark_application(
            spark_config={
                "spark.sql.catalogImplementation": "hive",
                "spark.executor.memory": "450m",
                # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
            }
        )
    except ImportError:
        raise ValueError("spark tests are requested, but pyspark is not installed")


@pytest.fixture
def basic_spark_df_execution_engine(spark_session):
    from great_expectations.execution_engine import SparkDFExecutionEngine

    conf: List[tuple] = spark_session.sparkContext.getConf().getAll()
    spark_config: Dict[str, str] = dict(conf)
    execution_engine: SparkDFExecutionEngine = SparkDFExecutionEngine(
        spark_config=spark_config,
    )
    return execution_engine


@pytest.mark.order(index=3)
@pytest.fixture
def spark_session_v012(test_backends):
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    try:
        import pyspark
        from pyspark.sql import SparkSession

        return get_or_create_spark_application(
            spark_config={
                "spark.sql.catalogImplementation": "hive",
                "spark.executor.memory": "450m",
                # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
            }
        )
    except ImportError:
        raise ValueError("spark tests are requested, but pyspark is not installed")


@pytest.fixture
def empty_expectation_suite():
    expectation_suite = {
        "expectation_suite_name": "default",
        "meta": {},
        "expectations": [],
    }
    return expectation_suite


@pytest.fixture
def basic_expectation_suite():
    expectation_suite = ExpectationSuite(
        expectation_suite_name="default",
        meta={},
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "infinities"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist", kwargs={"column": "nulls"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist", kwargs={"column": "naturals"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "naturals"},
            ),
        ],
    )
    return expectation_suite


@pytest.fixture
def file_data_asset(tmp_path):
    tmp_path = str(tmp_path)
    path = os.path.join(tmp_path, "file_data_asset.txt")
    with open(path, "w+") as file:
        file.write(json.dumps([0, 1, 2, 3, 4]))

    return ge.data_asset.FileDataAsset(file_path=path)


@pytest.fixture
def numeric_high_card_dict():
    data = {
        "norm_0_1": [
            0.7225866251125405,
            -0.5951819764073379,
            -0.2679313226299394,
            -0.22503289285616823,
            0.1432092195399402,
            1.1874676802669433,
            1.2766412196640815,
            0.15197071140718296,
            -0.08787273509474242,
            -0.14524643717509128,
            -1.236408169492396,
            -0.1595432263317598,
            1.0856768114741797,
            0.5082788229519655,
            0.26419244684748955,
            -0.2532308428977167,
            -0.6362679196021943,
            -3.134120304969242,
            -1.8990888524318292,
            0.15701781863102648,
            -0.775788419966582,
            -0.7400872167978756,
            -0.10578357492485335,
            0.30287010067847436,
            -1.2127058770179304,
            -0.6750567678010801,
            0.3341434318919877,
            1.8336516507046157,
            1.105410842250908,
            -0.7711783703442725,
            -0.20834347267477862,
            -0.06315849766945486,
            0.003016997583954831,
            -1.0500016329150343,
            -0.9168020284223636,
            0.306128397266698,
            1.0980602112281863,
            -0.10465519493772572,
            0.4557797534454941,
            -0.2524452955086468,
            -1.6176089110359837,
            0.46251282530754667,
            0.45751208998354903,
            0.4222844954971609,
            0.9651098606162691,
            -0.1364401431697167,
            -0.4988616288584964,
            -0.29549238375582904,
            0.6950204582392359,
            0.2975369992016046,
            -1.0159498719807218,
            1.3704532401348395,
            1.1210419577766673,
            1.2051869452003332,
            0.10749349867353084,
            -3.1876892257116562,
            1.316240976262548,
            -1.3777452919511493,
            -1.0666211985935259,
            1.605446695828751,
            -0.39682821266996865,
            -0.2828059717857655,
            1.30488698803017,
            -2.116606225467923,
            -0.2026680301462151,
            -0.05504008273574069,
            -0.028520163428411835,
            0.4424105678123449,
            -0.3427628263418371,
            0.23805293411919937,
            -0.7515414823259695,
            -0.1272505897548366,
            1.803348436304099,
            -2.0178252709022124,
            0.4860300090112474,
            1.2304054166426217,
            0.7228668982068365,
            1.7400607500575112,
            0.3480274098246697,
            -0.3887978895385282,
            -1.6511926233909175,
            0.14517929503564567,
            -1.1599010576123796,
            -0.016133552438119002,
            0.47157644883706273,
            0.27657785075518254,
            1.4464286976282463,
            -1.2605489185634533,
            -1.2548765025615338,
            0.0755319579826929,
            1.0476733637516833,
            -0.7038690219524807,
            -0.9580696842862921,
            -0.18135657098008018,
            -0.18163993379314564,
            0.4092798531146971,
            -2.049808182546896,
            -1.2447062617916826,
            -1.6681140306283337,
            1.0709944517933483,
            -0.7059385234342846,
            -0.8033587669003331,
            -1.8152275905903312,
            0.11729996097670137,
            2.2994900038012376,
            -0.1291192451734159,
            -0.6731565869164164,
            -0.06690994571366346,
            -0.40330072968473235,
            -0.23927186025094221,
            2.7756216937096676,
            0.06441299443146056,
            -0.5095247173507204,
            -0.5228853558871007,
            0.806629654091097,
            -2.110096084114651,
            -0.1233374136509439,
            -1.021178519845751,
            0.058906278340351045,
            -0.26316852406211017,
            -1.2990807244026237,
            -0.1937986598084067,
            0.3909222793445317,
            0.578027315076297,
            -0.11837271520846208,
            -1.134297652720464,
            0.496915417153268,
            -0.5315184110418045,
            0.5284176849952198,
            -1.6810338988102331,
            0.41220454054009154,
            1.0554031136792,
            -1.4222775023918832,
            -1.1664353586956209,
            0.018952180522661358,
            -0.04620616876577671,
            -0.8446292647938418,
            -0.6889432180332509,
            -0.16012081070647954,
            0.5680940644754282,
            -1.9792941921407943,
            0.35441842206114726,
            0.12433268557499534,
            0.25366905921805377,
            0.6262297786892028,
            1.327981424671081,
            1.774834324890265,
            -0.9725604763128438,
            0.42824027889428,
            0.19725541390327114,
            1.4640606982992412,
            1.6484993842838995,
            0.009848260786412894,
            -2.318740403198263,
            -0.4125245127403577,
            -0.15500831770388285,
            1.010740123094443,
            0.7509498708766653,
            -0.021415407776108144,
            0.6466776546788641,
            -1.421096837521404,
            0.5632248951325018,
            -1.230539161899903,
            -0.26766333435961503,
            -1.7208241092827994,
            -1.068122926814994,
            -1.6339248620455546,
            0.07225436117508208,
            -1.2018233250224348,
            -0.07213000691963527,
            -1.0080992229563746,
            -1.151378048476321,
            -0.2660104149809121,
            1.6307779136408695,
            0.8394822016824073,
            -0.23362802143120032,
            -0.36799502320054384,
            0.35359852278856263,
            0.5830948999779656,
            -0.730683771776052,
            1.4715728371820667,
            -1.0668090648998136,
            -1.025762014881618,
            0.21056106958224155,
            -0.5141254207774576,
            -0.1592942838690149,
            0.7688711617969363,
            -2.464535892598544,
            -0.33306989349452987,
            0.9457207224940593,
            0.36108072442574435,
            -0.6490066877470516,
            -0.8714147266896871,
            0.6567118414749348,
            -0.18543305444915045,
            0.11156511615955596,
            0.7299392157186994,
            -0.9902398239693843,
            -1.3231344439063761,
            -1.1402773433114928,
            0.3696183719476138,
            -1.0512718152423168,
            -0.6093518314203102,
            0.0010622538704462257,
            -0.17676306948277776,
            -0.6291120128576891,
            1.6390197341434742,
            -0.8105788162716191,
            -2.0105672384392204,
            -0.7909143328024505,
            -0.10510684692203587,
            -0.013384480496840259,
            0.37683659744804815,
            -0.15123337965442354,
            1.8427651248902048,
            1.0371006855495906,
            0.29198928612503655,
            -1.7455852392709181,
            1.0854545339796853,
            1.8156620972829793,
            1.2399563224061596,
            1.1196530775769857,
            0.4349954478175989,
            0.11093680938321168,
            0.9945934589378227,
            -0.5779739742428905,
            1.0398502505219054,
            -0.09401160691650227,
            0.22793239636661505,
            -1.8664992140331715,
            -0.16104499274010126,
            -0.8497511318264537,
            -0.005035074822415585,
            -1.7956896952184151,
            1.8304783101189757,
            0.19094408763231646,
            1.3353023874309002,
            0.5889134606052353,
            -0.48487660139277866,
            0.4817014755127622,
            1.5981632863770983,
            2.1416849775567943,
            -0.5524061711669017,
            0.3364804821524787,
            -0.8609687548167294,
            0.24548635047971906,
            -0.1281468603588133,
            -0.03871410517044196,
            -0.2678174852638268,
            0.41800607312114096,
            -0.2503930647517959,
            0.8432391494945226,
            -0.5684563173706987,
            -0.6737077809046504,
            2.0559579098493606,
            -0.29098826888414253,
            -0.08572747304559661,
            -0.301857666880195,
            -0.3446199959065524,
            0.7391340848217359,
            -0.3087136212446006,
            0.5245553707204758,
            -3.063281336805349,
            0.47471623010413705,
            0.3733427291759615,
            -0.26216851429591426,
            -0.5433523111756248,
            0.3305385199964823,
            -1.4866150542941634,
            -0.4699911958560942,
            0.7312367186673805,
            -0.22346998944216903,
            -0.4102860865811592,
            -0.3003478250288424,
            -0.3436168605845268,
            0.9456524589400904,
            -0.03710285453384255,
            0.10330609878001526,
            0.6919858329179392,
            0.8673477607085118,
            0.380742577915601,
            0.5785785515837437,
            -0.011421905830097267,
            0.587187810965595,
            -1.172536467775141,
            -0.532086162097372,
            -0.34440413367820183,
            -1.404900386188497,
            -0.1916375229779241,
            1.6910999461291834,
            -0.6070351182769795,
            -0.8371447893868493,
            0.8853944070432224,
            1.4062946075925473,
            -0.4575973141608374,
            1.1458755768004445,
            0.2619874618238163,
            1.7105876844856704,
            -1.3938976454537522,
            -0.11403217166441704,
            -1.0354305240085717,
            -0.4285770475062154,
            0.10326635421187867,
            0.6911853442971228,
            0.6293835213179542,
            -0.819693698713199,
            -0.7378190403744175,
            -1.495947672573938,
            -1.2406693914431872,
            -1.0486341638186725,
            -1.3715759883075953,
            3.585407817418151,
            -0.8007079372574223,
            -1.527336776754733,
            -0.4716571043072485,
            -0.6967311271405545,
            1.0003347462169225,
            -0.30569565002022697,
            0.3646134876772732,
            0.49083033603832493,
            0.07754580794955847,
            -0.13467337850920083,
            0.02134473458605164,
            0.5025183900540823,
            -0.940929087894874,
            1.441600637127558,
            -0.0857298131221344,
            -0.575175243519591,
            0.42622029657630595,
            -0.3239674701415489,
            0.22648849821602596,
            -0.6636465305318631,
            0.30415000329164754,
            -0.6170241274574016,
            0.07578674772163065,
            0.2952841441615124,
            0.8120317689468056,
            -0.46861353019671337,
            0.04718559572470416,
            -0.3105660017232523,
            -0.28898463203535724,
            0.9575298065734561,
            -0.1977556031830993,
            0.009658232624257272,
            1.1432743259603295,
            -1.8989396918936858,
            0.20787070770386357,
            1.4256750543782999,
            -0.03838329973778874,
            -0.9051229357470373,
            -1.2002277085489457,
            2.405569956130733,
            1.895817948326675,
            -0.8260858325924574,
            0.5759061866255807,
            2.7022875569683342,
            1.0591327405967745,
            0.21449833798124354,
            0.19970388388081273,
            0.018242139911433558,
            -0.630960146999549,
            -2.389646042147776,
            0.5424304992480339,
            -1.2159551561948718,
            -1.6851632640204128,
            -0.4812221268109694,
            0.6217652794219579,
            -0.380139431677482,
            -0.2643524783321051,
            0.5106648694993016,
            -0.895602157034141,
            -0.20559568725141816,
            1.5449271875734911,
            1.544075783565114,
            0.17877619857826843,
            1.9729717339967108,
            0.8302033109816261,
            -0.39118561199170965,
            -0.4428357598297098,
            -0.02550407946753186,
            -1.0202977138210447,
            2.6604654314300835,
            1.9163029269361842,
            0.34697436596877657,
            -0.8078124769022497,
            -1.3876596649099957,
            0.44707250163663864,
            -0.6752837232272447,
            -0.851291770954755,
            0.7599767868730256,
            0.8134109401706875,
            -1.6766750539980289,
            -0.06051832829232975,
            -0.4652931327216134,
            -0.9249124398287735,
            1.9022739762222731,
            1.7632300613807597,
            1.675335012283785,
            0.47529854476887495,
            -0.7892463423254658,
            0.3910120652706098,
            0.5812432547936405,
            0.2693084649672777,
            -0.08138564925779349,
            0.9150619269526952,
            -0.8637356349272142,
            -0.14137853834901817,
            -0.20192754829896423,
            0.04718228147088756,
            -0.9743600144318,
            -0.9936290943927825,
            0.3544612180477054,
            0.6839546770735121,
            1.5089070357620178,
            1.301167565172228,
            -1.5396145667672985,
            0.42854366341485456,
            -1.5876582617301032,
            -0.0316985879141714,
            0.3144220016570915,
            -0.05054766725644431,
            0.2934139006870167,
            0.11396170275994542,
            -0.6472140129693643,
            1.6556030742445431,
            1.0319410208453506,
            0.3292217603989991,
            -0.058758121958605435,
            -0.19917171648476298,
            -0.5192866115874029,
            0.1997510689920335,
            -1.3675686656161756,
            -1.7761517497832053,
            -0.11260276070167097,
            0.9717892642758689,
            0.0840815981843948,
            -0.40211265381258554,
            0.27384496844034517,
            -1.0403875081272367,
            1.2884781173493884,
            -1.8066239592554476,
            1.1136979156298865,
            -0.06223155785690416,
            1.3930381289015936,
            0.4586305673655182,
            1.3159249757827194,
            -0.5369892835955705,
            0.17827408233621184,
            0.22693934439969682,
            0.8216240002114816,
            -1.0422409752281838,
            0.3329686606709231,
            -1.5128804353968217,
            1.0323052869815534,
            1.1640486934424354,
            1.6450118078345612,
            -0.6717687395070293,
            -0.08135119186406627,
            1.2746921873544188,
            -0.8255794145095643,
            0.7123504776564864,
            0.6953336934741682,
            2.191382322698439,
            1.4155790749261592,
            2.4681081786912866,
            -2.2904357033803815,
            -0.8375155191566624,
            1.1040106662196736,
            0.7084133268872015,
            -3.401968681942055,
            0.23237090512844757,
            1.1199436238058174,
            0.6333916486592628,
            -0.6012340913121055,
            -0.3693951838866523,
            -1.7742670566875682,
            -0.36431378282545124,
            -0.4042586409194551,
            -0.04648644034604476,
            1.5138191613743486,
            -0.2053670782251071,
            1.8679122383251414,
            0.8355881018692999,
            -0.5369705129279005,
            -0.7909355080370954,
            2.1080036780007987,
            0.019537331188020687,
            -1.4672982688640615,
            -1.486842866467901,
            -1.1036839537574874,
            1.0800858540685894,
            -0.2313974176207594,
            0.47763272078271807,
            -1.9196070490691473,
            -0.8193535127855751,
            -0.6853651905832031,
            -0.18272370464882973,
            -0.33413577684633056,
            2.2261342671906106,
            1.6853726343573683,
            0.8563421109235769,
            1.0468799885096596,
            0.12189082561416206,
            -1.3596466927672854,
            -0.7607432068282968,
            0.7061728288620306,
            -0.4384478018639071,
            0.8620104661898899,
            1.04258758121448,
            -1.1464159128515612,
            0.9617945424413628,
            0.04987102831355013,
            -0.8472878887606543,
            0.32986774370339184,
            1.278319839581162,
            -0.4040926804592034,
            -0.6691567800662129,
            0.9415431940597389,
            0.3974846022291844,
            -0.8425204662387112,
            -1.506166868030291,
            -0.04248497940038203,
            0.26434168799067986,
            -1.5698380163561454,
            -0.6651727917714935,
            1.2400220571204048,
            -0.1251830593977037,
            0.6156254221302833,
            0.43585628657139575,
            -1.6014619037611209,
            1.9152323656075512,
            -0.8847911114213622,
            1.359854519784993,
            -0.5554989575409871,
            0.25064804193232354,
            0.7976616257678464,
            0.37834567410982123,
            -0.6300374359617635,
            -1.0613465068052854,
            -0.866474302027355,
            1.2458556977164312,
            0.577814049080149,
            2.069400463823993,
            0.9068690176961165,
            -0.5031387968484738,
            -0.3640749863516844,
            -1.041502465417534,
            0.6732994659644133,
            -0.006355018868252906,
            -0.3650517541386253,
            1.0975063446734974,
            -2.203726812834859,
            1.060685913143899,
            -0.4618706570892267,
            0.06475263817517128,
            -0.19326357638969882,
            -0.01812119454736379,
            0.1337618009668529,
            1.1838276997792907,
            0.4273677345455913,
            -0.4912341608307858,
            0.2349993979417651,
            0.9566260826411601,
            -0.7948243131958422,
            -0.6168334352331588,
            0.3369425926447926,
            0.8547756445246633,
            0.2666330662219728,
            2.431868771129661,
            1.0089732701876513,
            -0.1162341515974066,
            -1.1746306816795218,
            -0.08227639025627424,
            0.794676385688044,
            0.15005011094018297,
            -0.8763821573601055,
            -1.0811684990769739,
            0.6311588092267179,
            0.026124278982220386,
            0.8306502001533514,
            1.0856487813261877,
            -0.018702855899823106,
            -0.07338137135247896,
            -0.8435746484744243,
            -0.18091216366556986,
            0.2295807891528797,
            -1.0689295774443397,
            -1.5621175533013612,
            1.3314045672598216,
            0.6211561903553582,
            1.0479302317100871,
            -1.1509436982013124,
            0.447985084931758,
            0.19917261474342404,
            0.3582887259341301,
            0.9953552868908098,
            0.8948165434511316,
            0.4949033431999123,
            -0.23004847985703908,
            0.6411581535557106,
            -1.1589671573242186,
            -0.13691519182560624,
            -0.8849560872785238,
            0.6629182075027006,
            2.2608150731789696,
            2.2823614453180294,
            -1.2291376923498247,
            -0.9267975556981378,
            0.2597417839242135,
            -0.7667310491821938,
            0.10503294084132372,
            2.960320355577672,
            -1.0645098483081497,
            -1.2888339889815872,
            -0.6564570556444346,
            0.4742489396354781,
            0.8879606773334898,
            -0.6477585196839569,
            -0.7309497810668936,
            1.7025953934976548,
            0.1789174966941155,
            -0.4839093362740933,
            -0.8917713440107442,
            1.4521776747175792,
            -0.1676974219641624,
            -0.500672037099228,
            -0.2947747621553442,
            0.929636971325952,
            -0.7614935150071248,
            1.6886298813725842,
            -0.8136217834373227,
            1.2030997228178093,
            1.382267485738376,
            2.594387458306705,
            -0.7703668776292266,
            -0.7642584795112598,
            1.3356598324609947,
            -0.5745269784148925,
            -2.212092904499444,
            -1.727975556661197,
            -0.18543087256023608,
            -0.10167435635752538,
            1.3480966068787303,
            0.0142803272337873,
            -0.480077631815393,
            -0.32270216749876185,
            -1.7884435311074431,
            -0.5695640948971382,
            -0.22859087912027687,
            -0.08783386938029487,
            -0.18151955278624396,
            0.2031493507095467,
            0.06444304447669409,
            -0.4339138073294572,
            0.236563959074551,
            -0.2937958719187449,
            0.1611232843821199,
            -0.6574871644742827,
            1.3141902865107886,
            0.6093649138398077,
            0.056674985715912514,
            -1.828714441504608,
            -0.46768482587669535,
            0.6489735384886999,
            0.5035677725398181,
            -0.887590772676158,
            -0.3222316759913631,
            -0.35172770495027483,
            -0.4329205472963193,
            -0.8449916868048998,
            0.38282765028957993,
            1.3171924061732359,
            0.2956667124648384,
            0.5390909497681301,
            -0.7591989862253667,
            -1.1520792974885883,
            -0.39344757869384944,
            0.6192677330177175,
            -0.05578834574542242,
            0.593015990282657,
            0.9374465229256678,
            0.647772562443425,
            1.1071167572595217,
            -1.3015016617832518,
            1.267300472456379,
            -0.5807673178649629,
            0.9343468385348384,
            -0.28554893036513673,
            0.4487573993840033,
            0.6749018890520516,
            -1.20482985206765,
            0.17291806504654686,
            -0.4124576407610529,
            -0.9203236505429044,
            -0.7461342369802754,
            -0.19694162321688435,
            0.46556512963300906,
            0.5198366004764268,
            -1.7222561645076129,
            -0.7078891617994071,
            -1.1653209054214695,
            1.5560964971092122,
            0.3335520152642012,
            0.008390825910327906,
            0.11336719644324977,
            0.3158913817073965,
            0.4704483453862008,
            -0.5700583482495889,
            -1.276634964816531,
            -1.7880560933777756,
            -0.26514994709973827,
            0.6194447367446946,
            -0.654762456435761,
            1.0621929196158544,
            0.4454719444987052,
            -0.9323145612076791,
            1.3197357985874438,
            -0.8792938558447049,
            -0.2470423905508279,
            0.5128954444799875,
            -0.09202044992462606,
            -1.3082892596744382,
            -0.34428948138804927,
            0.012422196356164879,
            1.4626152292162142,
            0.34678216997159833,
            0.409462409138861,
            0.32838364873801185,
            1.8776849459782967,
            1.6816627852133539,
            -0.24894138693568296,
            0.7150105850753732,
            0.22929306929129853,
            -0.21434910504054566,
            1.3339497173912471,
            -1.2497042452057836,
            -0.04487255356399775,
            -0.6486304639082145,
            -0.8048044333264733,
            -1.8090170501469942,
            1.481689285694336,
            -1.4772553200884717,
            -0.36792462539303805,
            -1.103508260812736,
            -0.2135236993720317,
            0.40889179796540165,
            1.993585196733386,
            0.43879096427562897,
            -0.44512875171982147,
            -1.1780830020629518,
            -1.666001035275436,
            -0.2977294957665528,
            1.7299614542270356,
            0.9882265798853356,
            2.2412430815464597,
            0.5801434875813244,
            -0.739190619909163,
            -1.2663490594895201,
            0.5735521649879137,
            1.2105709455012765,
            1.9112159951415644,
            -2.259218931706201,
            -0.563310876529377,
            -2.4119185903750493,
            0.9662624485722368,
            -0.22788851242764951,
            0.9198283887420099,
            0.7855927065251492,
            -0.7459868094792474,
            0.10543289218409971,
            0.6401750224618271,
            -0.0077375118689326705,
            -0.11647036625911977,
            -0.4722391874001602,
            -0.2718425102733572,
            -0.8796746964457087,
            0.6112903638894259,
            0.5347851929096421,
            -0.4749419210717794,
            1.0633720764557604,
            -0.2590556665572949,
            2.590182301241823,
            1.4524061372706638,
            -0.8503733047335056,
            0.5609357391481067,
            -1.5661825434426477,
            0.8019667474525984,
            1.2716795425969496,
            0.20011166646917924,
            -0.7105405282282679,
            -0.5593129072748189,
            -1.2401371010520867,
            -0.7002520937780202,
            -2.236596391787529,
            -1.8130090502823886,
            -0.23990633860801777,
            1.7428780878151378,
            1.4661206538178901,
            -0.8678567353744017,
            0.2957423562639015,
            0.13935419069962593,
            1.399598845123674,
            0.059729544605779575,
            -0.9607778026198247,
            0.18474907798482051,
            1.0117193651915666,
            -0.9173540069396245,
            0.8934765521365161,
            -0.665655291396948,
            -0.32955768273493324,
            0.3062873812209283,
            0.177342106982554,
            0.3595522704599547,
            -1.5964209653110262,
            0.6705899137346863,
            -1.1034642863469553,
            -1.0029562484065524,
            0.10622956543479244,
            0.4261871936541378,
            0.7777501694354336,
            -0.806235923997437,
            -0.8272801398172428,
            -1.2783440745845536,
            0.5982979227669168,
            -0.28214494859284556,
            1.101560367699546,
            -0.14008021262664466,
            -0.38717961692054237,
            0.9962925044431369,
            -0.7391490127960976,
            -0.06294945881724459,
            0.7283671247384875,
            -0.8458895297768138,
            0.22808829204347086,
            0.43685668023014523,
            0.9204095286935638,
            -0.028241645704951284,
            0.15951784765135396,
            0.8068984900818966,
            -0.34387965576978663,
            0.573828962760762,
            -0.13374515460012618,
            -0.5552788325377814,
            0.5644705833909952,
            -0.7500532220469983,
            0.33436674493862256,
            -0.8595435026628129,
            -0.38943898244735853,
            0.6401502590131951,
            -1.2968645995363652,
            0.5861622311675501,
            0.2311759458689689,
            0.10962292708600496,
            -0.26025023584932205,
            -0.5398478003611565,
            -1.0514168636922954,
            1.2689172189127857,
            1.7029909647408918,
            -0.02325431623491577,
            -0.3064675950620902,
            -1.5816446841009473,
            0.6874254059433739,
            0.7755967316475798,
            1.4119333324396597,
            0.14198739135512406,
            0.2927714469848192,
            -0.7239793888399496,
            0.3506448783535265,
            -0.7568480706640158,
            -1.2158508387501554,
            0.22197589131086445,
            -0.5621415304506887,
            -1.2381112050191665,
            -1.917208333033256,
            -0.3321665793941188,
            -0.5916951886991071,
            -1.244826507645294,
            -0.29767661008214463,
            0.8590635852032509,
            -1.8579290298421591,
            -1.0470546224962876,
            -2.540080936704841,
            0.5458326769958273,
            0.042222128206941614,
            0.6080450228346708,
            0.6542717901662132,
            -1.7292955132690793,
            -0.4793123354077725,
            0.7341767020417185,
            -1.3322222208234826,
            -0.5076389542432337,
            0.684399163420284,
            0.3948487980667425,
            -1.7919279627150193,
            1.582925890933478,
            0.8341846456063038,
            0.11776890377042544,
            1.7471239793853526,
            1.2269451783893597,
            0.4235463733287474,
            1.5908284320029056,
            -1.635191535538596,
            0.04419903330064594,
            -1.264385360373252,
            0.5370192519783876,
            1.2368603501240771,
            -0.9241079150337286,
            -0.3428051342915208,
            0.0882286441353256,
            -2.210824604513402,
            -1.9000343283757128,
            0.4633735273417207,
            -0.32534396967175094,
            0.026187836765356437,
            0.18253601230609245,
            0.8519745761039671,
            -0.028225375482784816,
            -0.5114197447067229,
            -1.2428743809444227,
            0.2879711400745508,
            1.2857130031108321,
            0.5296743558975853,
            -0.8440551904275335,
            -1.3776032491368861,
            1.8164028526343798,
            -1.1422045767986222,
            -1.8675179752970443,
            0.6969635320800454,
            0.9444010906414336,
            -1.28197913481747,
            -0.06259132322304235,
            -0.4518754825442558,
            0.9183188639099813,
            -0.2916931407869574,
            -1.1464007469977915,
            -0.4475136941593681,
            0.44385573868752803,
            2.1606711638680762,
            -1.4813603018181851,
            -0.5647618024870872,
            -1.474746204557383,
            -2.9067748098220485,
            0.06132111635940877,
            -0.09663310829361334,
            -1.087053744976143,
            -1.774855117659402,
            0.8130120568830074,
            -0.5179279676199186,
            -0.32549430825787784,
            -1.1995838271705979,
            0.8587480835176114,
            -0.02095126282663596,
            0.6677898019388228,
            -1.1891003375304232,
            -2.1125937754631305,
            -0.047765192715672734,
            0.09812525010300294,
            -1.034992359189106,
            1.0213451864081846,
            1.0788796513160641,
            -1.444469239557739,
            0.28341828947950637,
            -2.4556013891966737,
            1.7126080715698266,
            -0.5943068899412715,
            1.0897594994215383,
            -0.16345461884651272,
            0.7027032523865234,
            2.2851158088542562,
            0.5038100496225458,
            -0.16724173993999966,
            -0.6747457076421414,
            0.42254684460738184,
            1.277203836895222,
            -0.34438446183574595,
            0.38956738377878264,
            -0.26884968654334923,
            -0.02148772950361766,
            0.02044885235644607,
            -1.3873669828232345,
            0.19995968746809226,
            -1.5826859815811556,
            -0.20385119370067947,
            0.5724329589281247,
            -1.330307658319185,
            0.7756101314358208,
            -0.4989071461473931,
            0.5388161769427321,
            -0.9811085284266614,
            2.335331094403556,
            -0.5588657325211347,
            -1.2850853695283377,
            0.40092993245913744,
            -1.9675685522110529,
            0.9378938542456674,
            -0.18645815013912917,
            -0.6828273180353106,
            -1.840122530632185,
            -1.2581798109361761,
            0.2867275394896832,
        ],
    }
    return data


@pytest.fixture
def numeric_high_card_dataset(test_backend, numeric_high_card_dict):
    schemas = {
        "pandas": {
            "norm_0_1": "float64",
        },
        "postgresql": {
            # "norm_0_1": "DOUBLE_PRECISION",
            "norm_0_1": "NUMERIC",
        },
        "sqlite": {
            "norm_0_1": "FLOAT",
        },
        "mysql": {
            "norm_0_1": "DOUBLE",
        },
        "mssql": {
            "norm_0_1": "FLOAT",
        },
        "spark": {
            "norm_0_1": "FloatType",
        },
    }
    return get_dataset(test_backend, numeric_high_card_dict, schemas=schemas)


@pytest.fixture
def datetime_dataset(test_backend):
    data = {
        "datetime": [
            str(datetime.datetime(2020, 2, 4, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 5, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 6, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 7, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 8, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 9, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 10, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 11, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 12, 22, 12, 5, 943152)),
            str(datetime.datetime(2020, 2, 13, 22, 12, 5, 943152)),
        ]
    }

    schemas = {
        "pandas": {
            "datetime": "datetime64",
        },
        "postgresql": {
            "datetime": "TIMESTAMP",
        },
        "sqlite": {
            "datetime": "TIMESTAMP",
        },
        "mysql": {
            "datetime": "TIMESTAMP",
        },
        "mssql": {
            "datetime": "DATETIME",
        },
        "spark": {
            "datetime": "TimestampType",
        },
    }
    return get_dataset(test_backend, data, schemas=schemas)


@pytest.fixture
def non_numeric_low_card_dataset(test_backend):
    """Provide dataset fixtures that have special values and/or are otherwise useful outside
    the standard json testing framework"""

    data = {
        "lowcardnonnum": [
            "a",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
            "b",
        ]
    }
    schemas = {
        "pandas": {
            "lowcardnonnum": "str",
        },
        "postgresql": {
            "lowcardnonnum": "TEXT",
        },
        "sqlite": {
            "lowcardnonnum": "VARCHAR",
        },
        "mysql": {
            "lowcardnonnum": "TEXT",
        },
        "mssql": {
            "lowcardnonnum": "VARCHAR",
        },
        "spark": {
            "lowcardnonnum": "StringType",
        },
    }
    return get_dataset(test_backend, data, schemas=schemas)


@pytest.fixture
def non_numeric_high_card_dataset(test_backend):
    """Provide dataset fixtures that have special values and/or are otherwise useful outside
    the standard json testing framework"""

    data = {
        "highcardnonnum": [
            "CZVYSnQhHhoti8mQ66XbDuIjE5FMeIHb",
            "cPWAg2MJjh8fkRRU1B9aD8vWq3P8KTxJ",
            "4tehKwWiCDpuOmTPRYYqTqM7TvEa8Zi7",
            "ZvlAnCGiGfkKgQoNrhnnyrjmU7sLsUZz",
            "AaqMhdYukVdexTk6LlWvzXYXTp5upPuf",
            "ZSKmXUB35K14khHGyjYtuCHuI8yeM7yR",
            "F1cwKp4HsCN2s2kXQGR5RUa3WAcibCq2",
            "coaX8bSHoVZ8FP8SuQ57SFbrvpRHcibq",
            "3IzmbSJF525qtn7O4AvfKONnz7eFgnyU",
            "gLCtw7435gaR532PNFVCtvk14lNJpZXv",
            "hNyjMYZkVlOKRjhg8cKymU5Bvnh0MK5R",
            "IqKC2auGTNehP8y24HzDQOdt9oysgFyx",
            "TePy034aBKlNeAmcJmKJ4p1yF7EUYEOg",
            "cIfDv6ieTAobe84P84InzDKrJrccmqbq",
            "m1979gfI6lVF9ijJA245bchYFd1EaMap",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "7wcR161jyKYhFLEZkhFqSXLwXW46I5x8",
            "IpmNsUFgbbVnL0ljJZOBHnTV0FKARwSn",
            "hsA4btHJg6Gq1jwOuOc3pl2UPB5QUwZg",
            "vwZyG0jGUys3HQdUiOocIbzhUdUugwKX",
            "rTc9h94WjOXN5Wg40DyatFEFfp9mgWj6",
            "p1f20s14ZJGUTIBUNeBmJEkWKlwoyqjA",
            "VzgAIYNKHA0APN0oZtzMAfmbCzJenswy",
            "IO7BqR3iS136YMlLCEo6W3jKNOVJIlLG",
            "eTEyhiRuyEcTnHThi1W6yi1mxUjq8TEp",
            "4OHPKQgk3sPPYpKWcEWUtNZ0jv00UuPU",
            "ZJCstyyUvTR2gwSM6FLgkXYDwG54qo8u",
            "nGQsvDAzuL5Yc2XpqoG5P7RhpiTpJp8H",
            "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
            "CP22IFHDX1maoSjTEdtBfrMHWQKACGDB",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hGwZQW7ao9HqNV2xAovuMBdyafNDE8q6",
            "OJmDHbqP1wzarsaSwCphsqvdy5SnTQMT",
            "JQbXIcgwUhttfPIGB7VGGfL2KiElabrO",
            "eTTNDggfPpRC22SEVNo9W0BPEWO4Cr57",
            "GW2JuUJmuCebia7RUiCNl2BTjukIzZWj",
            "oVFAvQEKmRTLBqdCuPoJNvzPvQ7UArWC",
            "zeMHFFKLr5j4DIFxRQ7jHWCMClrP3LmJ",
            "eECcArV5TZRftL6ZWaUDO6D2l3HiZj1Y",
            "xLNJXaCkOLrD6E0kgGaFOFwctNXjrd77",
            "1f8KOCkOvehXYvN8PKv1Ch6dzOjRAr01",
            "uVF6HJgjVmoipK1sEpVOFJYuv2TXXsOG",
            "agIk8H2nFa0K27IFr0VM2RNp6saihYI3",
            "cAUnysbb8SBLSTr0H7cA1fmnpaL80e0N",
            "fM1IzD5USx4lMYi6bqPCEZjd2aP7G9vv",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "i65d8jqET5FsVw9t5BwAvBjkEJI6eUMj",
            "HbT1b7DQL7n7ZEt2FsKHIggycT1XIYd8",
            "938eC0iGMSqZNlqgDNG9YVE7t4izO2Ev",
            "PyZetp4izgE4ymPcUXyImF5mm7I6zbta",
            "FaXA6YSUrvSnW7quAimLqQMNrU1Dxyjs",
            "PisVMvI9RsqQw21B7qYcKkRo5c8C2AKd",
            "eSQIxFqyYVf55UMzMEZrotPO74i3Sh03",
            "2b74DhJ6YFHrAkrjK4tvvKkYUKll44bR",
            "3svDRnrELyAsC69Phpnl2Os89856tFBJ",
            "ZcSGN9YYNHnHjUp0SktWoZI7JDmvRTTN",
            "m9eDkZ5oZEOFP3HUfaZEirecv2UhQ1B1",
            "wZTwJmMX5Q58DhDdmScdigTSyUUC04sO",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "KAuFgcmRKQPIIqGMAQQPfjyC1VXt40vs",
            "0S4iueoqKNjvS55O57BdY3DbfwhIDwKc",
            "ywbQfOLkvXEUzZISZp1cpwCenrrNPjfF",
            "Mayxk8JkV3Z6aROtnsKyqwVK5exiJa8i",
            "pXqIRP5fQzbDtj1xFqgJey6dyFOJ1YiU",
            "6Ba6RSM56x4MIaJ2wChQ3trBVOw1SWGM",
            "puqzOpRJyNVAwH2vLjVCL3uuggxO5aoB",
            "jOI4E43wA3lYBWbV0nMxqix885Tye1Pf",
            "YgTTYpRDrxU1dMKZeVHYzYNovH2mWGB7",
            "24yYfUg1ATvfI1PW79ytsEqHWJHI69wQ",
            "mS2AVcLFp6i36sX7yAUrdfM0g0RB2X4D",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "ItvI4l02oAIZEd5cPtDf4OnyBazji0PL",
            "DW4oLNP49MNNENFoFf7jDTI04xdvCiWg",
            "vrOZrkAS9MCGOqzhCv4cmr5AGddVBShU",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "R74JT4EEhh3Xeu5tbx8bZFkXZRhx6HUn",
            "bd9yxS6b1QrKXuT4irY4kpjSyLmKZmx6",
            "UMdFQNSiJZtLK3jxBETZrINDKcRqRd0c",
            "He7xIY2BMNZ7vSO47KfKoYskVJeeedI7",
            "G8PqO0ADoKfDPsMT1K0uOrYf1AtwlTSR",
            "hqfmEBNCA7qgntcQVqB7beBt0hB7eaxF",
            "mlYdlfei13P6JrT7ZbSZdsudhE24aPYr",
            "gUTUoH9LycaItbwLZkK9qf0xbRDgOMN4",
            "xw3AuIPyHYq59Qbo5QkQnECSqd2UCvLo",
            "kbfzRyRqGZ9WvmTdYKDjyds6EK4fYCyx",
            "7AOZ3o2egl6aU1zOrS8CVwXYZMI8NTPg",
            "Wkh43H7t95kRb9oOMjTSqC7163SrI4rU",
            "x586wCHsLsOaXl3F9cYeaROwdFc2pbU1",
            "oOd7GdoPn4qqfAeFj2Z3ddyFdmkuPznh",
            "suns0vGgaMzasYpwDEEof2Ktovy0o4os",
            "of6W1csCTCBMBXli4a6cEmGZ9EFIOFRC",
            "mmTiWVje9SotwPgmRxrGrNeI9DssAaCj",
            "pIX0vhOzql5c6Z6NpLbzc8MvYiONyT54",
            "nvyCo3MkIK4tS6rkuL4Yw1RgGKwhm4c2",
            "prQGAOvQbB8fQIrp8xaLXmGwcxDcCnqt",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "mty9rQJBeTsBQ7ra8vWRbBaWulzhWRSG",
            "JL38Vw7yERPC4gBplBaixlbpDg8V7gC6",
            "MylTvGl5L1tzosEcgGCQPjIRN6bCUwtI",
            "hmr0LNyYObqe5sURs408IhRb50Lnek5K",
            "CZVYSnQhHhoti8mQ66XbDuIjE5FMeIHb",
            "cPWAg2MJjh8fkRRU1B9aD8vWq3P8KTxJ",
            "4tehKwWiCDpuOmTPRYYqTqM7TvEa8Zi7",
            "ZvlAnCGiGfkKgQoNrhnnyrjmU7sLsUZz",
            "AaqMhdYukVdexTk6LlWvzXYXTp5upPuf",
            "ZSKmXUB35K14khHGyjYtuCHuI8yeM7yR",
            "F1cwKp4HsCN2s2kXQGR5RUa3WAcibCq2",
            "coaX8bSHoVZ8FP8SuQ57SFbrvpRHcibq",
            "3IzmbSJF525qtn7O4AvfKONnz7eFgnyU",
            "gLCtw7435gaR532PNFVCtvk14lNJpZXv",
            "hNyjMYZkVlOKRjhg8cKymU5Bvnh0MK5R",
            "IqKC2auGTNehP8y24HzDQOdt9oysgFyx",
            "TePy034aBKlNeAmcJmKJ4p1yF7EUYEOg",
            "cIfDv6ieTAobe84P84InzDKrJrccmqbq",
            "m1979gfI6lVF9ijJA245bchYFd1EaMap",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "7wcR161jyKYhFLEZkhFqSXLwXW46I5x8",
            "IpmNsUFgbbVnL0ljJZOBHnTV0FKARwSn",
            "hsA4btHJg6Gq1jwOuOc3pl2UPB5QUwZg",
            "vwZyG0jGUys3HQdUiOocIbzhUdUugwKX",
            "rTc9h94WjOXN5Wg40DyatFEFfp9mgWj6",
            "p1f20s14ZJGUTIBUNeBmJEkWKlwoyqjA",
            "VzgAIYNKHA0APN0oZtzMAfmbCzJenswy",
            "IO7BqR3iS136YMlLCEo6W3jKNOVJIlLG",
            "eTEyhiRuyEcTnHThi1W6yi1mxUjq8TEp",
            "4OHPKQgk3sPPYpKWcEWUtNZ0jv00UuPU",
            "ZJCstyyUvTR2gwSM6FLgkXYDwG54qo8u",
            "nGQsvDAzuL5Yc2XpqoG5P7RhpiTpJp8H",
            "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
            "CP22IFHDX1maoSjTEdtBfrMHWQKACGDB",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hGwZQW7ao9HqNV2xAovuMBdyafNDE8q6",
            "OJmDHbqP1wzarsaSwCphsqvdy5SnTQMT",
            "JQbXIcgwUhttfPIGB7VGGfL2KiElabrO",
            "eTTNDggfPpRC22SEVNo9W0BPEWO4Cr57",
            "GW2JuUJmuCebia7RUiCNl2BTjukIzZWj",
            "oVFAvQEKmRTLBqdCuPoJNvzPvQ7UArWC",
            "zeMHFFKLr5j4DIFxRQ7jHWCMClrP3LmJ",
            "eECcArV5TZRftL6ZWaUDO6D2l3HiZj1Y",
            "xLNJXaCkOLrD6E0kgGaFOFwctNXjrd77",
            "1f8KOCkOvehXYvN8PKv1Ch6dzOjRAr01",
            "uVF6HJgjVmoipK1sEpVOFJYuv2TXXsOG",
            "agIk8H2nFa0K27IFr0VM2RNp6saihYI3",
            "cAUnysbb8SBLSTr0H7cA1fmnpaL80e0N",
            "fM1IzD5USx4lMYi6bqPCEZjd2aP7G9vv",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "i65d8jqET5FsVw9t5BwAvBjkEJI6eUMj",
            "HbT1b7DQL7n7ZEt2FsKHIggycT1XIYd8",
            "938eC0iGMSqZNlqgDNG9YVE7t4izO2Ev",
            "PyZetp4izgE4ymPcUXyImF5mm7I6zbta",
            "FaXA6YSUrvSnW7quAimLqQMNrU1Dxyjs",
            "PisVMvI9RsqQw21B7qYcKkRo5c8C2AKd",
            "eSQIxFqyYVf55UMzMEZrotPO74i3Sh03",
            "2b74DhJ6YFHrAkrjK4tvvKkYUKll44bR",
            "3svDRnrELyAsC69Phpnl2Os89856tFBJ",
            "ZcSGN9YYNHnHjUp0SktWoZI7JDmvRTTN",
            "m9eDkZ5oZEOFP3HUfaZEirecv2UhQ1B1",
            "wZTwJmMX5Q58DhDdmScdigTSyUUC04sO",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "KAuFgcmRKQPIIqGMAQQPfjyC1VXt40vs",
            "0S4iueoqKNjvS55O57BdY3DbfwhIDwKc",
            "ywbQfOLkvXEUzZISZp1cpwCenrrNPjfF",
            "Mayxk8JkV3Z6aROtnsKyqwVK5exiJa8i",
            "pXqIRP5fQzbDtj1xFqgJey6dyFOJ1YiU",
            "6Ba6RSM56x4MIaJ2wChQ3trBVOw1SWGM",
            "puqzOpRJyNVAwH2vLjVCL3uuggxO5aoB",
            "jOI4E43wA3lYBWbV0nMxqix885Tye1Pf",
            "YgTTYpRDrxU1dMKZeVHYzYNovH2mWGB7",
            "24yYfUg1ATvfI1PW79ytsEqHWJHI69wQ",
            "mS2AVcLFp6i36sX7yAUrdfM0g0RB2X4D",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "ItvI4l02oAIZEd5cPtDf4OnyBazji0PL",
            "DW4oLNP49MNNENFoFf7jDTI04xdvCiWg",
            "vrOZrkAS9MCGOqzhCv4cmr5AGddVBShU",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "R74JT4EEhh3Xeu5tbx8bZFkXZRhx6HUn",
            "bd9yxS6b1QrKXuT4irY4kpjSyLmKZmx6",
            "UMdFQNSiJZtLK3jxBETZrINDKcRqRd0c",
            "He7xIY2BMNZ7vSO47KfKoYskVJeeedI7",
            "G8PqO0ADoKfDPsMT1K0uOrYf1AtwlTSR",
            "hqfmEBNCA7qgntcQVqB7beBt0hB7eaxF",
            "mlYdlfei13P6JrT7ZbSZdsudhE24aPYr",
            "gUTUoH9LycaItbwLZkK9qf0xbRDgOMN4",
            "xw3AuIPyHYq59Qbo5QkQnECSqd2UCvLo",
            "kbfzRyRqGZ9WvmTdYKDjyds6EK4fYCyx",
            "7AOZ3o2egl6aU1zOrS8CVwXYZMI8NTPg",
            "Wkh43H7t95kRb9oOMjTSqC7163SrI4rU",
            "x586wCHsLsOaXl3F9cYeaROwdFc2pbU1",
            "oOd7GdoPn4qqfAeFj2Z3ddyFdmkuPznh",
            "suns0vGgaMzasYpwDEEof2Ktovy0o4os",
            "of6W1csCTCBMBXli4a6cEmGZ9EFIOFRC",
            "mmTiWVje9SotwPgmRxrGrNeI9DssAaCj",
            "pIX0vhOzql5c6Z6NpLbzc8MvYiONyT54",
            "nvyCo3MkIK4tS6rkuL4Yw1RgGKwhm4c2",
            "prQGAOvQbB8fQIrp8xaLXmGwcxDcCnqt",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "mty9rQJBeTsBQ7ra8vWRbBaWulzhWRSG",
            "JL38Vw7yERPC4gBplBaixlbpDg8V7gC6",
            "MylTvGl5L1tzosEcgGCQPjIRN6bCUwtI",
            "hmr0LNyYObqe5sURs408IhRb50Lnek5K",
        ],
        # Built from highcardnonnum using the following:
        # vals = pd.Series(data["highcardnonnum"])
        # sample_vals = vals.sample(n=10, random_state=42)
        # weights = np.random.RandomState(42).rand(10)
        # weights = weights / np.sum(weights)
        # new_vals = sample_vals.sample(n=200, weights=weights, replace=True, random_state=11)
        "medcardnonnum": [
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "mS2AVcLFp6i36sX7yAUrdfM0g0RB2X4D",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
        ],
    }
    schemas = {
        "pandas": {
            "highcardnonnum": "str",
            "medcardnonnum": "str",
        },
        "postgresql": {
            "highcardnonnum": "TEXT",
            "medcardnonnum": "TEXT",
        },
        "sqlite": {
            "highcardnonnum": "VARCHAR",
            "medcardnonnum": "VARCHAR",
        },
        "mysql": {
            "highcardnonnum": "TEXT",
            "medcardnonnum": "TEXT",
        },
        "mssql": {
            "highcardnonnum": "VARCHAR",
            "medcardnonnum": "VARCHAR",
        },
        "spark": {
            "highcardnonnum": "StringType",
            "medcardnonnum": "StringType",
        },
    }
    return get_dataset(test_backend, data, schemas=schemas)


@pytest.fixture
def periodic_table_of_elements():
    data = [
        "Hydrogen",
        "Helium",
        "Lithium",
        "Beryllium",
        "Boron",
        "Carbon",
        "Nitrogen",
        "Oxygen",
        "Fluorine",
        "Neon",
        "Sodium",
        "Magnesium",
        "Aluminum",
        "Silicon",
        "Phosphorus",
        "Sulfur",
        "Chlorine",
        "Argon",
        "Potassium",
        "Calcium",
        "Scandium",
        "Titanium",
        "Vanadium",
        "Chromium",
        "Manganese",
        "Iron",
        "Cobalt",
        "Nickel",
        "Copper",
        "Zinc",
        "Gallium",
        "Germanium",
        "Arsenic",
        "Selenium",
        "Bromine",
        "Krypton",
        "Rubidium",
        "Strontium",
        "Yttrium",
        "Zirconium",
        "Niobium",
        "Molybdenum",
        "Technetium",
        "Ruthenium",
        "Rhodium",
        "Palladium",
        "Silver",
        "Cadmium",
        "Indium",
        "Tin",
        "Antimony",
        "Tellurium",
        "Iodine",
        "Xenon",
        "Cesium",
        "Barium",
        "Lanthanum",
        "Cerium",
        "Praseodymium",
        "Neodymium",
        "Promethium",
        "Samarium",
        "Europium",
        "Gadolinium",
        "Terbium",
        "Dysprosium",
        "Holmium",
        "Erbium",
        "Thulium",
        "Ytterbium",
        "Lutetium",
        "Hafnium",
        "Tantalum",
        "Tungsten",
        "Rhenium",
        "Osmium",
        "Iridium",
        "Platinum",
        "Gold",
        "Mercury",
        "Thallium",
        "Lead",
        "Bismuth",
        "Polonium",
        "Astatine",
        "Radon",
        "Francium",
        "Radium",
        "Actinium",
        "Thorium",
        "Protactinium",
        "Uranium",
        "Neptunium",
        "Plutonium",
        "Americium",
        "Curium",
        "Berkelium",
        "Californium",
        "Einsteinium",
        "Fermium",
        "Mendelevium",
        "Nobelium",
        "Lawrencium",
        "Rutherfordium",
        "Dubnium",
        "Seaborgium",
        "Bohrium",
        "Hassium",
        "Meitnerium",
        "Darmstadtium",
        "Roentgenium",
        "Copernicium",
        "Nihomium",
        "Flerovium",
        "Moscovium",
        "Livermorium",
        "Tennessine",
        "Oganesson",
    ]
    return data


def dataset_sample_data(test_backend):
    # No infinities for mysql
    if test_backend == "mysql":
        data = {
            # "infinities": [-np.inf, -10, -np.pi, 0, np.pi, 10/2.2, np.inf],
            "nulls": [np.nan, None, 0, 1.1, 2.2, 3.3, None],
            "naturals": [1, 2, 3, 4, 5, 6, 7],
        }
    else:
        data = {
            "infinities": [-np.inf, -10, -np.pi, 0, np.pi, 10 / 2.2, np.inf],
            "nulls": [np.nan, None, 0, 1.1, 2.2, 3.3, None],
            "naturals": [1, 2, 3, 4, 5, 6, 7],
        }
    schemas = {
        "pandas": {"infinities": "float64", "nulls": "float64", "naturals": "float64"},
        "postgresql": {
            "infinities": "DOUBLE_PRECISION",
            "nulls": "DOUBLE_PRECISION",
            "naturals": "NUMERIC",
        },
        "sqlite": {"infinities": "FLOAT", "nulls": "FLOAT", "naturals": "FLOAT"},
        "mysql": {"nulls": "DOUBLE", "naturals": "DOUBLE"},
        "mssql": {"infinities": "FLOAT", "nulls": "FLOAT", "naturals": "FLOAT"},
        "spark": {
            "infinities": "FloatType",
            "nulls": "FloatType",
            "naturals": "FloatType",
        },
    }
    return data, schemas


@pytest.fixture
def dataset(test_backend):
    """Provide dataset fixtures that have special values and/or are otherwise useful outside
    the standard json testing framework"""
    data, schemas = dataset_sample_data(test_backend)
    return get_dataset(test_backend, data, schemas=schemas)


@pytest.fixture
def pandas_dataset():
    test_backend = "PandasDataset"
    data, schemas = dataset_sample_data(test_backend)
    return get_dataset(test_backend, data, schemas=schemas)


@pytest.fixture
def sqlalchemy_dataset(test_backends):
    """Provide dataset fixtures that have special values and/or are otherwise useful outside
    the standard json testing framework"""
    if "postgresql" in test_backends:
        backend = "postgresql"
    elif "sqlite" in test_backends:
        backend = "sqlite"
    else:
        return

    data = {
        "infinities": [-np.inf, -10, -np.pi, 0, np.pi, 10 / 2.2, np.inf],
        "nulls": [np.nan, None, 0, 1.1, 2.2, 3.3, None],
        "naturals": [1, 2, 3, 4, 5, 6, 7],
    }
    schemas = {
        "postgresql": {
            "infinities": "DOUBLE_PRECISION",
            "nulls": "DOUBLE_PRECISION",
            "naturals": "DOUBLE_PRECISION",
        },
        "sqlite": {"infinities": "FLOAT", "nulls": "FLOAT", "naturals": "FLOAT"},
    }
    return get_dataset(backend, data, schemas=schemas, profiler=None)


@pytest.fixture
def sqlitedb_engine(test_backend):
    if test_backend == "sqlite":
        try:
            import sqlalchemy as sa

            return sa.create_engine("sqlite://")
        except ImportError:
            raise ValueError("sqlite tests require sqlalchemy to be installed")
    else:
        pytest.skip("Skipping test designed for sqlite on non-sqlite backend.")


@pytest.fixture
def postgresql_engine(test_backend):
    if test_backend == "postgresql":
        try:
            import sqlalchemy as sa

            db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
            engine = sa.create_engine(
                f"postgresql://postgres@{db_hostname}/test_ci"
            ).connect()
            yield engine
            engine.close()
        except ImportError:
            raise ValueError("SQL Database tests require sqlalchemy to be installed.")
    else:
        pytest.skip("Skipping test designed for postgresql on non-postgresql backend.")


@pytest.fixture(scope="function")
def empty_data_context(tmp_path) -> DataContext:
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    context = ge.data_context.DataContext.create(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    os.makedirs(asset_config_path, exist_ok=True)
    assert context.list_datasources() == []
    return context


@pytest.fixture
def titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled(
    tmp_path_factory,
    test_backends,
    monkeypatch,
):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "test_fixtures",
                "great_expectations_v013_no_datasource_stats_enabled.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("test_sets", "Titanic.csv")),
        str(
            os.path.join(
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("test_sets", "Titanic.csv")),
        str(
            os.path.join(context_path, "..", "data", "titanic", "Titanic_19120414_1313")
        ),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )

    context: DataContext = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    datasource_config: str = f"""
        class_name: Datasource

        execution_engine:
            class_name: PandasExecutionEngine

        data_connectors:
            my_basic_data_connector:
                class_name: InferredAssetFilesystemDataConnector
                base_directory: {data_path}
                default_regex:
                    pattern: (.*)\\.csv
                    group_names:
                        - data_asset_name

            my_special_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users:
                        base_directory: {data_path}
                        pattern: (.+)_(\\d+)_(\\d+)\\.csv
                        group_names:
                            - name
                            - timestamp
                            - size

            my_other_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users: {{}}

            my_runtime_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - pipeline_stage_name
                    - airflow_run_id
        """

    # noinspection PyUnusedLocal
    datasource: Datasource = context.test_yaml_config(
        name="my_datasource", yaml_config=datasource_config, pretty_print=False
    )

    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def assetless_dataconnector_context(
    tmp_path_factory,
    monkeypatch,
):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            "./test_fixtures/great_expectations_v013_no_datasource_stats_enabled.yml",
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    context = ge.data_context.DataContext(context_path)
    assert context.root_directory == context_path

    datasource_config = f"""
            class_name: Datasource

            execution_engine:
                class_name: PandasExecutionEngine

            data_connectors:
                my_other_data_connector:
                    class_name: ConfiguredAssetFilesystemDataConnector
                    base_directory: {data_path}
                    glob_directive: "*.csv"

                    default_regex:
                        pattern: (.+)\\.csv
                        group_names:
                            - name
                    assets:
                        {{}}
            """

    context.test_yaml_config(
        name="my_datasource", yaml_config=datasource_config, pretty_print=False
    )
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def deterministic_asset_dataconnector_context(
    tmp_path_factory,
    monkeypatch,
):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            "./test_fixtures/great_expectations_v013_no_datasource_stats_enabled.yml",
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(
            os.path.join(
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )
    context = ge.data_context.DataContext(context_path)
    assert context.root_directory == context_path

    datasource_config = f"""
        class_name: Datasource

        execution_engine:
            class_name: PandasExecutionEngine

        data_connectors:
            my_other_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users: {{}}
        """

    context.test_yaml_config(
        name="my_datasource", yaml_config=datasource_config, pretty_print=False
    )
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    # add simple template config
    simple_checkpoint_template_config: CheckpointConfig = CheckpointConfig(
        name="my_simple_template_checkpoint",
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template-$VAR",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
        },
        runtime_configuration={
            "result_format": {
                "result_format": "BASIC",
                "partial_unexpected_count": 20,
            }
        },
    )
    simple_checkpoint_template_config_key: ConfigurationIdentifier = (
        ConfigurationIdentifier(
            configuration_key=simple_checkpoint_template_config.name
        )
    )
    context.checkpoint_store.set(
        key=simple_checkpoint_template_config_key,
        value=simple_checkpoint_template_config,
    )

    # add nested template configs
    nested_checkpoint_template_config_1: CheckpointConfig = CheckpointConfig(
        name="my_nested_checkpoint_template_1",
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template-$VAR",
        expectation_suite_name="suite_from_template_1",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
        evaluation_parameters={
            "environment": "FOO",
            "tolerance": "FOOBOO",
            "aux_param_0": "FOOBARBOO",
            "aux_param_1": "FOOBARBOO",
            "template_1_key": 456,
        },
        runtime_configuration={
            "result_format": "FOOBARBOO",
            "partial_unexpected_count": "FOOBARBOO",
            "template_1_key": 123,
        },
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource_template_1",
                    "data_connector_name": "my_special_data_connector_template_1",
                    "data_asset_name": "users_from_template_1",
                    "data_connector_query": {"partition_index": -999},
                }
            }
        ],
    )
    nested_checkpoint_template_config_1_key: ConfigurationIdentifier = (
        ConfigurationIdentifier(
            configuration_key=nested_checkpoint_template_config_1.name
        )
    )
    context.checkpoint_store.set(
        key=nested_checkpoint_template_config_1_key,
        value=nested_checkpoint_template_config_1,
    )

    nested_checkpoint_template_config_2: CheckpointConfig = CheckpointConfig(
        name="my_nested_checkpoint_template_2",
        config_version=1,
        template_name="my_nested_checkpoint_template_1",
        run_name_template="%Y-%M-foo-bar-template-$VAR-template-2",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "MyCustomStoreEvaluationParametersActionTemplate2",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
            {
                "name": "new_action_from_template_2",
                "action": {"class_name": "Template2SpecialAction"},
            },
        ],
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
        },
        runtime_configuration={
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
        },
    )
    nested_checkpoint_template_config_2_key: ConfigurationIdentifier = (
        ConfigurationIdentifier(
            configuration_key=nested_checkpoint_template_config_2.name
        )
    )
    context.checkpoint_store.set(
        key=nested_checkpoint_template_config_2_key,
        value=nested_checkpoint_template_config_2,
    )

    nested_checkpoint_template_config_3: CheckpointConfig = CheckpointConfig(
        name="my_nested_checkpoint_template_3",
        config_version=1,
        template_name="my_nested_checkpoint_template_2",
        run_name_template="%Y-%M-foo-bar-template-$VAR-template-3",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "MyCustomStoreEvaluationParametersActionTemplate3",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
            {
                "name": "new_action_from_template_3",
                "action": {"class_name": "Template3SpecialAction"},
            },
        ],
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
            "template_3_key": 123,
        },
        runtime_configuration={
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
            "template_3_key": "bloopy!",
        },
    )
    nested_checkpoint_template_config_3_key: ConfigurationIdentifier = (
        ConfigurationIdentifier(
            configuration_key=nested_checkpoint_template_config_3.name
        )
    )
    context.checkpoint_store.set(
        key=nested_checkpoint_template_config_3_key,
        value=nested_checkpoint_template_config_3,
    )

    # add minimal SimpleCheckpoint
    simple_checkpoint_config: CheckpointConfig = CheckpointConfig(
        name="my_minimal_simple_checkpoint",
        class_name="SimpleCheckpoint",
        config_version=1,
    )
    simple_checkpoint_config_key: ConfigurationIdentifier = ConfigurationIdentifier(
        configuration_key=simple_checkpoint_config.name
    )
    context.checkpoint_store.set(
        key=simple_checkpoint_config_key,
        value=simple_checkpoint_config,
    )

    # add SimpleCheckpoint with slack webhook
    simple_checkpoint_with_slack_webhook_config: CheckpointConfig = CheckpointConfig(
        name="my_simple_checkpoint_with_slack",
        class_name="SimpleCheckpoint",
        config_version=1,
        slack_webhook="https://hooks.slack.com/foo/bar",
    )
    simple_checkpoint_with_slack_webhook_config_key: ConfigurationIdentifier = (
        ConfigurationIdentifier(
            configuration_key=simple_checkpoint_with_slack_webhook_config.name
        )
    )
    context.checkpoint_store.set(
        key=simple_checkpoint_with_slack_webhook_config_key,
        value=simple_checkpoint_with_slack_webhook_config,
    )

    # add SimpleCheckpoint with slack webhook and notify_with
    simple_checkpoint_with_slack_webhook_and_notify_with_all_config: CheckpointConfig = CheckpointConfig(
        name="my_simple_checkpoint_with_slack_and_notify_with_all",
        class_name="SimpleCheckpoint",
        config_version=1,
        slack_webhook="https://hooks.slack.com/foo/bar",
        notify_with="all",
    )
    simple_checkpoint_with_slack_webhook_and_notify_with_all_config_key: ConfigurationIdentifier = ConfigurationIdentifier(
        configuration_key=simple_checkpoint_with_slack_webhook_and_notify_with_all_config.name
    )
    context.checkpoint_store.set(
        key=simple_checkpoint_with_slack_webhook_and_notify_with_all_config_key,
        value=simple_checkpoint_with_slack_webhook_and_notify_with_all_config,
    )

    # add SimpleCheckpoint with site_names
    simple_checkpoint_with_site_names_config: CheckpointConfig = CheckpointConfig(
        name="my_simple_checkpoint_with_site_names",
        class_name="SimpleCheckpoint",
        config_version=1,
        site_names=["local_site"],
    )
    simple_checkpoint_with_site_names_config_key: ConfigurationIdentifier = (
        ConfigurationIdentifier(
            configuration_key=simple_checkpoint_with_site_names_config.name
        )
    )
    context.checkpoint_store.set(
        key=simple_checkpoint_with_site_names_config_key,
        value=simple_checkpoint_with_site_names_config,
    )

    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def empty_data_context_with_config_variables(monkeypatch, empty_data_context):
    monkeypatch.setenv("FOO", "BAR")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", "ive_been_$--replaced")
    root_dir = empty_data_context.root_directory
    ge_config_path = file_relative_path(
        __file__,
        "./test_fixtures/great_expectations_basic_with_variables.yml",
    )
    shutil.copy(ge_config_path, os.path.join(root_dir, "great_expectations.yml"))
    config_variables_path = file_relative_path(
        __file__,
        "./test_fixtures/config_variables.yml",
    )
    shutil.copy(config_variables_path, os.path.join(root_dir, "uncommitted"))
    return DataContext(context_root_dir=root_dir)


@pytest.fixture
def empty_context_with_checkpoint(empty_data_context):
    context = empty_data_context
    root_dir = empty_data_context.root_directory
    fixture_name = "my_checkpoint.yml"
    fixture_path = file_relative_path(
        __file__, f"./data_context/fixtures/contexts/{fixture_name}"
    )
    checkpoints_file = os.path.join(root_dir, "checkpoints", fixture_name)
    shutil.copy(fixture_path, checkpoints_file)
    assert os.path.isfile(checkpoints_file)
    return context


@pytest.fixture
def empty_context_with_checkpoint_stats_enabled(empty_data_context_stats_enabled):
    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    fixture_name = "my_checkpoint.yml"
    fixture_path = file_relative_path(
        __file__, f"./data_context/fixtures/contexts/{fixture_name}"
    )
    checkpoints_file = os.path.join(root_dir, "checkpoints", fixture_name)
    shutil.copy(fixture_path, checkpoints_file)
    return context


@pytest.fixture
def empty_data_context_stats_enabled(tmp_path_factory, monkeypatch):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")
    project_path = str(tmp_path_factory.mktemp("empty_data_context"))
    context = ge.data_context.DataContext.create(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    os.makedirs(asset_config_path, exist_ok=True)
    return context


@pytest.fixture
def empty_context_with_checkpoint_v1_stats_enabled(
    empty_data_context_stats_enabled, monkeypatch
):
    try:
        monkeypatch.delenv("VAR")
        monkeypatch.delenv("MY_PARAM")
        monkeypatch.delenv("OLD_PARAM")
    except:
        pass

    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    fixture_name = "my_v1_checkpoint.yml"
    fixture_path = file_relative_path(
        __file__, f"./data_context/fixtures/contexts/{fixture_name}"
    )
    checkpoints_file = os.path.join(root_dir, "checkpoints", fixture_name)
    shutil.copy(fixture_path, checkpoints_file)
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def titanic_data_context(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_v013_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_no_data_docs_no_checkpoint_store(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_titanic_pre_v013_no_data_docs.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_no_data_docs(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_titanic_no_data_docs.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_stats_enabled_no_config_store(tmp_path_factory, monkeypatch):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_stats_enabled(tmp_path_factory, monkeypatch):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_v013_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_stats_enabled_config_version_2(tmp_path_factory, monkeypatch):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_stats_enabled_config_version_3(tmp_path_factory, monkeypatch):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./test_fixtures/great_expectations_v013_upgraded_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "./test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_stats_enabled_config_version_2_with_checkpoint(
    tmp_path_factory, monkeypatch, titanic_data_context_stats_enabled_config_version_2
):
    context = titanic_data_context_stats_enabled_config_version_2
    root_dir = context.root_directory
    fixture_name = "my_checkpoint.yml"
    fixture_path = file_relative_path(
        __file__, f"./data_context/fixtures/contexts/{fixture_name}"
    )
    checkpoints_file = os.path.join(root_dir, "checkpoints", fixture_name)
    shutil.copy(fixture_path, checkpoints_file)
    return context


@pytest.fixture
def titanic_sqlite_db(sa):
    try:
        import sqlalchemy as sa
        from sqlalchemy import create_engine

        titanic_db_path = file_relative_path(__file__, "./test_sets/titanic.db")
        engine = create_engine("sqlite:///{}".format(titanic_db_path))
        assert engine.execute("select count(*) from titanic").fetchall()[0] == (1313,)
        return engine
    except ImportError:
        raise ValueError("sqlite tests require sqlalchemy to be installed")


@pytest.fixture
def titanic_expectation_suite():
    return ExpectationSuite(
        expectation_suite_name="Titanic.warning",
        meta={},
        data_asset_type="Dataset",
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist", kwargs={"column": "PClass"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "Name"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_equal",
                kwargs={"value": 1313},
            ),
        ],
    )


@pytest.fixture
def empty_sqlite_db(sa):
    """An empty in-memory sqlite db that always gets run."""
    try:
        import sqlalchemy as sa
        from sqlalchemy import create_engine

        engine = create_engine("sqlite://")
        assert engine.execute("select 1").fetchall()[0] == (1,)
        return engine
    except ImportError:
        raise ValueError("sqlite tests require sqlalchemy to be installed")


@pytest.fixture
@freeze_time("09/26/2019 13:42:41")
def site_builder_data_context_with_html_store_titanic_random(
    tmp_path_factory, filesystem_csv_3
):
    base_dir = str(tmp_path_factory.mktemp("project_dir"))
    project_dir = os.path.join(base_dir, "project_path")
    os.mkdir(project_dir)

    os.makedirs(os.path.join(project_dir, "data"))
    os.makedirs(os.path.join(project_dir, "data/titanic"))
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(os.path.join(project_dir, "data", "titanic", "Titanic.csv")),
    )

    os.makedirs(os.path.join(project_dir, "data", "random"))
    shutil.copy(
        os.path.join(filesystem_csv_3, "f1.csv"),
        str(os.path.join(project_dir, "data", "random", "f1.csv")),
    )
    shutil.copy(
        os.path.join(filesystem_csv_3, "f2.csv"),
        str(os.path.join(project_dir, "data", "random", "f2.csv")),
    )
    ge.data_context.DataContext.create(project_dir)
    shutil.copy(
        file_relative_path(
            __file__, "./test_fixtures/great_expectations_site_builder.yml"
        ),
        str(os.path.join(project_dir, "great_expectations", "great_expectations.yml")),
    )
    context = ge.data_context.DataContext(
        context_root_dir=os.path.join(project_dir, "great_expectations")
    )

    context.add_datasource(
        "titanic",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join(project_dir, "data", "titanic"),
            }
        },
    )
    context.add_datasource(
        "random",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join(project_dir, "data", "random"),
            }
        },
    )

    context.profile_datasource("titanic")
    context.profile_datasource("random")
    context.profile_datasource(context.list_datasources()[0]["name"])

    context._project_config.anonymous_usage_statistics = {
        "enabled": True,
        "data_context_id": "f43d4897-385f-4366-82b0-1a8eda2bf79c",
    }

    return context


@pytest.fixture(scope="function")
@freeze_time("09/26/2019 13:42:41")
def site_builder_data_context_v013_with_html_store_titanic_random(
    tmp_path, filesystem_csv_3
):
    base_dir = tmp_path / "project_dir"
    base_dir.mkdir()
    base_dir = str(base_dir)
    project_dir = os.path.join(base_dir, "project_path")
    os.mkdir(project_dir)

    os.makedirs(os.path.join(project_dir, "data"))
    os.makedirs(os.path.join(project_dir, "data", "titanic"))
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(os.path.join(project_dir, "data", "titanic", "Titanic.csv")),
    )

    os.makedirs(os.path.join(project_dir, "data", "random"))
    shutil.copy(
        os.path.join(filesystem_csv_3, "f1.csv"),
        str(os.path.join(project_dir, "data", "random", "f1.csv")),
    )
    shutil.copy(
        os.path.join(filesystem_csv_3, "f2.csv"),
        str(os.path.join(project_dir, "data", "random", "f2.csv")),
    )
    ge.data_context.DataContext.create(project_dir)
    shutil.copy(
        file_relative_path(
            __file__, "./test_fixtures/great_expectations_v013_site_builder.yml"
        ),
        str(os.path.join(project_dir, "great_expectations", "great_expectations.yml")),
    )
    context = ge.data_context.DataContext(
        context_root_dir=os.path.join(project_dir, "great_expectations")
    )

    context.add_datasource(
        "titanic",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join(project_dir, "data", "titanic"),
            }
        },
    )
    context.add_datasource(
        "random",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join(project_dir, "data", "random"),
            }
        },
    )

    context.profile_datasource("titanic")
    context.profile_datasource("random")
    context.profile_datasource(context.list_datasources()[0]["name"])

    context._project_config.anonymous_usage_statistics = {
        "enabled": True,
        "data_context_id": "f43d4897-385f-4366-82b0-1a8eda2bf79c",
    }

    return context


@pytest.fixture(scope="function")
def titanic_multibatch_data_context(tmp_path):
    """
    Based on titanic_data_context, but with 2 identical batches of
    data asset "titanic"
    """
    project_path = tmp_path / "titanic_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(__file__, "./test_fixtures/great_expectations_titanic.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, "./test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def v10_project_directory(tmp_path_factory):
    """
    GE 0.10.x project for testing upgrade helper
    """
    project_path = str(tmp_path_factory.mktemp("v10_project"))
    context_root_dir = os.path.join(project_path, "great_expectations")
    shutil.copytree(
        file_relative_path(
            __file__, "./test_fixtures/upgrade_helper/great_expectations_v10_project/"
        ),
        context_root_dir,
    )
    shutil.copy(
        file_relative_path(
            __file__, "./test_fixtures/upgrade_helper/great_expectations_v1_basic.yml"
        ),
        os.path.join(context_root_dir, "great_expectations.yml"),
    )
    return context_root_dir


@pytest.fixture
def v20_project_directory(tmp_path_factory):
    """
    GE config_version: 2 project for testing upgrade helper
    """
    project_path = str(tmp_path_factory.mktemp("v20_project"))
    context_root_dir = os.path.join(project_path, "great_expectations")
    shutil.copytree(
        file_relative_path(
            __file__, "./test_fixtures/upgrade_helper/great_expectations_v20_project/"
        ),
        context_root_dir,
    )
    shutil.copy(
        file_relative_path(
            __file__, "./test_fixtures/upgrade_helper/great_expectations_v2.yml"
        ),
        os.path.join(context_root_dir, "great_expectations.yml"),
    )
    return context_root_dir


@pytest.fixture
def data_context_parameterized_expectation_suite_no_checkpoint_store(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_basic.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sqlalchemy_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_with_bad_datasource(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()

    This DataContext has a connection to a datasource named my_postgres_db
    which is not a valid datasource.

    It is used by test_get_batch_multiple_datasources_do_not_scan_all()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_bad_datasource.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_parameterized_expectation_suite_no_checkpoint_store_with_usage_statistics_enabled(
    tmp_path_factory,
):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sqlalchemy_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_parameterized_expectation_suite(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_v013_basic.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sqlalchemy_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_parameterized_expectation_suite_with_usage_statistics_enabled(
    tmp_path_factory,
):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sqlalchemy_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_with_bad_notebooks(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    custom_notebook_assets_dir = "notebook_assets"

    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_basic_with_bad_notebooks.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),
    )

    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copytree(
        os.path.join(fixture_dir, custom_notebook_assets_dir),
        str(os.path.join(context_path, "plugins", custom_notebook_assets_dir)),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_custom_notebooks(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_custom_notebooks.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),
    )

    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)

    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_simple_expectation_suite(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_basic.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "rendering_fixtures/expectations_suite_1.json",
        ),
        os.path.join(asset_config_path, "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sqlalchemy_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_simple_expectation_suite_with_custom_pandas_dataset(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "./test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_basic_with_custom_pandas_dataset.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "rendering_fixtures/expectations_suite_1.json",
        ),
        os.path.join(asset_config_path, "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sqlalchemy_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def filesystem_csv_data_context_with_validation_operators(
    titanic_data_context_stats_enabled, filesystem_csv_2
):
    titanic_data_context_stats_enabled.add_datasource(
        "rad_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )
    return titanic_data_context_stats_enabled


@pytest.fixture()
def filesystem_csv_data_context(empty_data_context, filesystem_csv_2):
    empty_data_context.add_datasource(
        "rad_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_2),
            }
        },
    )
    return empty_data_context


@pytest.fixture
def filesystem_csv(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp("filesystem_csv")
    base_dir = str(base_dir)
    # Put a few files in the directory
    with open(os.path.join(base_dir, "f1.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])
    with open(os.path.join(base_dir, "f2.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])

    os.makedirs(os.path.join(base_dir, "f3"), exist_ok=True)
    with open(os.path.join(base_dir, "f3", "f3_20190101.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])
    with open(os.path.join(base_dir, "f3", "f3_20190102.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])

    return base_dir


@pytest.fixture(scope="function")
def filesystem_csv_2(tmp_path):
    base_dir = tmp_path / "filesystem_csv_2"
    base_dir.mkdir()
    base_dir = str(base_dir)

    # Put a file in the directory
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    toy_dataset.to_csv(os.path.join(base_dir, "f1.csv"), index=None)
    assert os.path.isabs(base_dir)
    assert os.path.isfile(os.path.join(base_dir, "f1.csv"))

    return base_dir


@pytest.fixture(scope="function")
def filesystem_csv_3(tmp_path):
    base_dir = tmp_path / "filesystem_csv_3"
    base_dir.mkdir()
    base_dir = str(base_dir)

    # Put a file in the directory
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    toy_dataset.to_csv(os.path.join(base_dir, "f1.csv"), index=None)

    toy_dataset_2 = PandasDataset({"y": [1, 2, 3]})
    toy_dataset_2.to_csv(os.path.join(base_dir, "f2.csv"), index=None)

    return base_dir


@pytest.fixture(scope="function")
def filesystem_csv_4(tmp_path):
    base_dir = tmp_path / "filesystem_csv_4"
    base_dir.mkdir()
    base_dir = str(base_dir)

    # Put a file in the directory
    toy_dataset = PandasDataset(
        {
            "x": [1, 2, 3],
            "y": [1, 2, 3],
        }
    )
    toy_dataset.to_csv(os.path.join(base_dir, "f1.csv"), index=None)

    return base_dir


@pytest.fixture
def titanic_profiled_evrs_1():
    with open(
        file_relative_path(
            __file__, "./render/fixtures/BasicDatasetProfiler_evrs.json"
        ),
    ) as infile:
        return expectationSuiteValidationResultSchema.loads(infile.read())


@pytest.fixture
def titanic_profiled_name_column_evrs():
    # This is a janky way to fetch expectations matching a specific name from an EVR suite.
    # TODO: It will no longer be necessary once we implement ValidationResultSuite._group_evrs_by_column
    from great_expectations.render.renderer.renderer import Renderer

    with open(
        file_relative_path(
            __file__, "./render/fixtures/BasicDatasetProfiler_evrs.json"
        ),
    ) as infile:
        titanic_profiled_evrs_1 = expectationSuiteValidationResultSchema.load(
            json.load(infile)
        )

    evrs_by_column = Renderer()._group_evrs_by_column(titanic_profiled_evrs_1)
    name_column_evrs = evrs_by_column["Name"]

    return name_column_evrs


@pytest.fixture
def titanic_profiled_expectations_1():
    with open(
        file_relative_path(
            __file__, "./render/fixtures/BasicDatasetProfiler_expectations.json"
        ),
    ) as infile:
        return expectationSuiteSchema.load(json.load(infile))


@pytest.fixture
def titanic_profiled_name_column_expectations():
    from great_expectations.render.renderer.renderer import Renderer

    with open(
        file_relative_path(
            __file__, "./render/fixtures/BasicDatasetProfiler_expectations.json"
        ),
    ) as infile:
        titanic_profiled_expectations = expectationSuiteSchema.load(json.load(infile))

    columns, ordered_columns = Renderer()._group_and_order_expectations_by_column(
        titanic_profiled_expectations
    )
    name_column_expectations = columns["Name"]

    return name_column_expectations


@pytest.fixture
def titanic_validation_results():
    with open(
        file_relative_path(__file__, "./test_sets/expected_cli_results_default.json"),
    ) as infile:
        return expectationSuiteValidationResultSchema.load(json.load(infile))


# various types of evr
@pytest.fixture
def evr_failed():
    return ExpectationValidationResult(
        success=False,
        result={
            "element_count": 1313,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 3,
            "unexpected_percent": 0.2284843869002285,
            "unexpected_percent_nonmissing": 0.2284843869002285,
            "partial_unexpected_list": [
                "Daly, Mr Peter Denis ",
                "Barber, Ms ",
                "Geiger, Miss Emily ",
            ],
            "partial_unexpected_index_list": [77, 289, 303],
            "partial_unexpected_counts": [
                {"value": "Barber, Ms ", "count": 1},
                {"value": "Daly, Mr Peter Denis ", "count": 1},
                {"value": "Geiger, Miss Emily ", "count": 1},
            ],
        },
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_match_regex",
            kwargs={
                "column": "Name",
                "regex": "^\\s+|\\s+$",
                "result_format": "SUMMARY",
            },
        ),
    )


@pytest.fixture
def evr_failed_with_exception():
    return ExpectationValidationResult(
        success=False,
        exception_info={
            "raised_exception": True,
            "exception_message": "Invalid partition object.",
            "exception_traceback": 'Traceback (most recent call last):\n  File "/great_expectations/great_expectations/data_asset/data_asset.py", line 216, in wrapper\n    return_obj = func(self, **evaluation_args)\n  File "/great_expectations/great_expectations/dataset/dataset.py", line 106, in inner_wrapper\n    evaluation_result = func(self, column, *args, **kwargs)\n  File "/great_expectations/great_expectations/dataset/dataset.py", line 3381, in expect_column_kl_divergence_to_be_less_than\n    raise ValueError("Invalid partition object.")\nValueError: Invalid partition object.\n',
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_kl_divergence_to_be_less_than",
            kwargs={
                "column": "live",
                "partition_object": None,
                "threshold": None,
                "result_format": "SUMMARY",
            },
            meta={"BasicDatasetProfiler": {"confidence": "very low"}},
        ),
    )


@pytest.fixture
def evr_success():
    return ExpectationValidationResult(
        success=True,
        result={"observed_value": 1313},
        exception_info={
            "raised_exception": False,
            "exception_message": None,
            "exception_traceback": None,
        },
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 0, "max_value": None, "result_format": "SUMMARY"},
        ),
    )


@pytest.fixture
def sqlite_view_engine(test_backends):
    # Create a small in-memory engine with two views, one of which is temporary
    if "sqlite" in test_backends:
        try:
            import sqlalchemy as sa

            sqlite_engine = sa.create_engine("sqlite://")
            df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
            df.to_sql(name="test_table", con=sqlite_engine, index=True)
            sqlite_engine.execute(
                "CREATE TEMP VIEW test_temp_view AS SELECT * FROM test_table where a < 4;"
            )
            sqlite_engine.execute(
                "CREATE VIEW test_view AS SELECT * FROM test_table where a > 4;"
            )
            return sqlite_engine
        except ImportError:
            sa = None
    else:
        pytest.skip("SqlAlchemy tests disabled; not testing views")


@pytest.fixture
def expectation_suite_identifier():
    return ExpectationSuiteIdentifier("my.expectation.suite.name")


@pytest.fixture
def basic_sqlalchemy_datasource(sqlitedb_engine):
    return SqlAlchemyDatasource("basic_sqlalchemy_datasource", engine=sqlitedb_engine)


@pytest.fixture
def test_cases_for_sql_data_connector_sqlite_execution_engine(sa):
    if sa is None:
        raise ValueError("SQL Database tests require sqlalchemy to be installed.")

    db_file = file_relative_path(
        __file__,
        os.path.join("test_sets", "test_cases_for_sql_data_connector.db"),
    )

    engine = sa.create_engine(f"sqlite:////{db_file}")
    conn = engine.connect()

    # Build a SqlAlchemyDataset using that database
    return SqlAlchemyExecutionEngine(
        name="test_sql_execution_engine",
        engine=conn,
    )


@pytest.fixture
def test_folder_connection_path_csv(tmp_path_factory):
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path_csv"))
    df1.to_csv(path_or_buf=os.path.join(path, "test.csv"), index=False)
    return str(path)


@pytest.fixture
def test_folder_connection_path_tsv(tmp_path_factory):
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path_tsv"))
    df1.to_csv(path_or_buf=os.path.join(path, "test.tsv"), sep="\t", index=False)
    return str(path)


@pytest.fixture
def test_folder_connection_path_parquet(tmp_path_factory):
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path_parquet"))
    df1.to_parquet(path=os.path.join(path, "test.parquet"))
    return str(path)


@pytest.fixture
def test_db_connection_string(tmp_path_factory, test_backends):
    if "sqlite" not in test_backends:
        pytest.skip("skipping fixture because sqlite not selected")
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    df2 = pd.DataFrame({"col_1": [0, 1, 2, 3, 4], "col_2": ["b", "c", "d", "e", "f"]})

    try:
        import sqlalchemy as sa

        basepath = str(tmp_path_factory.mktemp("db_context"))
        path = os.path.join(basepath, "test.db")
        engine = sa.create_engine("sqlite:///" + str(path))
        df1.to_sql(name="table_1", con=engine, index=True)
        df2.to_sql(name="table_2", con=engine, index=True, schema="main")

        # Return a connection string to this newly-created db
        return "sqlite:///" + str(path)
    except ImportError:
        raise ValueError("SQL Database tests require sqlalchemy to be installed.")


@pytest.fixture
def test_df(tmp_path_factory):
    def generate_ascending_list_of_datetimes(
        k, start_date=datetime.date(2020, 1, 1), end_date=datetime.date(2020, 12, 31)
    ):
        start_time = datetime.datetime(
            start_date.year, start_date.month, start_date.day
        )
        days_between_dates = (end_date - start_date).total_seconds()

        datetime_list = [
            start_time
            + datetime.timedelta(seconds=random.randrange(days_between_dates))
            for i in range(k)
        ]
        datetime_list.sort()
        return datetime_list

    k = 120
    random.seed(1)

    timestamp_list = generate_ascending_list_of_datetimes(
        k, end_date=datetime.date(2020, 1, 31)
    )
    date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

    batch_ids = [random.randint(0, 10) for i in range(k)]
    batch_ids.sort()

    session_ids = [random.randint(2, 60) for i in range(k)]
    session_ids.sort()
    session_ids = [i - random.randint(0, 2) for i in session_ids]

    events_df = pd.DataFrame(
        {
            "id": range(k),
            "batch_id": batch_ids,
            "date": date_list,
            "y": [d.year for d in date_list],
            "m": [d.month for d in date_list],
            "d": [d.day for d in date_list],
            "timestamp": timestamp_list,
            "session_ids": session_ids,
            "event_type": [
                random.choice(["start", "stop", "continue"]) for i in range(k)
            ],
            "favorite_color": [
                "#"
                + "".join([random.choice(list("0123456789ABCDEF")) for j in range(6)])
                for i in range(k)
            ],
        }
    )
    return events_df


@pytest.fixture
def test_connectable_postgresql_db(sa, test_backends, test_df):
    """Populates a postgres DB with a `test_df` table in the `connection_test` schema to test DataConnectors against"""

    if "postgresql" not in test_backends:
        pytest.skip("skipping fixture because postgresql not selected")

    import sqlalchemy as sa

    url = sa.engine.url.URL(
        drivername="postgresql",
        username="postgres",
        password="",
        host=os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        port="5432",
        database="test_ci",
    )
    engine = sa.create_engine(url)

    schema_check_results = engine.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'connection_test';"
    ).fetchall()
    if len(schema_check_results) == 0:
        engine.execute("CREATE SCHEMA connection_test;")

    table_check_results = engine.execute(
        """
SELECT EXISTS (
   SELECT FROM information_schema.tables
   WHERE  table_schema = 'connection_test'
   AND    table_name   = 'test_df'
);
"""
    ).fetchall()
    if table_check_results != [(True,)]:
        test_df.to_sql(name="test_df", con=engine, index=True, schema="connection_test")

    # Return a connection string to this newly-created db
    return engine


@pytest.fixture
def data_context_with_runtime_sql_datasource_for_testing_get_batch(
    sa, empty_data_context
):
    context = empty_data_context
    db_file = file_relative_path(
        __file__,
        os.path.join("test_sets", "test_cases_for_sql_data_connector.db"),
    )

    datasource_config = f"""
        class_name: Datasource

        execution_engine:
            class_name: SqlAlchemyExecutionEngine
            connection_string: sqlite:///{db_file}

        data_connectors:
            my_runtime_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - pipeline_stage_name
                    - airflow_run_id
        """

    context.test_yaml_config(
        name="my_runtime_sql_datasource", yaml_config=datasource_config
    )

    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def data_context_with_simple_sql_datasource_for_testing_get_batch(
    sa, empty_data_context
):
    context = empty_data_context

    db_file = file_relative_path(
        __file__,
        os.path.join("test_sets", "test_cases_for_sql_data_connector.db"),
    )

    config = yaml.load(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
introspection:
    whole_table: {}

    daily:
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%m-%d"

    weekly:
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%W"

    by_id_dozens:
        splitter_method: _split_on_divided_integer
        splitter_kwargs:
            column_name: id
            divisor: 12
""",
    )

    try:
        context.add_datasource("my_sqlite_db", **config)
    except AttributeError:
        pytest.skip("SQL Database tests require sqlalchemy to be installed.")

    return context


@pytest.fixture
def data_context_with_pandas_datasource_for_testing_get_batch(
    empty_data_context_v3, tmp_path_factory
):
    context = empty_data_context_v3

    base_directory: str = str(
        tmp_path_factory.mktemp(
            "data_context_with_pandas_datasource_for_testing_get_batch"
        )
    )

    sample_file_names: List[str] = [
        "test_dir_charlie/A/A-1.csv",
        "test_dir_charlie/A/A-2.csv",
        "test_dir_charlie/A/A-3.csv",
        "test_dir_charlie/B/B-1.csv",
        "test_dir_charlie/B/B-2.csv",
        "test_dir_charlie/B/B-3.csv",
        "test_dir_charlie/C/C-1.csv",
        "test_dir_charlie/C/C-2.csv",
        "test_dir_charlie/C/C-3.csv",
        "test_dir_charlie/D/D-1.csv",
        "test_dir_charlie/D/D-2.csv",
        "test_dir_charlie/D/D-3.csv",
    ]

    create_files_in_directory(
        directory=base_directory, file_name_list=sample_file_names
    )

    config = yaml.load(
        f"""
class_name: Datasource
execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {base_directory}/test_dir_charlie
        glob_directive: "*/*.csv"

        default_regex:
            pattern: (.+)/(.+)-(\\d+)\\.csv
            group_names:
                - subdirectory
                - data_asset_name
                - number
""",
    )

    context.add_datasource("my_pandas_datasource", **config)
    return context


@pytest.fixture
def basic_datasource(tmp_path_factory):
    base_directory: str = str(
        tmp_path_factory.mktemp("basic_datasource_runtime_data_connector")
    )

    basic_datasource: Datasource = instantiate_class_from_config(
        config=yaml.load(
            f"""
class_name: Datasource

data_connectors:
    test_runtime_data_connector:
        module_name: great_expectations.datasource.data_connector
        class_name: RuntimeDataConnector
        batch_identifiers:
        - pipeline_stage_name
        - airflow_run_id
        - custom_key_0

execution_engine:
    class_name: PandasExecutionEngine

    """,
        ),
        runtime_environment={
            "name": "my_datasource",
        },
        config_defaults={
            "module_name": "great_expectations.datasource",
        },
    )

    return basic_datasource


@pytest.fixture(scope="function")
def misc_directory(tmp_path):
    misc_dir = tmp_path / "random"
    misc_dir.mkdir()
    assert os.path.isabs(misc_dir)
    return misc_dir
