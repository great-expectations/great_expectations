from great_expectations.self_check.util import build_test_backends_list


def pytest_addoption(parser):
    # note: --no-spark will be deprecated in favor of --spark
    parser.addoption(
        "--no-spark",
        action="store_true",
        help="If set, suppress tests against the spark test suite",
    )
    parser.addoption(
        "--spark",
        action="store_true",
        help="If set, execute tests against the spark test suite",
    )
    parser.addoption(
        "--no-sqlalchemy",
        action="store_true",
        help="If set, suppress all tests using sqlalchemy",
    )
    parser.addoption(
        "--postgresql",
        action="store_true",
        help="If set, execute tests against postgresql",
    )
    # note: --no-postgresql will be deprecated in favor of --postgresql
    parser.addoption(
        "--no-postgresql",
        action="store_true",
        help="If set, supress tests against postgresql",
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
        "--bigquery",
        action="store_true",
        help="If set, execute tests against bigquery",
    )
    parser.addoption(
        "--aws",
        action="store_true",
        help="If set, execute tests against AWS resources like S3, RedShift and Athena",
    )
    parser.addoption(
        "--trino",
        action="store_true",
        help="If set, execute tests against trino",
    )
    parser.addoption(
        "--redshift",
        action="store_true",
        help="If set, execute tests against redshift",
    )
    parser.addoption(
        "--athena",
        action="store_true",
        help="If set, execute tests against athena",
    )
    parser.addoption(
        "--snowflake",
        action="store_true",
        help="If set, execute tests against snowflake",
    )
    parser.addoption(
        "--aws-integration",
        action="store_true",
        help="If set, run aws integration tests for usage_statistics",
    )
    parser.addoption(
        "--docs-tests",
        action="store_true",
        help="If set, run integration tests for docs",
    )
    parser.addoption(
        "--azure", action="store_true", help="If set, execute tests again Azure"
    )
    parser.addoption(
        "--cloud", action="store_true", help="If set, execute tests again GX Cloud"
    )
    parser.addoption(
        "--performance-tests",
        action="store_true",
        help="If set, run performance tests (which might also require additional arguments like --bigquery)",
    )


def pytest_generate_tests(metafunc):
    test_backends = build_test_backends_list(metafunc)
    if "test_backend" in metafunc.fixturenames:
        metafunc.parametrize("test_backend", test_backends, scope="module")
    if "test_backends" in metafunc.fixturenames:
        metafunc.parametrize("test_backends", [test_backends], scope="module")
