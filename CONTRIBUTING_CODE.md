# Contribute a code change

To modify existing Great Expectations code, you complete the following tasks:

- [Fork and clone the Great Expectations repository](#fork-and-clone-the-great-expectations-repository)

- [Install Python dependencies](#install-python-dependencies)

- [Create a virtual environment (optional)](#create-a-virtual-environment-optional)

- [Install dependencies from requirements-dev.txt](#install-dependencies-from-requirements-devtxt)

- [Install great_expectations](#install-great_expectations)

- [Configure backends for testing (optional)](#configure-backends-for-testing-optional)

- [Test code changes](#test-code-changes)

- [Test performance](#test-performance)

- [Submit a pull request](#submit-a-pull-request)

To discuss your code change before you implement it, join the [Great Expectations Slack community](https://greatexpectations.io/slack) and make your suggestion in the [#contributing](https://greatexpectationstalk.slack.com/archives/CV828B2UX) channel.

To request a documentation change, or a change that doesn't require local testing, see the [README](https://github.com/great-expectations/great_expectations/tree/develop/docs) in the `docs` repository.

To create and submit a Custom Expectation to Great Expectations for consideration, see [CONTRIBUTING_EXPECTATIONS](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md) in the `great_expectations` repository.

To submit a custom package to Great Expectations for consideration, see [CONTRIBUTING_PACKAGES](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_PACKAGES.md) in the `great_expectations` repository.

## Prerequisites

- A GitHub account.

- A working version of Git on your computer. See [Getting Started - Installing Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

- A new SSH (Secure Shell Protocol) key. See [Generating a new SSH key and adding it to the ssh-agent](https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent).

- The latest Python version installed and configured. See [Python downloads](https://www.python.org/downloads/).

## Fork and clone the Great Expectations repository

1. Open a browser and go to the [Great Expectations repository](https://github.com/great-expectations/great_expectations).

2. Click **Fork** and then **Create Fork**.

3. Click **Code** and then select the **HTTPS** or **SSH** tabs.

4. Copy the URL, open a Git terminal, and then run the following command:

    ```sh
    git clone <url>
    ```
5. Run the following command to specify a new remote upstream repository that will be synced with the fork:

    ```sh
    git remote add upstream git@github.com:great-expectations/great_expectations.git
    ```
6. Run the following command to create a branch for your changes:

    ```sh
    git checkout -b <branch-name>
    ```
## Install Python dependencies

Python dependencies are required to modify Great Expectations code, submit a new feature, or submit a Custom Expectation.

1. Run the following command to create a virtual environment in your local repository using Python versions 3.8 to 3.11, activate the environment, and then install the necessary dependencies:

    ```python
    python3 -m venv gx_dev

    source gx_dev/bin/activate

    pip install --upgrade pip

    pip install -c constraints-dev.txt -e ".[test]"
    ```
    To specify other dependencies, add a comma after `test` and enter the dependency name. For example, "[test,postgresql,trino]". The supported dependencies include: `arrow`, `athena`, `aws_secrets`, `azure`, `azure_secrets`, `bigquery`, `dev`, `dremio`, `excel`, `gcp`, `hive`, `mssql`, `mysql`, `pagerduty`, `postgresql`, `redshift`, `s3`, `snowflake`, `spark`, `sqlalchemy`, `teradata`, `test`, `trino`, `vertica`.

2. Optional. If you're using Amazon Redshift, run the following command to install the `libpq-dev` package:

    ```sh
    sudo apt-get install -y libpq-dev
    ```

3. Optional. If you're using PostgreSQL, run the following command to install PostgreSQL:

    ```sh
    brew install postgresql
    ```

4. Optional. If you're using the `pyodbc`, `dremio`, or `mssql` modules to connect to ODBC databases, run one of the following commands:

    ```sh
    sudo apt-get install -y unixodbc-dev
    ```
    or
    ```sh
     brew install unixodbc
    ```

    If your Mac computer has an Apple Silicon chip, you might need to 
    
    1. specify additional compiler or linker options. For example:

    `export LDFLAGS="-L/opt/homebrew/Cellar/unixodbc/[your version]/lib"`

    `export CPPFLAGS="-I/opt/homebrew/Cellar/unixodbc/[your version]/include"`

    2. reinstall pyodbc:

    ```
    python -m pip install --force-reinstall --no-binary :all: pyodbc
    python -c "import pyodbc; print(pyodbc.version)"
    ```

    3. install the ODSBC 17 driver: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver15

5. Add `ulimit -n 4096` to the `~/.zshrc` or `~/.bashrc` files to prevent `OSError: [Errno 24] Too many open files` errors.

6. Run the following command to confirm pandas and SQLAlchemy with SQLite tests are passing:

   ```sh
    ulimit -n 4096

    pytest -v
   ```
## Create a virtual environment (optional)

A virtual environment allows you to create and test code without affecting the primary Great Expectations codebase.

### Python

1. Run the following command to create a virtual environment named `great_expectations_dev`:

   ```python
    python3 -m venv <path_to_environments_folder\>/great_expectations_dev
   ```
2. Run the following command to activate the virtual environment:

   ```python
    <source path_to_environments_folder\>/great_expectations_dev/bin/activate
   ```
### Anaconda

1. Run the following command to create a virtual environment named `great_expectations_dev`:

   ```python
    conda create --name great_expectations_dev
   ```
2. Run the following command to activate the virtual environment:

   ```python
    conda activate great_expectations_dev
   ```
## Install dependencies from requirements-dev.txt

You need to install dependencies from the Great Expectations `requirements-dev.txt` file to make sure you have the correct libraries installed in your Python environment.

If you're using the macOS operating system, you'll be able to use the `pip` or `pip3` commands to install dependencies from `requirements-dev.txt`. If you're a Windows user using Anaconda, you'll need to install the dependencies in the `requirements-dev.txt` individually.

Run the following command to install dependencies from requirements-dev.txt:

```sh
pip install -r requirements-dev.txt -c constraints-dev.txt
```

To add support for Spark or SQLAlchemy tests, you can substitute `requirements-dev-spark.txt` or `requirements-dev-sqlalchemy.txt` for `requirements-dev.txt`. For some database backends such as MSSQL, you might need to install additional drivers. See the following resources for more information:

- [Installing Microsoft ODBC driver for macOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos)

- [Installing Microsoft ODBC driver for Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)


##  Install Great Expectations

Run the following command in the root of the `great_expectations` repository to install Great Expectations in editable mode:

```sh
pip install -e .
```

`-e` will install Great Expectations in “editable” mode. This is not required, but is often very convenient as a developer.

## Configure backends for testing (optional)

Some Great Expectations features require specific backends for local testing.

### Prerequisites

- Docker (PostgreSQL and MySQL). See [Get Docker](https://docs.docker.com/get-docker/).

### PostgreSQL

1. CD to `assets/docker/postgresql` in your `great_expectations` repository, and then and run the following command:

    ```sh
    docker-compose up -d
    ```
2. Run the following command to verify the PostgreSQL container is running:

    ```sh
    docker-compose ps
    ```
    The command should return results similar to the following example:

    ````console
		Name                       Command              State           Ports
	———————————————————————————————————————————
	postgresql_travis_db_1   docker-entrypoint.sh postgres   Up      0.0.0.0:5432->5432/tcp
	````
3. Run the following command to run tests on the PostgreSQL container:

    ```sh
    pytest -v --postgresql
    ```

4. When you finish testing, run the following command to shut down the PostgreSQL container:

     ```sh
    docker-compose down
    ```
#### Troubleshooting

Errors similar to the following are returned when you try to start the PostgreSQL container and another service is using port 5432:

````console
	psycopg2.OperationalError: could not connect to server: Connection refused
	    Is the server running on host "localhost" (::1) and accepting
	    TCP/IP connections on port 5432?
	could not connect to server: Connection refused
	    Is the server running on host "localhost" (127.0.0.1) and accepting
	    TCP/IP connections on port 5432?
````
````console
	sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) FATAL:  database "test_ci" does not exist
	(Background on this error at: http://sqlalche.me/e/e3q8)
````
To resolve these errors, configure Docker to run on another port and confirm the server details are correct.

### MySQL

If another service is using port 3306, Docker might start the container but silently fail to set up the port.

1. CD to `assets/docker/mysql` in your `great_expectations` repository, and then and run the following command:

    ```sh
    docker-compose up -d
    ```
2. Run the following command to verify the MySQL container is running:

    ```sh
    docker-compose ps
    ```
    The command should return results similar to the following example:

    ```console
	      Name                   Command             State                 Ports
	------------------------------------------------------------------------------------------
	mysql_mysql_db_1   docker-entrypoint.sh mysqld   Up      0.0.0.0:3306->3306/tcp, 33060/tcp
	````
3. Run the following command to run tests on the MySQL container:

    ```sh
    pytest -v --mysql
    ```

4. When you finish testing, run the following command to shut down the MySQL container:

    ```sh
    docker-compose down
    ```

### Spark

Use the following information to use Spark for code testing.

#### Prerequisites

- Java. See [Java downloads](https://www.java.com/en/download/).

- The PATH or JAVA_HOME environment variables set correctly. See [Setting Java variables in Windows](https://www.ibm.com/docs/en/b2b-integrator/5.2?topic=installation-setting-java-variables-in-windows) or [Setting Java variables in Linux](https://www.ibm.com/docs/en/b2b-integrator/5.2?topic=installation-setting-java-variables-in-linux).

On Mac, run the following commands to set the PATH and JAVA_HOME environment variables:
```
export JAVA_HOME=`/usr/libexec/java_home`
export PATH=$PATH:$JAVA_HOME/bin
```


#### Install PySpark

When you install PySpark, Spark is also installed. See [Spark Overview](https://spark.apache.org/docs/latest/index.html#downloading).

Run the following command to install PySpark and Apache Spark:

```console
pip install pyspark
```

## Test code changes

Great Expectations production code must be thoroughly tested, and you must perform unit testing on all branches of every method, including likely error states. Most new feature contributions should include multiple unit tests. Contributions that modify or extend existing features should include a test of the new behavior.

Most contributions do not require new integration tests, unless they change the Great Expectations CLI.

Great Expectations code is not tested against all SQL database types. Continuous Integration (CI) testing for SQL is limited to PostgreSQL, SQLite, MS SQL, and BigQuery.

### Unit testing

To perform unit testing, run `pytest` in the `great_expectations` directory root. By default, tests are run against `pandas` and `sqlite`. You can use `pytest` flags to test additional backends like `postgresql`, `spark`, and `mssql`. For example, to run a test against PostgreSQL backend, you run `pytest --postgresql`.

The following are the supported `pytest` flags for general testing:

- `--spark`: Execute tests against Spark backend.
- `--postgresql`: Execute tests against PostgreSQL.
- `--mysql`: Execute tests against MySql.
- `--mssql`: Execute tests against Microsoft SQL Server.
- `--bigquery`: Execute tests against Google BigQuery (requires additional set up).
- `--aws`: Execute tests against AWS resources such as Amazon S3, Amazon Redshift, and Athena (requires additional setup).

To skip all local backend tests (except pandas), run `pytest --no-sqlalchemy`.

Testing can generate warning messages. These warnings are often caused by dependencies such as pandas or SQLAlchemy. Run `pytest --no-sqlalchemy --disable-pytest-warnings` to suppress these warnings.

### BigQuery testing

1. [Select or create a Cloud Platform project](https://console.cloud.google.com/project).

2. [Setup Authentication](https://googleapis.dev/python/google-api-core/latest/auth.html).

3. In your project, [create a BigQuery dataset](https://cloud.google.com/bigquery/docs/datasets) named `test_ci` and [set the dataset default table expiration](https://cloud.google.com/bigquery/docs/updating-datasets#table-expiration) to `.1` day.

4. Run the following command to test your project with the `GE_TEST_BIGQUERY_PROJECT` and `GE_TEST_BIGQUERY_DATASET` environment variables:

    ```bash
    GE_TEST_BIGQUERY_PROJECT=<YOUR_GOOGLE_CLOUD_PROJECT>
    GE_TEST_BIGQUERY_DATASET=test_ci
    pytest tests/test_definitions/test_expectations_cfe.py --bigquery
    ```

### Unit testing Expectations

One of the most significant features of an Expectation is that it produces the same result on all supported execution environments including pandas, SQLAlchemy, and Spark. To accomplish this, Great Expectations encapsulates unit tests for Expectations as JSON files. These files are used as fixtures and executed using a specialized test runner that executes tests against all execution environments.

The following is the test fixture file structure:

````json
{
    "expectation_type" : "expect_column_max_to_be_between",
    "datasets" : [{
        "data" : {...},
        "schemas" : {...},
        "tests" : [...]
    }]
}
````
Below `datasets` are three entries: `data`, `schemas`, and `tests`.

#### Data

The `data` parameter defines a DataFrame of sample data to apply Expectations against. The DataFrame is defined as a dictionary of lists, with keys containing column names and values containing lists of data entries. All lists within a dataset must have the same length. For example:

````console
"data" : {
    "w" : [1, 2, 3, 4, 5, 5, 4, 3, 2, 1],
    "x" : [2, 3, 4, 5, 6, 7, 8, 9, null, null],
    "y" : [1, 1, 1, 2, 2, 2, 3, 3, 3, 4],
    "z" : ["a", "b", "c", "d", "e", null, null, null, null, null],
    "zz" : ["1/1/2016", "1/2/2016", "2/2/2016", "2/2/2016", "3/1/2016", "2/1/2017", null, null, null, null],
    "a" : [null, 0, null, null, 1, null, null, 2, null, null],
},
````
#### Schemas

The `schema` parameter defines the types to be used when instantiating tests against different execution environments, including different SQL dialects. Each schema is defined as a dictionary with column names and types as key-value pairs. If the schema isn’t specified for a given execution environment, Great Expectations introspects values and attempts to identify the schema. For example:

````console
"schemas": {
    "sqlite": {
        "w" : "INTEGER",
        "x" : "INTEGER",
        "y" : "INTEGER",
        "z" : "VARCHAR",
        "zz" : "DATETIME",
        "a" : "INTEGER",
    },
    "postgresql": {
        "w" : "INTEGER",
        "x" : "INTEGER",
        "y" : "INTEGER",
        "z" : "TEXT",
        "zz" : "TIMESTAMP",
        "a" : "INTEGER",
    }
},
````
#### Tests

The `tests` parameter defines the tests to be executed against the DataFrame. Each item in `tests` must include `title`, `exact_match_out`, `in`, and `out`. The test runner executes the named Expectation once for each item, with the values in `in` supplied as kwargs.

The test passes if the values in the expectation Validation Result correspond with the values in `out`. If `exact_match_out` is true, then every field in the Expectation output must have a corresponding, matching field in `out`. If it’s false, then only the fields specified in `out` need to match. For most use cases, false is a better result, because it allows narrower targeting of the relevant output.

`suppress_test_for` is an optional parameter to disable an Expectation for a specific list of backends. For example:

````sh
"tests" : [{
    "title": "Basic negative test case",
    "exact_match_out" : false,
    "in": {
        "column": "w",
        "result_format": "BASIC",
        "min_value": null,
        "max_value": 4
    },
    "out": {
        "success": false,
        "observed_value": 5
    },
    "suppress_test_for": ["sqlite"]
},
...
]

````

The test fixture files are stored in subdirectories of `tests/test_definitions/` corresponding to the class of Expectation:

- column_map_expectations
- column_aggregate_expectations
- column_pair_map_expectations
- column_distributional_expectations
- multicolumn_map_expectations
- other_expectations

By convention, the name of the file is the name of the Expectation, with a .json suffix. Creating a new JSON file automatically adds the new Expectation tests to the test suite.

If you are implementing a new Expectation, but don’t plan to immediately implement it for all execution environments, you should add the new test to the appropriate lists in the `candidate_test_is_on_temporary_notimplemented_list_v2_api` method within `tests/test_utils.py`.

You can run just the Expectation tests with `pytest tests/test_definitions/test_expectations.py`.

## Test performance

Test the performance of code changes to determine they perform as expected. BigQuery is required to complete performance testing.

1. Run the following command to set up the data for testing:

    ```bash
    GE_TEST_BIGQUERY_PEFORMANCE_DATASET=<YOUR_GCP_PROJECT> tests/performance/setup_bigquery_tables_for_performance_test.sh
    ```

2. Run the following command to start the performance test:

    ```sh
    pytest tests/performance/test_bigquery_benchmarks.py \
    --bigquery --performance-tests \
    -k 'test_taxi_trips_benchmark[1-True-V3]'  \
    --benchmark-json=tests/performance/results/`date "+%H%M"`_${USER}.json \
    -rP -vv
    ```

    Some benchmarks take significant time to complete. In the previous example, only the `test_taxi_trips_benchmark[1-True-V3]` benchmark runs. The output should appear similar to the following:

    ```console
    --------------------------------------------------- benchmark: 1 tests ------------------------------------------------------
    Name (time in s)                         Min     Max    Mean  StdDev  Median     IQR  Outliers     OPS  Rounds  Iterations
    -----------------------------------------------------------------------------------------------------------------------------
    test_taxi_trips_benchmark[1-True-V3]     5.0488  5.0488  5.0488  0.0000  5.0488  0.0000       0;0  0.1981       1           1
    -----------------------------------------------------------------------------------------------------------------------------
    ```
3. Run the following command to compare the test results:

    ```
    $ py.test-benchmark compare --group-by name tests/performance/results/initial_baseline.json tests/performance/results/*${USER}.json
    ```

    The output should appear similar to the following:

    ```console
    ---------------------------------------------------------------------------- benchmark 'test_taxi_trips_benchmark[1-True-V3]': 2 tests ---------------------------------------------------------------------------
    Name (time in s)                                        Min               Max              Mean            StdDev            Median               IQR            Outliers     OPS            Rounds  Iterations
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    test_taxi_trips_benchmark[1-True-V3] (initial_base)     5.0488 (1.0)      5.0488 (1.0)      5.0488 (1.0)      0.0000 (1.0)      5.0488 (1.0)      0.0000 (1.0)           0;0  0.1981 (1.0)           1           1
    test_taxi_trips_benchmark[1-True-V3] (2114_work)        6.4675 (1.28)     6.4675 (1.28)     6.4675 (1.28)     0.0000 (1.0)      6.4675 (1.28)     0.0000 (1.0)           0;0  0.1546 (0.78)          1           1
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ```

4. Optional. If your change is intended to improve performance, run the following command to generate the benchmark results that confirm the performance improvement:

    ```
    $ tests/performance/run_benchmark_multiple_times.sh minimal_multithreading
    ```
     The name for the tests should include the first argument provided to the script. In the previous example, this was `tests/performance/results/minimal_multithreading_*.json`.

## Submit a pull request

1. Push your changes to the remote fork of your repository.

2. Create a pull request from your fork. See [Creating a pull request from a fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

3. Add a meaningful title and description for your pull request (PR). Provide a detailed explanation of what you changed and why.  To help identify the type of issue you’re submitting, add one of the following identifiers to the pull request (PR) title:

    - [BUGFIX] for PRs that address minor bugs without changing behavior.

    - [FEATURE] for significant PRs that add a new feature likely to require being added to our feature maturity matrix.

    - [MAINTENANCE] for PRs that focus on updating repository settings or related changes.

    - [CONTRIB] for the contribution of Custom Expectations and supporting work into the `contrib/` directory.

    - [HACKATHON] for submissions to an active Great Expectations Hackathon.

    In the section for design review, include a description of any prior discussion or coordination on the features in the PR, such as mentioning the number of the issue where discussion has taken place. For example: Closes #123”, linking to a relevant discuss or slack article, citing a team meeting, or even noting that no discussion is relevant because the issue is small.

4. If this is your first Great Expectations contribution, you'll be prompted to complete the Contributor License Agreement (CLA). Complete the CLA and add `@cla-bot check` as a comment to the pull request (PR) to indicate that you’ve completed it.

5. Wait for the Continuous Integration (CI) checks to complete and then correct any syntax or formatting issues.

    A Great Expectations team member reviews, approves, and merges your PR. Depending on your GitHub notification settings, you'll be notified when there are comments or when your changes are successfully merged.

## Issue tags

Great Expectations uses a `stalebot` to automatically tag issues without activity as `stale`, and closes them when a response is not received within a week. To prevent `stalebot` from closing an issue, you can add the `stalebot-exempt` tag.

Additionally, Great Expectations adds the following tags to indicate issue status:

- The`help wanted` tag identifies useful issues that require help from community contributors to accelerate development.

- The `enhacement` and `expectation-request` tags identify new Great Expectations features that require additional investigation and discussion.

- The `good first issue` tag identifies issues that provide an introduction to the Great Expectations contribution process.
