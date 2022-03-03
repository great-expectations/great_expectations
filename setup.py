from setuptools import find_packages, setup

import versioneer

# Parse requirements.txt
with open("requirements.txt") as f:
    required = f.read().splitlines()

with open("requirements-dev-lite.txt") as f:
    dev_lite = f.read().splitlines()

req_boto = [req for req in dev_lite if req.startswith("boto")]
req_sqlalchemy = [req for req in dev_lite if req.startswith("sqlalchemy")]

with open("requirements-dev-airflow.txt") as f:
    req_airflow = f.read().splitlines()

with open("requirements-dev-arrow.txt") as f:
    req_arrow = f.read().splitlines()

with open("requirements-dev-athena.txt") as f:
    req_athena = f.read().splitlines()

with open("requirements-dev-azure.txt") as f:
    req_azure = f.read().splitlines()

with open("requirements-dev-bigquery.txt") as f:
    req_bigquery = f.read().splitlines()

with open("requirements-dev-dremio.txt") as f:
    req_dremio = f.read().splitlines()

with open("requirements-dev-excel.txt") as f:
    req_excel = f.read().splitlines()

with open("requirements-dev-mssql.txt") as f:
    req_mssql = f.read().splitlines()

with open("requirements-dev-mysql.txt") as f:
    req_mysql = f.read().splitlines()

with open("requirements-dev-pagerduty.txt") as f:
    req_pagerduty = f.read().splitlines()

with open("requirements-dev-postgresql.txt") as f:
    req_postgresql = f.read().splitlines()

with open("requirements-dev-redshift.txt") as f:
    req_redshift = f.read().splitlines()

with open("requirements-dev-snowflake.txt") as f:
    req_snowflake = f.read().splitlines()

with open("requirements-dev-spark.txt") as f:
    req_spark = f.read().splitlines()

with open("requirements-dev-teradata.txt") as f:
    req_teradata = f.read().splitlines()

# try:
#    import pypandoc
#    long_description = pypandoc.convert_file('README.md', 'rst')
# except (IOError, ImportError):
long_description = "Always know what to expect from your data. (See https://github.com/great-expectations/great_expectations for full description)."

config = {
    "description": "Always know what to expect from your data.",
    "author": "The Great Expectations Team",
    "url": "https://github.com/great-expectations/great_expectations",
    "author_email": "team@greatexpectations.io",
    "version": versioneer.get_version(),
    "cmdclass": versioneer.get_cmdclass(),
    "install_requires": required,
    "extras_require": {
        "airflow": req_airflwow + req_boto,
        "arrow": req_arrow,
        "athena": req_athena,
        "aws_secrets": req_boto,
        "azure": req_azure,
        "azure_secrets": req_azure,
        "bigquery": req_bigquery,
        "dremio": req_dremio,
        "excel": req_excel,
        "gcp": req_bigquery,
        "mssql": req_mssql,
        "mysql": req_mysql,
        "pagerduty": req_pagerduty,
        "postgresql": req_postgresql,
        "redshift": req_redshift,
        # "s3": ["boto3>=1.14"],
        "s3": req_boto,
        "snowflake": snowflake,
        "spark": req_spark,
        "sqlalchemy": req_sqlalchemy,
        "teradata": req_teradata,
    },
    "packages": find_packages(exclude=["contrib*", "docs*", "tests*", "examples*"]),
    "entry_points": {
        "console_scripts": ["great_expectations=great_expectations.cli:main"]
    },
    "name": "great_expectations",
    "long_description": long_description,
    "license": "Apache-2.0",
    "keywords": "data science testing pipeline data quality dataquality validation datavalidation",
    "include_package_data": True,
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Topic :: Software Development :: Testing",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
}

setup(**config)
