from setuptools import find_packages, setup

import versioneer

# Parse requirements.txt
with open("requirements.txt") as f:
    required = f.read().splitlines()

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
        "spark": ["pyspark>=2.3.2"],
        "sqlalchemy": ["sqlalchemy>=1.2,<1.4.0"],
        "airflow": ["apache-airflow[s3]>=1.9.0", "boto3>=1.7.3"],
        "gcp": [
            "google-cloud>=0.34.0",
            "google-cloud-storage>=1.28.0",
            "google-cloud-secret-manager>=1.0.0",
            "pybigquery==0.4.15",
        ],
        "redshift": ["psycopg2>=2.8"],
        "s3": ["boto3>=1.14"],
        "aws_secrets": ["boto3>=1.8.7"],
        "azure_secrets": ["azure-identity>=1.0.0", "azure-keyvault-secrets>=4.0.0"],
        "snowflake": ["snowflake-sqlalchemy>=1.2"],
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
    ],
}

setup(**config)
