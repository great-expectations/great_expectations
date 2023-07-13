"""Provide GX package dependencies.

This module contains static lists of GX dependencies, along with a utility for
checking and updating these static lists.

    Typical usage example:
        ge_dependencies = GXDependencies()
        print(ge_dependencies.get_required_dependency_names())
        print(ge_dependencies.get_dev_dependency_names())

    To verify lists are accurate, you can run this file or execute main() from
    within a cloned GX repository. This will check the existing requirements
    files against the static lists returned via the methods above in the
    usage example and raise exceptions if there are discrepancies.
"""
import pathlib
import re
from typing import Dict, List, Set


class GXDependencies:
    """Store and provide dependencies when requested.

    Also acts as a utility to check stored dependencies match our
    library requirements.

    Attributes: None
    """

    """This list should be kept in sync with our requirements.txt file."""
    GX_REQUIRED_DEPENDENCIES: List[str] = sorted(
        [
            "altair",
            "Click",
            "colorama",
            "cryptography",
            "importlib-metadata",
            "Ipython",
            "ipywidgets",
            "jinja2",
            "jsonpatch",
            "jsonschema",
            "makefun",
            "marshmallow",
            "mistune",
            "nbformat",
            "notebook",
            "numpy",
            "packaging",
            "pandas",
            "pydantic",
            "pyparsing",
            "python-dateutil",
            "pytz",
            "requests",
            "ruamel.yaml",
            "scipy",
            "tqdm",
            "typing-extensions",
            "urllib3",
            "tzlocal",
        ]
    )

    """This list should be kept in sync with our requirements-dev*.txt files."""
    ALL_GX_DEV_DEPENDENCIES: List[str] = sorted(
        [
            "PyMySQL",
            "adr-tools-python",
            "azure-identity",
            "azure-keyvault-secrets",
            "azure-storage-blob",
            "black",
            "boto3",
            "clickhouse-sqlalchemy",
            "docstring-parser",
            "feather-format",
            "ruff",
            "flask",
            "freezegun",
            "gcsfs",
            "google-cloud-bigquery",
            "google-cloud-bigquery-storage",
            "google-cloud-secret-manager",
            "google-cloud-storage",
            "invoke",
            "mistune",
            "moto",
            "mypy",
            "nbconvert",
            "openpyxl",
            "pika",
            "pre-commit",
            "psycopg2-binary",
            "pyarrow",
            "pyathena",
            "pyfakefs",
            "pyodbc",
            "pypd",
            "pyspark",
            "pytest",
            "pytest-benchmark",
            "pytest-cov",
            "pytest-mock",
            "pytest-icdiff",
            "pytest-order",
            "pytest-random-order",
            "pytest-timeout",
            "requirements-parser",
            "responses",
            "snapshottest",
            "snowflake-connector-python",
            "snowflake-sqlalchemy",
            "sqlalchemy",
            "sqlalchemy-bigquery",
            "sqlalchemy-dremio",
            "sqlalchemy-redshift",
            "teradatasqlalchemy",
            "xlrd",
            "sqlalchemy-vertica-python",
        ]
    )

    GX_DEV_DEPENDENCIES_EXCLUDED_FROM_TRACKING: List[str] = [
        # requirements-dev-contrib.txt:
        "adr-tools-python",
        "black",
        "ruff",
        "invoke",
        "mypy",
        "pre-commit",
        "pytest-cov",
        "pytest-order",
        "pytest-random-order",
        # requirements-dev-lite.txt:
        "flask",
        "freezegun",
        "mistune",
        "moto",
        "ipykernel",
        "nbconvert",
        "py",
        "pyfakefs",
        "pytest",
        "pytest-benchmark",
        "pytest-mock",
        "pytest-icdiff",
        "pytest-timeout",
        "requirements-parser",
        "responses",
        "snapshottest",
        # "sqlalchemy",  # Not excluded from tracking
        "trino",
        "clickhouse-sqlalchemy",
        "PyHive",
        "thrift",
        "thrift-sasl",
        # requirements-dev-tools.txt
        "jupyter",
        "jupyterlab",
        "matplotlib",
        # requirements-dev-all-contrib-expectations.txt
        "arxiv",
        "barcodenumber",
        "blockcypher",
        "coinaddrvalidator",
        "cryptoaddress",
        "cryptocompare",
        "dataprofiler",
        "disposable_email_domains",
        "dnspython",
        "edtf_validate",
        "ephem",
        "geonamescache",
        "geopandas",
        "geopy",
        "global-land-mask",
        "gtin",
        "holidays",
        # "indiapins",      # Currently a broken package
        "ipwhois",
        "isbnlib",
        "langid",
        "pgeocode",
        "phonenumbers",
        "price_parser",
        "primefac",
        "prophet",
        "pwnedpasswords",
        "py-moneyed",
        "pydnsbl",
        "pygeos",
        "pyogrio",
        "python-geohash",
        "python-stdnum",
        "pyvat",
        "rtree",
        "schwifty",
        "scikit-learn",
        "shapely",
        "simple_icd_10",
        "sympy",
        "tensorflow",
        "timezonefinder",
        "us",
        "user_agents",
        "uszipcode",
        "yahoo_fin",
        "zipcodes",
        # requirements-dev-api-docs-test.txt
        "docstring-parser",
    ]

    GX_DEV_DEPENDENCIES: Set[str] = set(ALL_GX_DEV_DEPENDENCIES) - set(
        GX_DEV_DEPENDENCIES_EXCLUDED_FROM_TRACKING
    )

    DEV_REQUIREMENTS_PREFIX = "requirements-dev"
    PRIMARY_REQUIREMENTS_FILE = "requirements.txt"

    def __init__(self) -> None:
        self._requirements_paths = self._init_requirements_paths()

    def _init_requirements_paths(self) -> Dict[str, pathlib.Path]:
        project_root = pathlib.Path(__file__).parents[3]
        reqs_dir = project_root.joinpath("reqs")
        assert project_root.exists() and reqs_dir.exists()

        pattern = "requirements*.txt"

        req_dict = {}
        req_files = list(project_root.glob(pattern)) + list(reqs_dir.glob(pattern))
        for req_file in req_files:
            req_dict[req_file.name] = req_file

        return req_dict

    def get_required_dependency_names(self) -> List[str]:
        """Sorted list of required GX dependencies"""
        return [name.lower() for name in self.GX_REQUIRED_DEPENDENCIES]

    def get_dev_dependency_names(self) -> Set[str]:
        """Set of dev GX dependencies"""
        return {name.lower() for name in self.GX_DEV_DEPENDENCIES}

    def get_required_dependency_names_from_requirements_file(self) -> List[str]:
        """Get unique names of required dependencies. Lowercase names.

        Returns:
            List of string names of required dependencies.
        """
        return sorted(
            {
                name.lower()
                for name in self._get_dependency_names_from_requirements_file(
                    self._requirements_paths[self.PRIMARY_REQUIREMENTS_FILE]
                )
            }
        )

    def get_dev_dependency_names_from_requirements_file(self) -> List[str]:
        """Get unique lowercase names of dependencies from all dev requirements files.
        Returns:
            List of string names of dev dependencies.
        """
        dev_dependency_names: Set[str] = set()
        dev_dependency_paths: List[pathlib.Path] = [
            path
            for name, path in self._requirements_paths.items()
            if name.startswith(self.DEV_REQUIREMENTS_PREFIX)
        ]
        for dev_dependency_path in dev_dependency_paths:
            dependency_names: List[
                str
            ] = self._get_dependency_names_from_requirements_file(
                dev_dependency_path.absolute()
            )
            dev_dependency_names.update(dependency_names)
        return sorted(name.lower() for name in dev_dependency_names)

    def _get_dependency_names_from_requirements_file(
        self, filepath: pathlib.Path
    ) -> List[str]:
        """Load requirements file and parse to retrieve dependency names.

        Args:
            filepath: Absolute filepath of requirements file to parse.

        Returns:
            List of string names of dependencies.
        """
        with filepath.open() as f:
            dependencies_with_versions = f.read().splitlines()
            return self._get_dependency_names(dependencies_with_versions)

    def _get_dependency_names(self, dependencies: List[str]) -> List[str]:
        """Parse dependency names from a list of strings.

        List of strings typically from a requirements*.txt file.

        Args:
            dependencies: List of strings of requirements.

        Returns:
            List of dependency names. E.g. 'pandas' from 'pandas>=0.23.0'.
        """
        dependency_matches = [
            re.search(r"^(?!--requirement)([\w\-.]+)", s) for s in dependencies
        ]
        dependency_names: List[str] = []
        for match in dependency_matches:
            if match is not None:
                dependency_names.append(match.group(0))
        return dependency_names


def main() -> None:
    """Run this module to generate a list of packages from requirements files to update our static lists"""
    ge_dependencies = GXDependencies()
    print("\n\nRequired Dependencies:\n\n")
    print(ge_dependencies.get_required_dependency_names_from_requirements_file())
    print("\n\nDev Dependencies:\n\n")
    print(ge_dependencies.get_dev_dependency_names_from_requirements_file())
    assert (
        ge_dependencies.get_required_dependency_names()
        == ge_dependencies.get_required_dependency_names_from_requirements_file()
    ), "Mismatch between required dependencies in requirements files and in GXDependencies"
    assert ge_dependencies.get_dev_dependency_names() == set(
        ge_dependencies.get_dev_dependency_names_from_requirements_file()
    ) - set(
        GXDependencies.GX_DEV_DEPENDENCIES_EXCLUDED_FROM_TRACKING
    ), "Mismatch between dev dependencies in requirements files and in GXDependencies"
    print(
        "\n\nRequired and Dev dependencies in requirements files match those in GXDependencies"
    )


if __name__ == "__main__":
    main()
