
'Provide GE package dependencies.\n\nThis module contains static lists of GE dependencies, along with a utility for\nchecking and updating these static lists.\n\n    Typical usage example:\n        ge_dependencies: GEDependencies = GEDependencies()\n        print(ge_dependencies.get_required_dependency_names())\n        print(ge_dependencies.get_dev_dependency_names())\n\n    To verify lists are accurate, you can run this file or execute main() from\n    within a cloned GE repository. This will check the existing requirements\n    files against the static lists returned via the methods above in the\n    usage example and raise exceptions if there are discrepancies.\n'
import os
import re
from typing import List, Set
from great_expectations.data_context.util import file_relative_path

class GEDependencies():
    'Store and provide dependencies when requested.\n\n    Also acts as a utility to check stored dependencies match our\n    library requirements.\n\n    Attributes: None\n    '
    'This list should be kept in sync with our requirements.txt file.'
    GE_REQUIRED_DEPENDENCIES: List[str] = sorted(['altair', 'Click', 'colorama', 'cryptography', 'importlib-metadata', 'Ipython', 'jinja2', 'jsonpatch', 'jsonschema', 'mistune', 'nbformat', 'notebook', 'numpy', 'packaging', 'pandas', 'pyparsing', 'python-dateutil', 'pytz', 'requests', 'ruamel.yaml', 'scipy', 'termcolor', 'tqdm', 'typing-extensions', 'urllib3', 'tzlocal'])
    'This list should be kept in sync with our requirements-dev*.txt files.'
    ALL_GE_DEV_DEPENDENCIES: List[str] = sorted(['PyMySQL', 'azure-identity', 'azure-keyvault-secrets', 'azure-storage-blob', 'black', 'boto3', 'feather-format', 'flake8', 'flask', 'freezegun', 'gcsfs', 'google-cloud-secret-manager', 'google-cloud-storage', 'ipywidgets', 'isort', 'mistune', 'mock-alchemy', 'moto', 'nbconvert', 'openpyxl', 'pre-commit', 'psycopg2-binary', 'pyarrow', 'pyathena', 'pyfakefs', 'pyodbc', 'pypd', 'pyspark', 'pytest', 'pytest-benchmark', 'pytest-cov', 'pytest-order', 'pyupgrade', 'requirements-parser', 's3fs', 'snapshottest', 'snowflake-connector-python', 'snowflake-sqlalchemy', 'sqlalchemy', 'sqlalchemy-bigquery', 'sqlalchemy-dremio', 'sqlalchemy-redshift', 'teradatasqlalchemy', 'xlrd'])
    GE_DEV_DEPENDENCIES_EXCLUDED_FROM_TRACKING: List[str] = ['black', 'flake8', 'isort', 'pre-commit', 'pytest-cov', 'pytest-order', 'pyupgrade', 'flask', 'freezegun', 'ipywidgets', 'mistune', 'mock-alchemy', 'moto', 'nbconvert', 'pyfakefs', 'pytest', 'pytest-benchmark', 'requirements-parser', 's3fs', 'snapshottest', 'trino']
    GE_DEV_DEPENDENCIES: List[str] = (set(ALL_GE_DEV_DEPENDENCIES) - set(GE_DEV_DEPENDENCIES_EXCLUDED_FROM_TRACKING))

    def __init__(self, requirements_relative_base_dir: str='../../../') -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._requirements_relative_base_dir = file_relative_path(__file__, requirements_relative_base_dir)
        self._dev_requirements_prefix: str = 'requirements-dev'

    def get_required_dependency_names(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Sorted list of required GE dependencies'
        return self.GE_REQUIRED_DEPENDENCIES

    def get_dev_dependency_names(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Sorted list of dev GE dependencies'
        return self.GE_DEV_DEPENDENCIES

    def get_required_dependency_names_from_requirements_file(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get unique names of required dependencies.\n\n        Returns:\n            List of string names of required dependencies.\n        '
        return sorted(set(self._get_dependency_names_from_requirements_file(self.required_requirements_path)))

    def get_dev_dependency_names_from_requirements_file(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get unique names of dependencies from all dev requirements files.\n        Returns:\n            List of string names of dev dependencies.\n        '
        dev_dependency_names: Set[str] = set()
        dev_dependency_filename: str
        for dev_dependency_filename in self.dev_requirements_paths:
            dependency_names: List[str] = self._get_dependency_names_from_requirements_file(os.path.join(self._requirements_relative_base_dir, dev_dependency_filename))
            dev_dependency_names.update(dependency_names)
        return sorted(dev_dependency_names)

    @property
    def required_requirements_path(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get path for requirements.txt\n\n        Returns:\n            String path of requirements.txt\n        '
        return os.path.join(self._requirements_relative_base_dir, 'requirements.txt')

    @property
    def dev_requirements_paths(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get all paths for requirements-dev files with dependencies in them.\n        Returns:\n            List of string filenames for dev requirements files\n        '
        return [filename for filename in os.listdir(self._requirements_relative_base_dir) if filename.startswith(self._dev_requirements_prefix)]

    def _get_dependency_names_from_requirements_file(self, filepath: str) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Load requirements file and parse to retrieve dependency names.\n\n        Args:\n            filepath: String relative filepath of requirements file to parse.\n\n        Returns:\n            List of string names of dependencies.\n        '
        with open(filepath) as f:
            dependencies_with_versions = f.read().splitlines()
            return self._get_dependency_names(dependencies_with_versions)

    def _get_dependency_names(self, dependencies: List[str]) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Parse dependency names from a list of strings.\n\n        List of strings typically from a requirements*.txt file.\n\n        Args:\n            dependencies: List of strings of requirements.\n\n        Returns:\n            List of dependency names. E.g. 'pandas' from 'pandas>=0.23.0'.\n        "
        dependency_matches = [re.search('^(?!--requirement)([\\w\\-.]+)', s) for s in dependencies]
        dependency_names: List[str] = []
        for match in dependency_matches:
            if (match is not None):
                dependency_names.append(match.group(0))
        return dependency_names

def main() -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Run this module to generate a list of packages from requirements files to update our static lists'
    ge_dependencies: GEDependencies = GEDependencies()
    print('\n\nRequired Dependencies:\n\n')
    print(ge_dependencies.get_required_dependency_names_from_requirements_file())
    print('\n\nDev Dependencies:\n\n')
    print(ge_dependencies.get_dev_dependency_names_from_requirements_file())
    assert (ge_dependencies.get_required_dependency_names() == ge_dependencies.get_required_dependency_names_from_requirements_file()), 'Mismatch between required dependencies in requirements files and in GEDependencies'
    assert (ge_dependencies.get_dev_dependency_names() == ge_dependencies.get_dev_dependency_names_from_requirements_file()), 'Mismatch between dev dependencies in requirements files and in GEDependencies'
    print('\n\nRequired and Dev dependencies in requirements files match those in GEDependencies')
if (__name__ == '__main__'):
    main()
