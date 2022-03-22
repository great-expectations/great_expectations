import sys
from importlib import metadata
from unittest import mock

from packaging import version

# mymodule = mock.MagicMock()
from great_expectations.core.usage_statistics.package_dependencies import GEDependencies


class MockWithVersion(mock.MagicMock):
    __version__ = "9.9.9"


mymodule = MockWithVersion()


@mock.patch.dict(sys.modules, some_module=MockWithVersion())
def test_get_imported_packages():
    # import sys
    # module = sys.modules["scipy"]
    # print(module)
    # print(module.__version__)
    # print(type(sys.modules))

    mymodule2 = MockWithVersion()
    print(mymodule2.__version__)

    v = version.parse(metadata.version("scipy"))
    print(v)
    print(type(v))

    try:
        print("Yay")
        print(sys.modules["some_module"])
        print(sys.modules["some_module"].__version__)
        module_version = metadata.version("some_module")
        print(module_version)
        print("yay2")
        # v = version.parse(metadata.version("some_module"))
        # print(v)
        # print(type(v))
    except metadata.PackageNotFoundError:
        pass


def test_get_required_dependency_names():
    ge_dependencies = GEDependencies().get_required_dependency_names()
    expected_ge_dependencies = [
        "altair",
        "Click",
        "colorama",
        "cryptography",
        "importlib-metadata",
        "ipywidgets",
        "jinja2",
        "jsonpatch",
        "jsonschema",
        "mistune",
        "nbformat",
        "numpy",
        "packaging",
        "pandas",
        "pyparsing",
        "python-dateutil",
        "pytz",
        "requests",
        "ruamel.yaml",
        "scipy",
        "termcolor",
        "tqdm",
        "typing-extensions",
        "urllib3",
        "tzlocal",
    ]
    assert ge_dependencies == expected_ge_dependencies


def test_pandas_package_version_created():
    pass
