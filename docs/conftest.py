# import pytest
#
# import numpy
# import great_expectations
#
#
# @pytest.fixture
# def pandas_npi_dataset():
#     npi = great_expectations.dataset.PandasDataset({"provider_id": [1,2,3]})
#     return npi
#
#
# @pytest.fixture(autouse=True)
# def add_standard_imports(doctest_namespace, pandas_npi_dataset):
#     doctest_namespace["np"] = numpy
#     doctest_namespace["ge"] = great_expectations
#     doctest_namespace["npi"] = pandas_npi_dataset


from os import chdir, getcwd
from shutil import rmtree
from tempfile import mkdtemp
import pytest
from sybil import Sybil
from sybil.parsers.codeblock import CodeBlockParser
from sybil.parsers.doctest import DocTestParser

@pytest.fixture(scope="module")
def tempdir():
    # there are better ways to do temp directories, but it's a simple example:
    path = mkdtemp()
    cwd = getcwd()
    try:
        chdir(path)
        yield path
    finally:
        chdir(cwd)
        rmtree(path)

pytest_collect_file = Sybil(
    parsers=[
        DocTestParser(),
        CodeBlockParser(future_imports=['print_function']),
    ],
    pattern='*.rst',
    fixtures=['tempdir']
).pytest()
