import pytest

import sqlalchemy as sa
import pandas as pd

from great_expectations import get_data_context
from great_expectations.dataset import PandasDataset, SqlAlchemyDataset


@pytest.fixture(scope="module")
def test_db_connection_string(tmpdir_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    df2 = pd.DataFrame(
        {'col_1': [0, 1, 2, 3, 4], 'col_2': ['b', 'c', 'd', 'e', 'f']})

    path = tmpdir_factory.mktemp("db_context").join("test.db")
    engine = sa.create_engine('sqlite:///' + str(path))
    df1.to_sql('table_1', con=engine, index=True)
    df2.to_sql('table_2', con=engine, index=True, schema='main')

    # Return a connection string to this newly-created db
    return 'sqlite:///' + str(path)


@pytest.fixture(scope="module")
def test_folder_connection_path(tmpdir_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    path = tmpdir_factory.mktemp("csv_context")
    df1.to_csv(path.join("test.csv"))

    return str(path)


def test_invalid_data_context():
    # Test an unknown data context name
    with pytest.raises(ValueError) as err:
        get_data_context('what_a_ridiculous_name', None)
        assert "Unknown data context." in str(err)


def test_sqlalchemy_data_context(test_db_connection_string):
    context = get_data_context(
        'SqlAlchemy', test_db_connection_string, echo=False)

    assert context.list_datasets() == ['table_1', 'table_2']
    dataset1 = context.get_dataset('table_1')
    dataset2 = context.get_dataset('table_2', schema='main')
    assert isinstance(dataset1, SqlAlchemyDataset)
    assert isinstance(dataset2, SqlAlchemyDataset)


def test_pandas_data_context(test_folder_connection_path):
    context = get_data_context('PandasCSV', test_folder_connection_path)

    assert context.list_datasets() == ['test.csv']
    dataset = context.get_dataset('test.csv')
    assert isinstance(dataset, PandasDataset)
