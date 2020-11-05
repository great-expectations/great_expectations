import numpy as np
import pandas as pd
from great_expectations.core.batch import Batch

from great_expectations.execution_engine import PandasExecutionEngine


def test_reader_fn():
    engine = PandasExecutionEngine()

    # Testing that can recognize basic excel file
    fn = engine._get_reader_fn(path="myfile.xlsx")
    assert "<function read_excel" in str(fn)

    # Ensuring that other way around works as well - reader_method should always override path
    fn_new = engine._get_reader_fn(reader_method="read_csv")
    assert "<function" in str(fn_new)


def test_get_compute_domain_with_no_domain_kwargs():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b":[2,3,4,None]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={})
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_column_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column": "a"})
    # Todo: why is this not querying the column?
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_row_condition():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_df = df[df['b'] > 2].reset_index()

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Todo: why is condition parser necessary?
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 2",
                                                                                     "condition_parser": "pandas"})
    # Ensuring data has been properly queried
    assert data['b'].equals(expected_df['b']), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert "row_condition" in compute_kwargs.keys(), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_resolve_metric_bundle():
    pass


def test_resolve_metric_bundle_with_nonexistent_metric():
    pass


def test_dataframe_property_given_loaded_batch():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Ensuring Data not distorted
    assert engine.dataframe.equals(df)






