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


def test_column_compute_domain():
    pass


def test_row_condition_compute_domain():
    pass


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






