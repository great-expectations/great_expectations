import pandas as pd

from great_expectations.datasource.util import hash_dataframe


def test_hash_dataframe_hashable_df():
    df = pd.DataFrame([{"col_1": 1}])
    assert hash_dataframe(df) == '03a57802981ad29e22de64b9a374ed86'


def test_hash_dataframe_unhashable_df():
    df = pd.DataFrame([{"col_1": {"val": 1}}])
    assert hash_dataframe(df) == 'b7f282f9e759690a3092a401e00d2e47'
