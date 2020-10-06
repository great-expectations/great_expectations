"""
Test the ability to use datetime objects as run time evaluation parameters.
"""

import pandas as pd

from great_expectations.dataset import PandasDataset


def test_pandas_datetime_evaluation_parameter():
    evaluation_params = {"now": pd.Timestamp.now(), "now_minus_48h": pd.Timestamp.now() - pd.to_timedelta(2, unit="d")}

    test_data = {
        "data_refresh": [pd.Timestamp.now(), (pd.Timestamp.now() - pd.to_timedelta(1, unit="d"))]
    }
    _df = pd.DataFrame(test_data)
    df = PandasDataset(_df)

    for param in evaluation_params:
        df.set_evaluation_parameter(param, evaluation_params[param])
    df.expect_column_max_to_be_between(column="data_refresh", min_value={"$PARAMETER": "now_minus_48h"})

    result = df.validate()

    assert result.success
