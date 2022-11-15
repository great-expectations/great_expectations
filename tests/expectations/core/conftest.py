import os

import pandas as pd
import pytest

from great_expectations.data_context.util import file_relative_path


@pytest.fixture(scope="module")
def titanic_df() -> pd.DataFrame:
    path = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "test_fixtures",
            "configuration_for_testing_v2_v3_migration",
            "data",
            "Titanic.csv",
        ),
    )
    df = pd.read_csv(path)
    return df
