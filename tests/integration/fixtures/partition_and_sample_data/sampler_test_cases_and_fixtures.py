"""Test cases and fixtures for sampler integration test configurations."""

from dataclasses import dataclass
from typing import List

import pandas as pd


class SamplerTaxiTestData:
    def __init__(self, test_df: pd.DataFrame, test_column_name: str):
        self._test_df = test_df
        self._test_column_name = test_column_name

    @property
    def test_df(self):
        return self._test_df

    @property
    def test_column_name(self):
        return self._test_column_name

    def first_n_rows(self, n: int) -> pd.DataFrame:
        """Return first n rows of the test df.

        Args:
            n: Number of rows to include.

        Returns:
            The first n rows of the loaded test dataframe.
        """
        return self.test_df.head(n=n)


@dataclass
class TaxiSamplingTestCase:
    sampling_method_name: str
    sampling_kwargs: dict
    num_expected_batch_definitions: int
    num_expected_rows_in_first_batch_definition: int


class TaxiSamplingTestCases:
    def __init__(self, taxi_test_data: SamplerTaxiTestData):
        self._taxi_test_data = taxi_test_data

    @property
    def taxi_test_data(self) -> SamplerTaxiTestData:
        return self._taxi_test_data

    @property
    def test_df(self) -> pd.DataFrame:
        return self._taxi_test_data.test_df

    @property
    def test_column_name(self) -> str:
        return self._taxi_test_data.test_column_name

    def test_cases(self) -> List[TaxiSamplingTestCase]:
        return [
            TaxiSamplingTestCase(
                sampling_method_name="sample_using_limit",
                sampling_kwargs={"n": 10},
                num_expected_batch_definitions=1,
                num_expected_rows_in_first_batch_definition=10,
            ),
        ]
