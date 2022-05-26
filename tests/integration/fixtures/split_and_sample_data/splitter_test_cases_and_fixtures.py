import datetime
from dataclasses import dataclass
from typing import List

import pandas as pd

from great_expectations.execution_engine.split_and_sample.data_splitter import DatePart


class TaxiTestData:
    def __init__(self, test_df: pd.DataFrame, test_column_name: str):
        self._test_df = test_df
        self._test_column_name = test_column_name

    @property
    def test_df(self):
        return self._test_df

    @property
    def test_column_name(self):
        return self._test_column_name

    def years_in_taxi_data(self) -> List[datetime.datetime]:
        return (
            pd.date_range(start="2018-01-01", end="2020-12-31", freq="AS")
            .to_pydatetime()
            .tolist()
        )

    def year_batch_identifier_data(self) -> List[dict]:
        return [{DatePart.YEAR.value: dt.year} for dt in self.years_in_taxi_data()]

    def months_in_taxi_data(self) -> List[datetime.datetime]:
        return (
            pd.date_range(start="2018-01-01", end="2020-12-31", freq="MS")
            .to_pydatetime()
            .tolist()
        )

    def year_month_batch_identifier_data(self) -> List[dict]:
        return [
            {DatePart.YEAR.value: dt.year, DatePart.MONTH.value: dt.month}
            for dt in self.months_in_taxi_data()
        ]

    def month_batch_identifier_data(self) -> List[dict]:
        return [{DatePart.MONTH.value: dt.month} for dt in self.months_in_taxi_data()]

    def year_month_day_batch_identifier_data(self) -> List[dict]:
        # Since taxi data does not contain all days,
        # we need to introspect the data to build the fixture:
        year_month_day_batch_identifier_list_unsorted: List[dict] = list(
            {val[0]: val[1], val[2]: val[3], val[4]: val[5]}
            for val in {
                (
                    DatePart.YEAR.value,
                    dt.year,
                    DatePart.MONTH.value,
                    dt.month,
                    DatePart.DAY.value,
                    dt.day,
                )
                for dt in self.test_df[self.test_column_name]
            }
        )

        return sorted(
            year_month_day_batch_identifier_list_unsorted,
            key=lambda x: (
                x[DatePart.YEAR.value],
                x[DatePart.MONTH.value],
                x[DatePart.DAY.value],
            ),
        )


@dataclass
class TaxiSplittingTestCase:
    splitter_method_name: str
    splitter_kwargs: dict
    num_expected_batch_definitions: int
    num_expected_rows_in_first_batch_definition: int
    expected_pickup_datetimes: List[dict]


class TaxiSplittingTestCases:
    def __init__(self, taxi_test_data: TaxiTestData):
        self._taxi_test_data = taxi_test_data

    @property
    def taxi_test_data(self) -> TaxiTestData:
        return self._taxi_test_data

    @property
    def test_df(self) -> pd.DataFrame:
        return self._taxi_test_data.test_df

    @property
    def test_column_name(self) -> str:
        return self._taxi_test_data.test_column_name

    def test_cases(self) -> List[TaxiSplittingTestCase]:
        return [
            TaxiSplittingTestCase(
                splitter_method_name="split_on_year",
                splitter_kwargs={"column_name": self.taxi_test_data.test_column_name},
                num_expected_batch_definitions=3,
                num_expected_rows_in_first_batch_definition=120,
                expected_pickup_datetimes=self.taxi_test_data.year_batch_identifier_data(),
            ),
            TaxiSplittingTestCase(
                splitter_method_name="split_on_year_and_month",
                splitter_kwargs={"column_name": self.taxi_test_data.test_column_name},
                num_expected_batch_definitions=36,
                num_expected_rows_in_first_batch_definition=10,
                expected_pickup_datetimes=self.taxi_test_data.year_month_batch_identifier_data(),
            ),
            TaxiSplittingTestCase(
                splitter_method_name="split_on_year_and_month_and_day",
                splitter_kwargs={"column_name": self.taxi_test_data.test_column_name},
                num_expected_batch_definitions=299,
                num_expected_rows_in_first_batch_definition=2,
                expected_pickup_datetimes=self.taxi_test_data.year_month_day_batch_identifier_data(),
            ),
            TaxiSplittingTestCase(
                splitter_method_name="split_on_date_parts",
                splitter_kwargs={
                    "column_name": self.taxi_test_data.test_column_name,
                    "date_parts": [DatePart.MONTH],
                },
                num_expected_batch_definitions=12,
                num_expected_rows_in_first_batch_definition=30,
                expected_pickup_datetimes=self.taxi_test_data.month_batch_identifier_data(),
            ),
            # Mix of types of date_parts, mixed case for string date part:
            TaxiSplittingTestCase(
                splitter_method_name="split_on_date_parts",
                splitter_kwargs={
                    "column_name": self.taxi_test_data.test_column_name,
                    "date_parts": [DatePart.YEAR, "mOnTh"],
                },
                num_expected_batch_definitions=36,
                num_expected_rows_in_first_batch_definition=10,
                expected_pickup_datetimes=self.taxi_test_data.year_month_batch_identifier_data(),
            ),
        ]
