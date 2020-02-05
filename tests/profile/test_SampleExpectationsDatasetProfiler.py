import json
import os
from collections import OrderedDict

import pytest
from six import PY2

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.datasource import PandasDatasource
from great_expectations.profile.base import DatasetProfiler
from great_expectations.profile.basic_dataset_profiler import SampleExpectationsDatasetProfiler
from great_expectations.profile.columns_exist import ColumnsExistProfiler
from tests.test_utils import expectationSuiteValidationResultSchema


def test__find_next_low_card_column(non_numeric_low_card_dataset, non_numeric_high_card_dataset):
    columns = non_numeric_low_card_dataset.get_table_columns()
    column_cache = {}
    profiled_columns = {
        "numeric": [],
        "low_card": [],
        "string": [],
        "datetime": []
    }

    assert SampleExpectationsDatasetProfiler._find_next_low_card_column(
        non_numeric_low_card_dataset,
        columns,
        profiled_columns,
        column_cache
    ) == "lowcardnonnum"
    x = 123