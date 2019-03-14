from __future__ import division

import inspect
import json
import re
from datetime import datetime
from functools import wraps
import jsonschema

from numbers import Number

import numpy as np
import pandas as pd
from dateutil.parser import parse
from scipy import stats
from six import string_types

from .base import Dataset
from .util import (
    DocInherit,
    recursively_convert_to_json_serializable,
    is_valid_partition_object,
    is_valid_categorical_partition_object,
    is_valid_continuous_partition_object,
    infer_distribution_parameters,
    _scipy_distribution_positional_args_from_dict,
    validate_distribution_parameters,
    parse_result_format,
    create_multiple_expectations,
)


class MetaSparkDFDataset(Dataset):
    """MetaSparkDFDataset is a thin layer between Dataset and SparkDFDataset.
    This two-layer inheritance is required to make @classmethod decorators work.
    Practically speaking, that means that MetaSparkDFDataset implements \
    expectation decorators, like `column_map_expectation` and `column_aggregate_expectation`, \
    and SparkDFDataset implements the expectation methods themselves.
    """

    def __init__(self, *args, **kwargs):
        super(MetaSparkDFDataset, self).__init__(*args, **kwargs)


class SparkDFDataset(MetaSparkDFDataset):
    """
    PandasDataset instantiates the great_expectations Expectations API as a subclass of a pandas.DataFrame.
    For the full API reference, please see :func:`Dataset <great_expectations.Dataset.base.Dataset>`
    Notes:
        1. Samples and Subsets of PandaDataSet have ALL the expectations of the original \
           data frame unless the user specifies the ``discard_subset_failing_expectations = True`` \
           property on the original data frame.
        2. Concatenations, joins, and merges of PandaDataSets ONLY contain the \
           default_expectations (see :func:`add_default_expectations`)
    """

    def __init__(self, spark_df, *args, **kwargs):
        super(SparkDFDataset, self).__init__(*args, **kwargs)
        self.discard_subset_failing_expectations = kwargs.get("discard_subset_failing_expectations", False)
        # self.add_default_expectations()
        self.spark_df = spark_df

    @DocInherit
    @Dataset.expectation(["column"])
    def expect_column_to_exist(
        self, column, column_index=None, result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if column in self.spark_df.columns:
            return {
                # FIXME: list.index does not check for duplicate values.
                "success": (column_index is None) or (self.spark_df.columns.index(column) == column_index)
            }
        else:
            return {"success": False}
