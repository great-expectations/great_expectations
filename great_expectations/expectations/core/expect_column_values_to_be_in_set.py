from typing import Optional, Union

import numpy as np
import pandas as pd

from ...core.expectation_configuration import ExpectationConfiguration
from ...dataset.dataset import DatasetBackendTypes
from ..expectation import (
    ColumnMapDatasetExpectation,
    InvalidExpectationConfigurationError,
)

# equivalence kwargs -> "column"
# validation kwargs += "value_set", "mostly"
# runtime kwargs += "result_format"


class ExpectColumnValuesToBeInSet(ColumnMapDatasetExpectation):
    validation_kwargs = ["column", "value_set", "mostly"]

    def build_validation_kwargs(self, configuration: ExpectationConfiguration):
        validation_kwargs = super().build_validation_kwargs(configuration)
        validation_kwargs.update(
            {"value_set": configuration.kwargs["value_set"],}
        )
        return validation_kwargs

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert "value_set" in configuration.kwargs, "value_set is required"
            assert isinstance(
                configuration.kwargs["value_set"], (list, set)
            ), "value_set must be a list or a set"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    # @builds_series as a property
    # Metric    production     can    be    a    part   of    an    expectation    Metric is a    class !

    # FIXME ADD
    # @validates(validation_engine="pandas")
    def _validate_pandas_series(
        self, series: pd.Series, value_set: Union[list, set], **kwargs
    ):
        if value_set is None:
            # Vacuously true
            return np.ones(len(series), dtype=np.bool_)
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed_value_set = self.parse_value_set(
                backend_type=DatasetBackendTypes.PandasDataFrame, value_set=value_set
            )
        else:
            parsed_value_set = value_set

        return series.isin(parsed_value_set)

    # @validates(validation_engine="sqlalchemy")
    # def _dasf:
    #     return condition
    #
    # @validates(validation_engine="sqlalchemy")
    # def _dasf:
    #     return condition
    #
    # @renders(StringTemplate, modes=())
    # def lkjdsf(self, mode={prescriptive}, {descriptive}, {valiation}):
    #     return "I'm a thing"
