import warnings

import pytest

import great_expectations as gx
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_context.util import file_relative_path


@pytest.mark.filesystem
def test_autoinspect_filedata_asset():
    # Expect an error to be raised since a file object doesn't have a columns attribute
    warnings.simplefilter("always", UserWarning)
    file_path = file_relative_path(__file__, "../test_sets/toy_data_complete.csv")
    my_file_data = gx.data_asset.FileDataAsset(file_path)

    with pytest.raises(gx.exceptions.GreatExpectationsError) as exc:
        my_file_data.profile(gx.profile.ColumnsExistProfiler)
    assert "Invalid data_asset for profiler; aborting" in exc.value.message

    # with warnings.catch_warnings(record=True):
    #     warnings.simplefilter("error")
    #     try:
    #         my_file_data.profile(ge.profile.ColumnsExistProfiler)
    #     except:
    #         raise
