import great_expectations as gx

from great_expectations.data_context.util import file_relative_path
from tests.test_utils import _get_batch_request_from_validator
from tests.datasource.new_fixtures import (
    test_dir_alpha
)

def test_ZEP_scenario_1(test_dir_alpha):
    context = gx.get_context(lite=True)

    # Use the built-in datasource to get a validator, runtime-style
    my_validator_1 = context.sources.default_pandas_reader.read_csv(test_dir_alpha+"/A.csv")
    my_validator_1.head()
    my_validator_1.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    # Add a configured asset and use it to fetch a Validator
    context.sources.default_pandas_reader.add_asset(
        name="my_asset",
        base_directory=test_dir_alpha,
    )
    return
    my_validator_2 = context.sources.default_pandas_reader.assets.my_asset.get_validator(
        filename="B.csv"
    )
    my_validator_2.head()
    my_validator_2.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    # Refine the asset configuration by adding a more detailed regex
    context.sources.default_pandas_reader.add_asset(
        name="my_asset",
        base_directory=test_dir_alpha,
        regex="(.*)\.csv",
    )
    my_validator_3 = context.sources.default_pandas_reader.assets.my_asset.get_validator(
        filename="B"
    )
    my_validator_3.head()
    my_validator_3.expect_column_values_to_be_between("x", min_value=1, max_value=2)

    # Get a batch request spanning multiple files, and use it to configure a profiler
    my_batch_request = context.sources.default_pandas_reader.assets.my_asset.get_batch_request()
    assistant_result = context.assistants.onboarding.run(my_batch_request)



    my_batch_request = context.sources.default_pandas_reader.assets.my_asset.get_batch_request(
        filename="B.csv"
    )

    # my_batch_request = _get_batch_request_from_validator(my_validator)
    # # assert isinstance(
    # #     my_batch_request["runtime_parameters"]["batch_data"], pd.DataFrame
    # # )
    # assert my_batch_request["runtime_parameters"]["batch_data"].to_dict() == {
    #     "a": {0: 1, 1: 4},
    #     "b": {0: 2, 1: 5},
    #     "c": {0: 3, 1: 6},
    # }

