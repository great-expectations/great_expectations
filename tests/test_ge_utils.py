import pytest

import great_expectations as ge


def test_validate_non_dataset(file_data_asset, empty_expectation_suite):
    with pytest.raises(ValueError, match=r"The validate util method only supports dataset validations"):
        ge.validate(file_data_asset, empty_expectation_suite, data_asset_type=ge.data_asset.FileDataAsset)


def test_validate_dataset(dataset, basic_expectation_suite):
    res = ge.validate(dataset, basic_expectation_suite)
    assert res["success"] is True
    assert res["statistics"]["evaluated_expectations"] == 4
    if isinstance(dataset, ge.dataset.PandasDataset):
        res = ge.validate(dataset, basic_expectation_suite,  data_asset_type=ge.dataset.PandasDataset)
        assert res["success"] is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset, basic_expectation_suite,  data_asset_type=ge.dataset.SqlAlchemyDataset)

    elif isinstance(dataset, ge.dataset.SqlAlchemyDataset):
        res = ge.validate(dataset, basic_expectation_suite,  data_asset_type=ge.dataset.SqlAlchemyDataset)
        assert res["success"] is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset, basic_expectation_suite,  data_asset_type=ge.dataset.PandasDataset)

    elif isinstance(dataset, ge.dataset.SparkDFDataset):
        res = ge.validate(dataset, basic_expectation_suite, data_asset_type=ge.dataset.SparkDFDataset)
        assert res["success"] is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset, basic_expectation_suite,  data_asset_type=ge.dataset.PandasDataset)


def test_validate_using_data_context(dataset, data_context):
    # Before running, the data context should not have compiled parameters
    assert data_context._compiled is False
    res = ge.validate(dataset, data_asset_name="mydatasource/mygenerator/my_dag_node", data_context=data_context)

    # After handling a validation result registration, it should be
    assert data_context._compiled is True

    # And, we should have validated the right number of expectations from the context-provided config
    assert res["success"] is False
    assert res["statistics"]["evaluated_expectations"] == 2


def test_validate_using_data_context_path(dataset, data_context):
    data_context_path = data_context.root_directory
    res = ge.validate(dataset, data_asset_name="mydatasource/mygenerator/my_dag_node", data_context=data_context_path)

    # We should have now found the right suite with expectations to evaluate
    assert res["success"] is False
    assert res["statistics"]["evaluated_expectations"] == 2


def test_validate_invalid_parameters(dataset, basic_expectation_suite, data_context):
    with pytest.raises(ValueError, match="Either an expectation suite or a DataContext is required for validation."):
        ge.validate(dataset)