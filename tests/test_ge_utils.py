import pytest
import great_expectations as ge


def test_validate_non_dataset(file_data_asset, empty_expectations_config):
    with pytest.raises(ValueError, match=r"The validate util method only supports dataset validations"):
        ge.validate(file_data_asset, empty_expectations_config, ge.data_asset.FileDataAsset)


def test_validate_dataset(dataset, basic_expectations_config):
    res = ge.validate(dataset, basic_expectations_config)
    assert res["success"] == True
    assert res["statistics"]["evaluated_expectations"] == 4
    if isinstance(dataset, ge.dataset.PandasDataset):
        res = ge.validate(dataset, basic_expectations_config, ge.dataset.PandasDataset)
        assert res["success"] == True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset, basic_expectations_config, ge.dataset.SqlAlchemyDataset)

    elif isinstance(dataset, ge.dataset.SqlAlchemyDataset):
        res = ge.validate(dataset, basic_expectations_config, ge.dataset.SqlAlchemyDataset)
        assert res["success"] == True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset, basic_expectations_config, ge.dataset.PandasDataset)

    elif isinstance(dataset, ge.dataset.SparkDFDataset):
        res = ge.validate(dataset, basic_expectations_config, ge.dataset.SparkDFDataset)
        assert res["success"] == True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset, basic_expectations_config, ge.dataset.PandasDataset)
