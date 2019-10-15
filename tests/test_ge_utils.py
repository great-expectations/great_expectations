import pytest
import os

import great_expectations as ge


def test_validate_non_dataset(file_data_asset, empty_expectation_suite):
    with pytest.raises(ValueError, match=r"The validate util method only supports dataset validations"):
        ge.validate(file_data_asset, empty_expectation_suite, data_asset_class=ge.data_asset.FileDataAsset)


def test_validate_dataset(dataset, basic_expectation_suite):
    res = ge.validate(dataset, basic_expectation_suite)
    assert res["success"] is True
    assert res["statistics"]["evaluated_expectations"] == 4
    if isinstance(dataset, ge.dataset.PandasDataset):
        res = ge.validate(dataset,
                          expectation_suite=basic_expectation_suite,  data_asset_class=ge.dataset.PandasDataset)
        assert res["success"] is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset, basic_expectation_suite,  data_asset_class=ge.dataset.SqlAlchemyDataset)

    elif isinstance(dataset, ge.dataset.SqlAlchemyDataset):
        res = ge.validate(dataset,
                          expectation_suite=basic_expectation_suite,  data_asset_class=ge.dataset.SqlAlchemyDataset)
        assert res["success"] is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset,
                        expectation_suite=basic_expectation_suite,  data_asset_class=ge.dataset.PandasDataset)

    elif isinstance(dataset, ge.dataset.SparkDFDataset):
        res = ge.validate(dataset, basic_expectation_suite, data_asset_class=ge.dataset.SparkDFDataset)
        assert res["success"] is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(ValueError, match=r"The validate util method only supports validation for subtypes of the provided data_asset_type"):
            ge.validate(dataset,
                        expectation_suite=basic_expectation_suite,  data_asset_class=ge.dataset.PandasDataset)


def test_validate_using_data_context(dataset, data_context):
    # Before running, the data context should not have compiled parameters
    assert data_context._compiled is False
    res = ge.validate(
        dataset,
        data_asset_name="mydatasource/mygenerator/my_dag_node",
        expectation_suite_name="default",
        data_context=data_context)

    # Since the handling of evaluation parameters is no longer happening without an action,
    # the context should still be not compiles after validation.
    assert data_context._compiled is False

    # And, we should have validated the right number of expectations from the context-provided config
    assert res["success"] is False
    assert res["statistics"]["evaluated_expectations"] == 2


def test_validate_using_data_context_path(dataset, data_context):
    print(data_context._project_config)
    data_context_path = data_context.root_directory
    res = ge.validate(
        dataset,
        data_asset_name="mydatasource/mygenerator/my_dag_node",
        expectation_suite_name="default",
        data_context=data_context_path)

    # We should have now found the right suite with expectations to evaluate
    assert res["success"] is False
    assert res["statistics"]["evaluated_expectations"] == 2


def test_validate_invalid_parameters(dataset, basic_expectation_suite, data_context):
    with pytest.raises(ValueError, match="Either an expectation suite or a DataContext is required for validation."):
        ge.validate(dataset)


def test_gen_directory_tree_str(tmp_path_factory):
    project_dir = str(tmp_path_factory.mktemp("project_dir"))
    os.mkdir(os.path.join(project_dir, "BBB"))
    with open(os.path.join(project_dir, "BBB", "bbb.txt"), 'w') as f:
        f.write("hello")
    with open(os.path.join(project_dir, "BBB", "aaa.txt"), 'w') as f:
        f.write("hello")

    os.mkdir(os.path.join(project_dir, "AAA"))

    print(ge.util.gen_directory_tree_str(project_dir))

    #Note: files and directories are sorteds alphabetically, so that this method can be used for testing.
    assert ge.util.gen_directory_tree_str(project_dir) == """\
project_dir0/
    AAA/
    BBB/
        aaa.txt
        bbb.txt
"""