import copy
import os

import pytest

import great_expectations as ge
from great_expectations.core.util import nested_update
from great_expectations.dataset.util import check_sql_engine_dialect
from great_expectations.util import (
    filter_properties_dict,
    get_currently_executing_function_call_arguments,
    lint_code,
)


def test_validate_non_dataset(file_data_asset, empty_expectation_suite):
    with pytest.raises(
        ValueError, match=r"The validate util method only supports dataset validations"
    ):
        with pytest.warns(
            Warning,
            match="No great_expectations version found in configuration object.",
        ):
            ge.validate(
                file_data_asset,
                empty_expectation_suite,
                data_asset_class=ge.data_asset.FileDataAsset,
            )


def test_validate_dataset(dataset, basic_expectation_suite):
    res = ge.validate(dataset, basic_expectation_suite)
    # assert res.success is True  # will not be true for mysql, where "infinities" column is missing
    assert res["statistics"]["evaluated_expectations"] == 4
    if isinstance(dataset, ge.dataset.PandasDataset):
        res = ge.validate(
            dataset,
            expectation_suite=basic_expectation_suite,
            data_asset_class=ge.dataset.PandasDataset,
        )
        assert res.success is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(
            ValueError,
            match=r"The validate util method only supports validation for subtypes of the provided data_asset_type",
        ):
            ge.validate(
                dataset,
                basic_expectation_suite,
                data_asset_class=ge.dataset.SqlAlchemyDataset,
            )

    elif (
        isinstance(dataset, ge.dataset.SqlAlchemyDataset)
        and dataset.sql_engine_dialect.name.lower() != "mysql"
    ):
        res = ge.validate(
            dataset,
            expectation_suite=basic_expectation_suite,
            data_asset_class=ge.dataset.SqlAlchemyDataset,
        )
        assert res.success is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(
            ValueError,
            match=r"The validate util method only supports validation for subtypes of the provided data_asset_type",
        ):
            ge.validate(
                dataset,
                expectation_suite=basic_expectation_suite,
                data_asset_class=ge.dataset.PandasDataset,
            )

    elif (
        isinstance(dataset, ge.dataset.SqlAlchemyDataset)
        and dataset.sql_engine_dialect.name.lower() == "mysql"
    ):
        # mysql cannot use the infinities column
        res = ge.validate(
            dataset,
            expectation_suite=basic_expectation_suite,
            data_asset_class=ge.dataset.SqlAlchemyDataset,
        )
        assert res.success is False
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(
            ValueError,
            match=r"The validate util method only supports validation for subtypes of the provided data_asset_type",
        ):
            ge.validate(
                dataset,
                expectation_suite=basic_expectation_suite,
                data_asset_class=ge.dataset.PandasDataset,
            )

    elif isinstance(dataset, ge.dataset.SparkDFDataset):
        res = ge.validate(
            dataset, basic_expectation_suite, data_asset_class=ge.dataset.SparkDFDataset
        )
        assert res.success is True
        assert res["statistics"]["evaluated_expectations"] == 4
        with pytest.raises(
            ValueError,
            match=r"The validate util method only supports validation for subtypes of the provided data_asset_type",
        ):
            ge.validate(
                dataset,
                expectation_suite=basic_expectation_suite,
                data_asset_class=ge.dataset.PandasDataset,
            )


def test_validate_using_data_context(
    dataset, data_context_parameterized_expectation_suite
):
    # Before running, the data context should not have compiled parameters
    assert (
        data_context_parameterized_expectation_suite._evaluation_parameter_dependencies_compiled
        is False
    )

    res = ge.validate(
        dataset,
        expectation_suite_name="my_dag_node.default",
        data_context=data_context_parameterized_expectation_suite,
    )

    # Since the handling of evaluation parameters is no longer happening without an action,
    # the context should still be not compiles after validation.
    assert (
        data_context_parameterized_expectation_suite._evaluation_parameter_dependencies_compiled
        is False
    )

    # And, we should have validated the right number of expectations from the context-provided config
    assert res.success is False
    assert res.statistics["evaluated_expectations"] == 2


def test_validate_using_data_context_path(
    dataset, data_context_parameterized_expectation_suite
):
    data_context_path = data_context_parameterized_expectation_suite.root_directory
    res = ge.validate(
        dataset,
        expectation_suite_name="my_dag_node.default",
        data_context=data_context_path,
    )

    # We should have now found the right suite with expectations to evaluate
    assert res.success is False
    assert res["statistics"]["evaluated_expectations"] == 2


def test_validate_invalid_parameters(
    dataset, basic_expectation_suite, data_context_parameterized_expectation_suite
):
    with pytest.raises(
        ValueError,
        match="Either an expectation suite or a DataContext is required for validation.",
    ):
        ge.validate(dataset)


def test_gen_directory_tree_str(tmp_path_factory):
    project_dir = str(tmp_path_factory.mktemp("project_dir"))
    os.mkdir(os.path.join(project_dir, "BBB"))
    with open(os.path.join(project_dir, "BBB", "bbb.txt"), "w") as f:
        f.write("hello")
    with open(os.path.join(project_dir, "BBB", "aaa.txt"), "w") as f:
        f.write("hello")

    os.mkdir(os.path.join(project_dir, "AAA"))

    print(ge.util.gen_directory_tree_str(project_dir))

    # Note: files and directories are sorteds alphabetically, so that this method can be used for testing.
    assert (
        ge.util.gen_directory_tree_str(project_dir)
        == """\
project_dir0/
    AAA/
    BBB/
        aaa.txt
        bbb.txt
"""
    )


def test_nested_update():
    # nested_update is useful for update nested dictionaries (such as batch_kwargs with reader_options as a dictionary)
    batch_kwargs = {
        "path": "/a/path",
        "reader_method": "read_csv",
        "reader_options": {"header": 0},
    }

    nested_update(batch_kwargs, {"reader_options": {"nrows": 1}})

    assert batch_kwargs == {
        "path": "/a/path",
        "reader_method": "read_csv",
        "reader_options": {"header": 0, "nrows": 1},
    }


def test_nested_update_lists():
    # nested_update is useful for update nested dictionaries (such as batch_kwargs with reader_options as a dictionary)
    dependencies = {
        "suite.warning": {"metric.name": ["column=foo"]},
        "suite.failure": {"metric.blarg": [""]},
    }

    new_dependencies = {
        "suite.warning": {
            "metric.other_name": ["column=foo"],
            "metric.name": ["column=bar"],
        }
    }

    nested_update(dependencies, new_dependencies)

    assert dependencies == {
        "suite.warning": {
            "metric.name": ["column=foo", "column=bar"],
            "metric.other_name": ["column=foo"],
        },
        "suite.failure": {"metric.blarg": [""]},
    }


def test_linter_raises_error_on_non_string_input():
    with pytest.raises(TypeError):
        lint_code(99)


def test_linter_changes_dirty_code():
    code = "foo = [1,2,3]"
    assert lint_code(code) == "foo = [1, 2, 3]\n"


def test_linter_leaves_clean_code():
    code = "foo = [1, 2, 3]\n"
    assert lint_code(code) == "foo = [1, 2, 3]\n"


def test_get_currently_executing_function_call_arguments(a=None, *args, **kwargs):
    if a is None:
        test_get_currently_executing_function_call_arguments(0, 1, 2, 3, b=5)
    else:
        assert a == 0
        assert args == (1, 2, 3)
        assert kwargs == {"b": 5}
        params = get_currently_executing_function_call_arguments(
            **{
                "additional_param_0": "xyz_0",
                "additional_param_1": "xyz_1",
                "additional_param_2": "xyz_2",
            }
        )
        assert params["a"] == 0
        assert params["args"] == (1, 2, 3)
        assert params["b"] == 5
        assert params["additional_param_0"] == "xyz_0"
        assert params["additional_param_1"] == "xyz_1"
        assert params["additional_param_2"] == "xyz_2"


def test_filter_properties_dict():
    source_dict: dict = {
        "a": 0,
        "b": None,
        "c": "xyz_0",
        "d": 1,
        "e": 9.8e1,
    }

    d0_begin: dict = copy.deepcopy(source_dict)
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        d0_end: dict = filter_properties_dict(
            properties=d0_begin,
            keep_fields=["c"],
            delete_fields=["a", "e"],
        )
    d0_end: dict = filter_properties_dict(
        properties=d0_begin,
    )
    d0_end_expected = copy.deepcopy(d0_begin)
    d0_end_expected.pop("b")
    assert d0_end == d0_end_expected

    d1_begin: dict = copy.deepcopy(source_dict)
    d1_end: dict = filter_properties_dict(
        properties=d1_begin,
        clean_empty=False,
    )
    d1_end_expected = copy.deepcopy(d1_begin)
    assert d1_end == d1_end_expected

    d2_begin: dict = copy.deepcopy(source_dict)
    d2_end: dict = filter_properties_dict(
        properties=d2_begin,
        keep_fields=["b"],
    )
    d2_end_expected = {"b": None}
    assert d2_end == d2_end_expected

    d3_begin: dict = copy.deepcopy(source_dict)
    d3_end: dict = filter_properties_dict(
        properties=d3_begin,
        keep_fields=["a", "e"],
    )
    d3_end_expected = {"a": 0, "e": 9.8e1}
    assert d3_end == d3_end_expected

    d4_begin: dict = copy.deepcopy(source_dict)
    d4_end: dict = filter_properties_dict(
        properties=d4_begin,
        delete_fields=["a", "e"],
    )
    d4_end_expected = {"c": "xyz_0", "d": 1}
    assert d4_end == d4_end_expected

    d5_begin: dict = copy.deepcopy(source_dict)
    filter_properties_dict(
        properties=d5_begin,
        delete_fields=["a", "e"],
        inplace=True,
    )
    d5_end = copy.deepcopy(d5_begin)
    d5_end_expected = {"c": "xyz_0", "d": 1}
    assert d5_end == d5_end_expected
