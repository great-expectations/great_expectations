from copy import copy, deepcopy

import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite


@pytest.fixture
def exp1():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp2():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp3():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp4():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )

@pytest.fixture
def exp5():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
        meta={"notes": "This is an expectation."},
    )

@pytest.fixture
def exp6():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2]},
        meta={"notes": "This is an expectation."},
    )

@pytest.fixture
def exp7():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3, 4]},
        meta={"notes": "This is an expectation."},
    )

@pytest.fixture
def exp8():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3]},
        meta={"notes": "This is an expectation."},
    )

@pytest.fixture
def column_pair_expectation():
    return ExpectationConfiguration(
        expectation_type="expect_column_pair_values_to_be_in_set",
        kwargs={
            "column_A": "1",
            "column_B": "b",
            "value_set": [(1, 1), (2, 2)],
            "result_format": "BASIC",
        },
    )


@pytest.fixture
def table_exp1():
    return ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_ordered_list",
        kwargs={"value": ["a", "b", "c"]},
    )


@pytest.fixture
def table_exp2():
    return ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 0, "max_value": 1},
    )


@pytest.fixture
def table_exp3():
    return ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal", kwargs={"value": 1}
    )


@pytest.fixture
def empty_suite():
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def suite_with_table_and_column_expectations(
    exp1, exp2, exp3, exp4, column_pair_expectation, table_exp1, table_exp2, table_exp3
):
    suite = ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[
            exp1,
            exp2,
            exp3,
            exp4,
            column_pair_expectation,
            table_exp1,
            table_exp2,
            table_exp3,
        ],
        meta={"notes": "This is an expectation suite."},
    )
    assert suite.expectations == [
        exp1,
        exp2,
        exp3,
        exp4,
        column_pair_expectation,
        table_exp1,
        table_exp2,
        table_exp3,
    ]
    return suite


@pytest.fixture
def baseline_suite(exp1, exp2):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1, exp2],
        meta={"notes": "This is an expectation suite."},
    )

@pytest.fixture
def domain_success_runtime_suite(exp1, exp2, exp3, exp4, exp5):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1, exp2, exp3, exp4, exp5],
        meta={"notes": "This is an expectation suite."},
    )

@pytest.fixture
def identical_suite(exp1, exp3):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1, exp3],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def equivalent_suite(exp1, exp3):
    return ExpectationSuite(
        expectation_suite_name="danger",
        expectations=[exp1, exp3],
        meta={
            "notes": "This is another expectation suite, with a different name and meta"
        },
    )


@pytest.fixture
def different_suite(exp1, exp4):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1, exp4],
        meta={"notes": "This is an expectation suite."},
    )

@pytest.fixture
def single_expectation_suite(exp1):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1],
        meta={"notes": "This is an expectation suite."},
    )


def test_expectation_suite_equality(baseline_suite, identical_suite, equivalent_suite):
    """Equality should depend on all defined properties of a configuration object, but not on whether the *instances*
    are the same."""
    assert baseline_suite is baseline_suite  # no difference
    assert (
        baseline_suite is not identical_suite
    )  # different instances, but same content
    assert baseline_suite == identical_suite  # different instances, but same content
    assert not (baseline_suite != identical_suite)  # ne works properly
    assert not (baseline_suite == equivalent_suite)  # different meta
    assert baseline_suite != equivalent_suite  # ne works properly


def test_expectation_suite_equivalence(
    baseline_suite, identical_suite, equivalent_suite, different_suite, single_expectation_suite
):
    """Equivalence should depend only on properties that affect the result of the expectation."""
    assert baseline_suite.isEquivalentTo(baseline_suite)  # no difference
    assert baseline_suite.isEquivalentTo(identical_suite)
    assert baseline_suite.isEquivalentTo(equivalent_suite)  # different meta
    assert not baseline_suite.isEquivalentTo(
        different_suite
    )  # different value_set in one expectation
    assert not single_expectation_suite.isEquivalentTo(baseline_suite)

def test_expectation_suite_dictionary_equivalence(baseline_suite):
    assert (
        baseline_suite.isEquivalentTo(
            {
                "expectation_suite_name": "warning",
                "expectations": [
                    {
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {
                            "column": "a",
                            "value_set": [1, 2, 3],
                            "result_format": "BASIC",
                        },
                        "meta": {"notes": "This is an expectation."},
                    },
                    {
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {
                            "column": "b",
                            "value_set": [-1, -2, -3],
                            "result_format": "BASIC",
                        },
                        "meta": {"notes": "This is an expectation."},
                    },
                ],
                "meta": {"notes": "This is an expectation suite."},
            }
        )
        is True
    )

    assert (
        baseline_suite.isEquivalentTo(
            {
                "expectation_suite_name": "warning",
                "expectations": [
                    {
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {
                            "column": "a",
                            "value_set": [-1, 2, 3],  # One value changed here
                            "result_format": "BASIC",
                        },
                        "meta": {"notes": "This is an expectation."},
                    },
                    {
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {
                            "column": "b",
                            "value_set": [-1, -2, -3],
                            "result_format": "BASIC",
                        },
                        "meta": {"notes": "This is an expectation."},
                    },
                ],
                "meta": {"notes": "This is an expectation suite."},
            }
        )
        is False
    )


def test_expectation_suite_copy(baseline_suite):
    suite_copy = copy(baseline_suite)
    assert suite_copy == baseline_suite
    suite_copy.data_asset_type = "blarg!"
    assert (
        baseline_suite.data_asset_type != "blarg"
    )  # copy on primitive properties shouldn't propagate
    suite_copy.expectations[0].meta["notes"] = "a different note"
    assert (
        baseline_suite.expectations[0].meta["notes"] == "a different note"
    )  # copy on deep attributes does propagate


def test_expectation_suite_deepcopy(baseline_suite):
    suite_deepcopy = deepcopy(baseline_suite)
    assert suite_deepcopy == baseline_suite
    suite_deepcopy.data_asset_type = "blarg!"
    assert (
        baseline_suite.data_asset_type != "blarg"
    )  # copy on primitive properties shouldn't propagate
    suite_deepcopy.expectations[0].meta["notes"] = "a different note"
    # deepcopy on deep attributes does not propagate
    assert baseline_suite.expectations[0].meta["notes"] == "This is an expectation."


def test_suite_without_metadata_includes_ge_version_metadata_if_none_is_provided():
    suite = ExpectationSuite("foo")
    assert "great_expectations.__version__" in suite.meta.keys()


def test_suite_does_not_overwrite_existing_version_metadata():
    suite = ExpectationSuite("foo", meta={"great_expectations.__version__": "0.0.0"})
    assert "great_expectations.__version__" in suite.meta.keys()
    assert suite.meta["great_expectations.__version__"] == "0.0.0"


def test_suite_with_metadata_includes_ge_version_metadata(baseline_suite):
    assert "great_expectations.__version__" in baseline_suite.meta.keys()


def test_add_citation(baseline_suite):
    assert (
        "citations" not in baseline_suite.meta
        or len(baseline_suite.meta["citations"]) == 0
    )
    baseline_suite.add_citation("hello!")
    assert baseline_suite.meta["citations"][0].get("comment") == "hello!"


def test_get_citations_with_no_citations(baseline_suite):
    assert "citations" not in baseline_suite.meta
    assert baseline_suite.get_citations() == []


def test_get_citations_not_sorted(baseline_suite):
    assert "citations" not in baseline_suite.meta
    baseline_suite.add_citation("first", citation_date="2000-01-01")
    baseline_suite.add_citation("third", citation_date="2000-01-03")
    baseline_suite.add_citation("second", citation_date="2000-01-02")
    assert baseline_suite.get_citations(sort=False) == [
        {
            "batch_kwargs": None,
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2000-01-01",
            "comment": "first",
        },
        {
            "batch_kwargs": None,
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2000-01-03",
            "comment": "third",
        },
        {
            "batch_kwargs": None,
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2000-01-02",
            "comment": "second",
        },
    ]


def test_get_citations_sorted(baseline_suite):
    assert "citations" not in baseline_suite.meta
    baseline_suite.add_citation("first", citation_date="2000-01-01")
    baseline_suite.add_citation("third", citation_date="2000-01-03")
    baseline_suite.add_citation("second", citation_date="2000-01-02")
    assert baseline_suite.get_citations(sort=True) == [
        {
            "batch_kwargs": None,
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2000-01-01",
            "comment": "first",
        },
        {
            "batch_kwargs": None,
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2000-01-02",
            "comment": "second",
        },
        {
            "batch_kwargs": None,
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2000-01-03",
            "comment": "third",
        },
    ]


def test_get_citations_with_multiple_citations_containing_batch_kwargs(baseline_suite):
    assert "citations" not in baseline_suite.meta
    baseline_suite.add_citation(
        "first", batch_kwargs={"path": "first"}, citation_date="2000-01-01"
    )
    baseline_suite.add_citation(
        "second", batch_kwargs={"path": "second"}, citation_date="2001-01-01"
    )
    baseline_suite.add_citation("third", citation_date="2002-01-01")

    obs = baseline_suite.get_citations(sort=True, require_batch_kwargs=True)
    assert obs == [
        {
            "batch_kwargs": {"path": "first"},
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2000-01-01",
            "comment": "first",
        },
        {
            "batch_kwargs": {"path": "second"},
            "batch_markers": None,
            "batch_parameters": None,
            "citation_date": "2001-01-01",
            "comment": "second",
        },
    ]


def test_get_table_expectations_returns_empty_list_on_empty_suite(empty_suite):
    assert empty_suite.get_table_expectations() == []


def test_get_table_expectations_returns_empty_list_on_suite_without_any(baseline_suite):
    assert baseline_suite.get_table_expectations() == []


def test_get_table_expectations(
    suite_with_table_and_column_expectations, table_exp1, table_exp2, table_exp3
):
    obs = suite_with_table_and_column_expectations.get_table_expectations()
    assert obs == [table_exp1, table_exp2, table_exp3]


def test_get_column_expectations_returns_empty_list_on_empty_suite(empty_suite):
    assert empty_suite.get_column_expectations() == []


def test_get_column_expectations(
    suite_with_table_and_column_expectations, exp1, exp2, exp3, exp4
):
    obs = suite_with_table_and_column_expectations.get_column_expectations()
    assert obs == [exp1, exp2, exp3, exp4]


def test_find_expectation_indexes(
    exp1, exp2, exp3, exp4, exp5, domain_success_runtime_suite
):
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "domain") == [1, 2, 3, 4]
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "success") == [3, 4]
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "runtime") == [3]


def test_remove_expectations(
  exp1, exp2, exp3, exp4, exp5, single_expectation_suite, domain_success_runtime_suite
):
    domain_success_runtime_suite.remove_expectation(exp5, match_type="runtime") # remove one matching expectation

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp5, match_type="runtime")
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "domain") == [1, 2, 3]
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "success") == [3]

    domain_success_runtime_suite.remove_expectation(exp4, match_type="domain") # remove 3 matching expectations

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp2, match_type="runtime")
    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp3, match_type="runtime")

    assert domain_success_runtime_suite.find_expectation_indexes(exp1, match_type="domain") == [0]
    assert domain_success_runtime_suite.isEquivalentTo(single_expectation_suite)


def test_patch_expectation_replace(
    exp5, exp6, domain_success_runtime_suite
):
    assert domain_success_runtime_suite.expectations[4] is exp5

    assert not domain_success_runtime_suite.expectations[4].isEquivalentTo(exp6, match_type="success")
    domain_success_runtime_suite.patch(
        expectation_type="expect_column_values_to_be_in_set",
        match_kwargs=exp5.kwargs,
        op="replace",
        path="/value_set",
        value=[1, 2],
        match_type="runtime")
    assert domain_success_runtime_suite.expectations[4].isEquivalentTo(exp6, match_type="success")


def test_patch_expectation_add(
    exp5, exp7, domain_success_runtime_suite
):
    assert domain_success_runtime_suite.expectations[4] is exp5

    assert not domain_success_runtime_suite.expectations[4].isEquivalentTo(exp7, match_type="success")
    domain_success_runtime_suite.patch(
        expectation_type="expect_column_values_to_be_in_set",
        match_kwargs=exp5.kwargs,
        op="add",
        path="/value_set/-",
        value=4,
        match_type="runtime")
    assert domain_success_runtime_suite.expectations[4].isEquivalentTo(exp7, match_type="success")


def test_patch_expectation_remove(
    exp5, exp8, domain_success_runtime_suite
):
    assert domain_success_runtime_suite.expectations[4] is exp5

    assert not domain_success_runtime_suite.expectations[4].isEquivalentTo(exp8, match_type="runtime")
    domain_success_runtime_suite.patch(
        expectation_type="expect_column_values_to_be_in_set",
        match_kwargs=exp5.kwargs,
        op="remove",
        path="/result_format",
        value=None,
        match_type="runtime")
    assert domain_success_runtime_suite.expectations[4].isEquivalentTo(exp8, match_type="runtime")


def test_add_or_replace(
    exp1, exp2, exp4, single_expectation_suite, baseline_suite, different_suite, domain_success_runtime_suite
):
    assert not single_expectation_suite.isEquivalentTo(baseline_suite)
    single_expectation_suite.add_or_replace(exp2, match_type="runtime")
    assert single_expectation_suite.isEquivalentTo(baseline_suite)

    assert not single_expectation_suite.isEquivalentTo(different_suite)
    single_expectation_suite.add_or_replace(exp4, match_type="domain")
    assert single_expectation_suite.isEquivalentTo(different_suite)

    with pytest.raises(ValueError):
        domain_success_runtime_suite.add_or_replace(exp2, match_type="success")
