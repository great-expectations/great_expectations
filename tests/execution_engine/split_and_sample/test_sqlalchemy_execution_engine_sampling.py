import pytest

from great_expectations.execution_engine.split_and_sample.sqlalchemy_data_sampler import (
    SqlAlchemyDataSampler,
)


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "sampler_method_name",
    [
        pytest.param(sampler_method_name, id=sampler_method_name)
        for sampler_method_name in [
            "sample_using_limit",
            "sample_using_random",
            "sample_using_mod",
            "sample_using_a_list",
            "sample_using_md5",
        ]
    ],
)
def test_get_sampler_method(underscore_prefix: str, sampler_method_name: str):
    """What does this test and why?

    This test is to ensure that the sampler methods are accessible with and without underscores.
    When new sampling methods are added, the parameter list should be updated.
    """
    data_splitter: SqlAlchemyDataSampler = SqlAlchemyDataSampler()

    sampler_method_name_with_prefix = f"{underscore_prefix}{sampler_method_name}"

    assert data_splitter.get_sampler_method(sampler_method_name_with_prefix) == getattr(
        data_splitter, sampler_method_name
    )
