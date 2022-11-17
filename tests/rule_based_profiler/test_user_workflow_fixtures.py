import pytest

from great_expectations.core import ExpectationSuite


@pytest.mark.integration
def test_alice_fixture_generation(
    alice_columnar_table_single_batch,
):
    assert (
        alice_columnar_table_single_batch["sample_data_relative_path"]
        == "alice_columnar_table_single_batch_data.csv"
    )
    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]
    assert len(profiler_config) > 0
    assert isinstance(profiler_config, str)
    assert isinstance(
        alice_columnar_table_single_batch["expected_expectation_suite"],
        ExpectationSuite,
    )
    assert (
        len(
            alice_columnar_table_single_batch["expected_expectation_suite"].expectations
        )
        == 24
    )


@pytest.mark.integration
def test_bobby_fixture_generation(
    bobby_columnar_table_multi_batch,
):
    profiler_config: str = bobby_columnar_table_multi_batch["profiler_config"]
    assert len(profiler_config) > 0
    assert isinstance(profiler_config, str)
    assert isinstance(
        bobby_columnar_table_multi_batch["test_configuration_quantiles_estimator"][
            "expected_expectation_suite"
        ],
        ExpectationSuite,
    )
    assert (
        len(
            bobby_columnar_table_multi_batch["test_configuration_quantiles_estimator"][
                "expected_expectation_suite"
            ].expectations
        )
        == 39
    )
