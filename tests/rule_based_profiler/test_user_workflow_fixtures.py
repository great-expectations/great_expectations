from great_expectations.core import ExpectationSuite
from tests.integration.profiling.rule_based_profilers.conftest import (
    alice_columnar_table_single_batch,
    bobby_columnar_table_multi_batch,
)


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
        == 21
    )

def test_bobby_fixture_generation(
    bobby_columnar_table_multi_batch,
):
    profiler_config: str = bobby_columnar_table_multi_batch["profiler_config"]
    assert len(profiler_config) > 0
    assert isinstance(profiler_config, str)
    assert isinstance(
        bobby_columnar_table_multi_batch["test_configuration_oneshot_sampling_method"][
            "expected_expectation_suite"
        ],
        ExpectationSuite,
    )
    assert (
        len(
            bobby_columnar_table_multi_batch[
                "test_configuration_oneshot_sampling_method"
            ]["expected_expectation_suite"].expectations
        )
        == 33
    )
