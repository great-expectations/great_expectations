from great_expectations.core import ExpectationSuite


def test_fixture_generation(alice_columnar_table_single_batch):
    assert (
        alice_columnar_table_single_batch["sample_data_relative_path"]
        == "alice_columnar_table_single_batch_data.csv"
    )
    assert len(alice_columnar_table_single_batch["profiler_configs"]) > 0
    for profiler_config in alice_columnar_table_single_batch["profiler_configs"]:
        assert len(profiler_config) > 0
        assert isinstance(profiler_config, str)
    assert isinstance(
        alice_columnar_table_single_batch["expected_expectation_suite"],
        ExpectationSuite,
    )
    # assert (
    #     len(
    #         alice_columnar_table_single_batch["expected_expectation_suite"].expectations
    #     )
    #     == 18
    # )
