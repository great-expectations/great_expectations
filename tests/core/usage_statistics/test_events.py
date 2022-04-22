from great_expectations.core.usage_statistics.events import UsageStatsEvents


def test_get_cli_event_name():
    assert (
        UsageStatsEvents.get_cli_event_name("checkpoint", "delete", ["begin"])
        == "cli.checkpoint.delete.begin"
    )
