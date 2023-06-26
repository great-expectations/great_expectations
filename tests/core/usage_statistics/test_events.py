from great_expectations.core.usage_statistics.events import UsageStatsEvents


def test_get_cli_event_name():
    assert (
        UsageStatsEvents.get_cli_event_name("checkpoint", "delete", ["begin"])
        == "cli.checkpoint.delete.begin"
    )


def test_get_cli_begin_and_end_event_names():
    assert UsageStatsEvents.get_cli_begin_and_end_event_names("datasource", "new") == [
        "cli.datasource.new.begin",
        "cli.datasource.new.end",
    ]
