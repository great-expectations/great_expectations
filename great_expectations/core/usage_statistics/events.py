"""Event names for use in sending usage stats events.

This module contains an enum with all event names and methods to retrieve
lists or specific types of events. It also includes a helper utility to
programmatically create new enum values if run as a module.

    Typical usage example:

        In send_usage_message() as an event name:

        handler.send_usage_message(
            event=UsageStatsEvents.EXECUTION_ENGINE_SQLALCHEMY_CONNECT,
            event_payload={
                "anonymized_name": handler.anonymizer.anonymize(self.name),
                "sqlalchemy_dialect": self.engine.name,
            },
            success=True,
        )

        In a test to retrieve all event names:
        all_event_names: List[str] = UsageStatsEvents.get_all_event_names()

        To retrieve CLI event names:
        (
            begin_event_name,
            end_event_name,
        ) = UsageStatsEvents.get_cli_begin_and_end_event_names(
            noun=cli_event_noun,
            verb=ctx.invoked_subcommand,
        )
"""

from __future__ import annotations

import enum
from typing import List, Optional


class UsageStatsEvents(str, enum.Enum):
    """Event names for all Great Expectations usage stats events.

    Note: These should be used in place of strings, or retrieved
        using methods like get_cli_event_name().
    """

    CLI_CHECKPOINT_DELETE = "cli.checkpoint.delete"
    CLI_CHECKPOINT_DELETE_BEGIN = "cli.checkpoint.delete.begin"
    CLI_CHECKPOINT_DELETE_END = "cli.checkpoint.delete.end"
    CLI_CHECKPOINT_LIST = "cli.checkpoint.list"
    CLI_CHECKPOINT_LIST_BEGIN = "cli.checkpoint.list.begin"
    CLI_CHECKPOINT_LIST_END = "cli.checkpoint.list.end"
    CLI_CHECKPOINT_NEW = "cli.checkpoint.new"
    CLI_CHECKPOINT_NEW_BEGIN = "cli.checkpoint.new.begin"
    CLI_CHECKPOINT_NEW_END = "cli.checkpoint.new.end"
    CLI_CHECKPOINT_RUN = "cli.checkpoint.run"
    CLI_CHECKPOINT_RUN_BEGIN = "cli.checkpoint.run.begin"
    CLI_CHECKPOINT_RUN_END = "cli.checkpoint.run.end"
    CLI_CHECKPOINT_SCRIPT = "cli.checkpoint.script"
    CLI_CHECKPOINT_SCRIPT_BEGIN = "cli.checkpoint.script.begin"
    CLI_CHECKPOINT_SCRIPT_END = "cli.checkpoint.script.end"
    CLI_DATASOURCE_DELETE = "cli.datasource.delete"
    CLI_DATASOURCE_DELETE_BEGIN = "cli.datasource.delete.begin"
    CLI_DATASOURCE_DELETE_END = "cli.datasource.delete.end"
    CLI_DATASOURCE_LIST = "cli.datasource.list"
    CLI_DATASOURCE_LIST_BEGIN = "cli.datasource.list.begin"
    CLI_DATASOURCE_LIST_END = "cli.datasource.list.end"
    CLI_DATASOURCE_NEW = "cli.datasource.new"
    CLI_DATASOURCE_NEW_BEGIN = "cli.datasource.new.begin"
    CLI_DATASOURCE_NEW_END = "cli.datasource.new.end"
    CLI_DATASOURCE_PROFILE = "cli.datasource.profile"
    CLI_DOCS_BUILD = "cli.docs.build"
    CLI_DOCS_BUILD_BEGIN = "cli.docs.build.begin"
    CLI_DOCS_BUILD_END = "cli.docs.build.end"
    CLI_DOCS_CLEAN = "cli.docs.clean"
    CLI_DOCS_CLEAN_BEGIN = "cli.docs.clean.begin"
    CLI_DOCS_CLEAN_END = "cli.docs.clean.end"
    CLI_DOCS_LIST = "cli.docs.list"
    CLI_DOCS_LIST_BEGIN = "cli.docs.list.begin"
    CLI_DOCS_LIST_END = "cli.docs.list.end"
    CLI_INIT_CREATE = "cli.init.create"
    CLI_NEW_DS_CHOICE = "cli.new_ds_choice"
    CLI_PROJECT_CHECK_CONFIG = "cli.project.check_config"
    CLI_PROJECT_UPGRADE_BEGIN = "cli.project.upgrade.begin"
    CLI_PROJECT_UPGRADE_END = "cli.project.upgrade.end"
    CLI_STORE_LIST = "cli.store.list"
    CLI_STORE_LIST_BEGIN = "cli.store.list.begin"
    CLI_STORE_LIST_END = "cli.store.list.end"
    CLI_SUITE_DELETE = "cli.suite.delete"
    CLI_SUITE_DELETE_BEGIN = "cli.suite.delete.begin"
    CLI_SUITE_DELETE_END = "cli.suite.delete.end"
    CLI_SUITE_DEMO = "cli.suite.demo"
    CLI_SUITE_DEMO_BEGIN = "cli.suite.demo.begin"
    CLI_SUITE_DEMO_END = "cli.suite.demo.end"
    CLI_SUITE_EDIT = "cli.suite.edit"
    CLI_SUITE_EDIT_BEGIN = "cli.suite.edit.begin"
    CLI_SUITE_EDIT_END = "cli.suite.edit.end"
    CLI_SUITE_LIST = "cli.suite.list"
    CLI_SUITE_LIST_BEGIN = "cli.suite.list.begin"
    CLI_SUITE_LIST_END = "cli.suite.list.end"
    CLI_SUITE_NEW = "cli.suite.new"
    CLI_SUITE_NEW_BEGIN = "cli.suite.new.begin"
    CLI_SUITE_NEW_END = "cli.suite.new.end"
    CLI_SUITE_SCAFFOLD = "cli.suite.scaffold"
    CLI_VALIDATION_OPERATOR_LIST = "cli.validation_operator.list"
    CLI_VALIDATION_OPERATOR_RUN = "cli.validation_operator.run"
    DATA_ASSET_VALIDATE = "data_asset.validate"
    DATA_CONTEXT___INIT__ = "data_context.__init__"
    DATA_CONTEXT_ADD_DATASOURCE = "data_context.add_datasource"
    DATA_CONTEXT_GET_BATCH_LIST = "data_context.get_batch_list"
    DATA_CONTEXT_BUILD_DATA_DOCS = "data_context.build_data_docs"
    DATA_CONTEXT_OPEN_DATA_DOCS = "data_context.open_data_docs"
    DATA_CONTEXT_RUN_CHECKPOINT = "data_context.run_checkpoint"
    DATA_CONTEXT_SAVE_EXPECTATION_SUITE = "data_context.save_expectation_suite"
    DATA_CONTEXT_TEST_YAML_CONFIG = "data_context.test_yaml_config"
    DATA_CONTEXT_RUN_VALIDATION_OPERATOR = "data_context.run_validation_operator"
    DATASOURCE_SQLALCHEMY_CONNECT = "datasource.sqlalchemy.connect"
    EXECUTION_ENGINE_SQLALCHEMY_CONNECT = "execution_engine.sqlalchemy.connect"
    CHECKPOINT_RUN = "checkpoint.run"
    EXPECTATION_SUITE_ADD_EXPECTATION = "expectation_suite.add_expectation"
    LEGACY_PROFILER_BUILD_SUITE = "legacy_profiler.build_suite"
    RULE_BASED_PROFILER_RUN = "profiler.run"
    RULE_BASED_PROFILER_RESULT_GET_EXPECTATION_SUITE = (
        "profiler.result.get_expectation_suite"
    )
    DATA_CONTEXT_RUN_RULE_BASED_PROFILER_ON_DATA = "data_context.run_profiler_on_data"
    DATA_CONTEXT_RUN_RULE_BASED_PROFILER_WITH_DYNAMIC_ARGUMENTS = (
        "data_context.run_profiler_with_dynamic_arguments"
    )
    DATA_ASSISTANT_RESULT_GET_EXPECTATION_SUITE = (
        "data_assistant.result.get_expectation_suite"
    )
    CLOUD_MIGRATE = "cloud_migrator.migrate"

    @classmethod
    def get_all_event_names(cls):
        """Get event names for all usage stats events."""
        return [event_name.value for event_name in cls]

    @classmethod
    def get_all_event_names_no_begin_end_events(cls):
        """Get event names for all usage stats events that don't end with BEGIN or END."""
        return [
            event_name.value
            for event_name in cls
            if not (event_name.name.endswith("BEGIN"))
            | (event_name.name.endswith("END"))
        ]

    @classmethod
    def get_cli_event_name(
        cls, noun: str, verb: str, other_items: Optional[List[str]] = None
    ) -> str:
        """Return the appropriate event name from the appropriate enum based on inputs.

        Args:
            noun: CLI noun as a string
            verb: CLI verb as a string
            other_items: List of optional additional items e.g. ["begin"] or ["end"]

        Returns:
            String from enum value for event
        """
        if other_items is not None:
            other_items_str: str = (
                f"_{'_'.join([item.upper() for item in other_items])}"
            )
        else:
            other_items_str = ""
        enum_name: str = f"CLI_{noun.upper()}_{verb.upper()}{other_items_str}"

        return getattr(cls, enum_name).value

    @classmethod
    def get_cli_begin_and_end_event_names(cls, noun: str, verb: str) -> List[str]:
        """Return the appropriate list of event names from the appropriate enums based on inputs.

        Args:
            noun: CLI noun as a string
            verb: CLI verb as a string

        Returns:
            List of strings from enum value for event
        """
        other_items_list: List[List[str]] = [["begin"], ["end"]]
        event_names: List[str] = [
            cls.get_cli_event_name(noun, verb, other_items=other_items)
            for other_items in other_items_list
        ]
        return event_names


if __name__ == "__main__":
    """You can use this optional utility to help generate enum attributes."""
    EVENT_LIST = [
        "cli.checkpoint.delete",
        "cli.checkpoint.list",
        "cli.checkpoint.new",
        "cli.checkpoint.run",
        "cli.checkpoint.script",
        "cli.datasource.delete",
        "cli.datasource.list",
        "cli.datasource.new",
        "cli.datasource.profile",
        "cli.docs.build",
        "cli.docs.clean",
        "cli.docs.list",
        "cli.init.create",
        "cli.new_ds_choice",
        "cli.project.check_config",
        "cli.store.list",
        "cli.suite.delete",
        "cli.suite.demo",
        "cli.suite.edit",
        "cli.suite.list",
        "cli.suite.new",
        "cli.suite.scaffold",
        "cli.validation_operator.list",
        "cli.validation_operator.run",
        "data_asset.validate",
        "data_context.__init__",
        "data_context.add_datasource",
        "data_context.get_batch_list",
        "data_context.build_data_docs",
        "data_context.open_data_docs",
        "data_context.run_checkpoint",
        "data_context.save_expectation_suite",
        "data_context.test_yaml_config",
        "data_context.run_validation_operator",
        "datasource.sqlalchemy.connect",
        "execution_engine.sqlalchemy.connect",
        "checkpoint.run",
        "expectation_suite.add_expectation",
        "legacy_profiler.build_suite",
        "profiler.run",
        "data_context.run_profiler_on_data",
        "data_context.run_profiler_with_dynamic_arguments",
    ]
    events = []
    for event in EVENT_LIST:
        if event.startswith("cli."):
            for suffix in ["", ".begin", ".end"]:
                event_with_suffix = f"{event}{suffix}"
                assignment_string = f'{event_with_suffix.replace(".", "_").upper()} = "{event_with_suffix}"'
                events.append(assignment_string)
        else:
            assignment_string = f'{event.replace(".", "_").upper()} = "{event}"'
            events.append(assignment_string)

    for e in events:
        print(e)
