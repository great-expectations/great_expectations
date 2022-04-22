from __future__ import annotations

import enum
from typing import List, Optional


class UsageStatsEvents(enum.Enum):

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
    CLI_DATASOURCE_PROFILE_BEGIN = "cli.datasource.profile.begin"
    CLI_DATASOURCE_PROFILE_END = "cli.datasource.profile.end"
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
    CLI_INIT_CREATE_BEGIN = "cli.init.create.begin"
    CLI_INIT_CREATE_END = "cli.init.create.end"
    CLI_NEW_DS_CHOICE = "cli.new_ds_choice"
    CLI_NEW_DS_CHOICE_BEGIN = "cli.new_ds_choice.begin"
    CLI_NEW_DS_CHOICE_END = "cli.new_ds_choice.end"
    CLI_PROJECT_CHECK_CONFIG = "cli.project.check_config"
    CLI_PROJECT_CHECK_CONFIG_BEGIN = "cli.project.check_config.begin"
    CLI_PROJECT_CHECK_CONFIG_END = "cli.project.check_config.end"
    CLI_PROJECT_UPGRADE = "cli.project.upgrade"
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
    CLI_SUITE_SCAFFOLD_BEGIN = "cli.suite.scaffold.begin"
    CLI_SUITE_SCAFFOLD_END = "cli.suite.scaffold.end"
    CLI_VALIDATION_OPERATOR_LIST = "cli.validation_operator.list"
    CLI_VALIDATION_OPERATOR_LIST_BEGIN = "cli.validation_operator.list.begin"
    CLI_VALIDATION_OPERATOR_LIST_END = "cli.validation_operator.list.end"
    CLI_VALIDATION_OPERATOR_RUN = "cli.validation_operator.run"
    CLI_VALIDATION_OPERATOR_RUN_BEGIN = "cli.validation_operator.run.begin"
    CLI_VALIDATION_OPERATOR_RUN_END = "cli.validation_operator.run.end"
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
    PROFILER_RUN = "profiler.run"
    DATA_CONTEXT_RUN_PROFILER_ON_DATA = "data_context.run_profiler_on_data"
    DATA_CONTEXT_RUN_PROFILER_WITH_DYNAMIC_ARGUMENTS = (
        "data_context.run_profiler_with_dynamic_arguments"
    )

    @classmethod
    def get_cli_event_name(
        cls, noun: str, verb: str, other_items: Optional[List[str]] = None
    ) -> UsageStatsEvents:
        """Return the appropriate enum based on inputs.

        Args:
            noun: CLI noun as a string
            verb: CLI verb as a string
            other_items: List of optional additional items e.g. [".begin"] or [".end"]

        Returns:
            String
        """
        if other_items is not None:

            other_items_str: str = (
                f"_{'_'.join([item.upper() for item in other_items])}"
            )
        else:
            other_items_str: str = ""
        enum_name: str = f"CLI_{noun.upper()}_{verb.upper()}{other_items_str}"

        return getattr(cls, enum_name)

    @classmethod
    def get_all_event_names(cls):
        return [event_name.value for event_name in cls]

    @classmethod
    def get_all_event_names_no_begin_end_events(cls):
        return [
            event_name.value
            for event_name in cls
            if not (event_name.name.endswith("BEGIN"))
            | (event_name.name.endswith("END"))
        ]

    @classmethod
    def get_all_events(cls):
        return [event_name for event_name in cls]


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
        "cli.project.upgrade",
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
