import datetime
import json
import os
from typing import List, Optional, Tuple, Union

from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint, LegacyCheckpoint
from great_expectations.cli.upgrade_helpers.base_upgrade_helper import BaseUpgradeHelper
from great_expectations.data_context.store.checkpoint_store import CheckpointStore
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
)


class UpgradeHelperV13(BaseUpgradeHelper):
    def __init__(
        self,
        data_context: Optional[DataContext] = None,
        context_root_dir: Optional[str] = None,
        update_version: bool = False,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (
            data_context or context_root_dir
        ), "Please provide a data_context object or a context_root_dir."
        self.data_context = data_context or DataContext(
            context_root_dir=context_root_dir
        )
        self.upgrade_log = {
            "update_version": update_version,
            "skipped_checkpoint_store_upgrade": False,
            "added_checkpoint_store": {},
            "skipped_checkpoint_config_upgrade": False,
            "skipped_datasources_upgrade": False,
            "skipped_validation_operators_upgrade": False,
        }
        self.upgrade_checklist = {
            "automatic": {"stores": {}, "store_names": {}},
            "manual": {
                "checkpoints": {},
                "datasources": {},
                "validation_operators": {},
            },
        }
        self._generate_upgrade_checklist()

    def _generate_upgrade_checklist(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._process_checkpoint_store_for_checklist()
        self._process_checkpoint_config_for_checklist()
        self._process_datasources_for_checklist()
        self._process_validation_operators_for_checklist()

    def _process_checkpoint_store_for_checklist(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if CheckpointStore.default_checkpoints_exist(
            directory_path=self.data_context.root_directory
        ):
            config_commented_map: CommentedMap = (
                self.data_context.get_config().commented_map
            )
            checkpoint_store_name: Optional[str] = config_commented_map.get(
                "checkpoint_store_name"
            )
            stores: dict = config_commented_map["stores"]
            if checkpoint_store_name:
                if stores.get(checkpoint_store_name):
                    self.upgrade_log["skipped_checkpoint_store_upgrade"] = True
                else:
                    self.upgrade_checklist["automatic"]["stores"] = {
                        checkpoint_store_name: DataContextConfigDefaults.DEFAULT_STORES.value[
                            DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value
                        ]
                    }
            else:
                checkpoint_store_name = (
                    DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value
                )
                self.upgrade_checklist["automatic"]["store_names"][
                    "checkpoint_store_name"
                ] = checkpoint_store_name
                if not stores.get(checkpoint_store_name):
                    self.upgrade_checklist["automatic"]["stores"] = {
                        checkpoint_store_name: DataContextConfigDefaults.DEFAULT_STORES.value[
                            checkpoint_store_name
                        ]
                    }
        else:
            self.upgrade_log["skipped_checkpoint_store_upgrade"] = True

    def _process_checkpoint_config_for_checklist(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        legacy_checkpoints: List[Union[(Checkpoint, LegacyCheckpoint)]] = []
        checkpoint: Union[(Checkpoint, LegacyCheckpoint)]
        checkpoint_name: str
        try:
            for checkpoint_name in sorted(self.data_context.list_checkpoints()):
                checkpoint = self.data_context.get_checkpoint(name=checkpoint_name)
                if checkpoint.config_version is None:
                    legacy_checkpoints.append(checkpoint)
            self.upgrade_checklist["manual"]["checkpoints"] = {
                checkpoint.name: checkpoint.get_config()
                for checkpoint in legacy_checkpoints
            }
            if len(self.upgrade_checklist["manual"]["checkpoints"]) == 0:
                self.upgrade_log["skipped_checkpoint_config_upgrade"] = True
        except ge_exceptions.InvalidTopLevelConfigKeyError:
            self.upgrade_log["skipped_checkpoint_config_upgrade"] = True

    def _process_datasources_for_checklist(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        config_commented_map: CommentedMap = (
            self.data_context.get_config().commented_map
        )
        datasources: dict = config_commented_map.get("datasources") or {}
        datasource_name: str
        datasource_config: dict
        self.upgrade_checklist["manual"]["datasources"] = {
            datasource_name: datasource_config
            for (datasource_name, datasource_config) in datasources.items()
            if (
                (
                    set(datasource_config.keys())
                    & {"execution_engine", "data_connectors", "introspection", "tables"}
                )
                == set()
            )
        }
        if len(self.upgrade_checklist["manual"]["datasources"]) == 0:
            self.upgrade_log["skipped_datasources_upgrade"] = True

    def _process_validation_operators_for_checklist(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        config_commented_map: CommentedMap = (
            self.data_context.get_config().commented_map
        )
        validation_operators: dict = (
            config_commented_map.get("validation_operators") or {}
        )
        validation_operator_name: str
        validation_operator_config: dict
        self.upgrade_checklist["manual"]["validation_operators"] = {
            validation_operator_name: validation_operator_config
            for (
                validation_operator_name,
                validation_operator_config,
            ) in validation_operators.items()
            if validation_operator_config
        }
        if len(self.upgrade_checklist["manual"]["validation_operators"]) == 0:
            self.upgrade_log["skipped_validation_operators_upgrade"] = True

    def manual_steps_required(self) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return any(
            [
                (len(manual_upgrade_item.keys()) > 0)
                for manual_upgrade_item in self.upgrade_checklist["manual"].values()
            ]
        )

    def get_upgrade_overview(self) -> Tuple[(str, bool)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        manual_steps_required = self.manual_steps_required()
        increment_version = self.upgrade_log["update_version"]
        confirmation_required = increment_version and (
            not self.upgrade_log["skipped_checkpoint_store_upgrade"]
        )
        upgrade_overview = "<cyan>++====================================++\n|| UpgradeHelperV13: Upgrade Overview ||\n++====================================++</cyan>\n\n"
        if increment_version:
            upgrade_overview += (
                (
                    "UpgradeHelperV13 will upgrade your project to be compatible with Great Expectations V3 API.\n"
                    + self._upgrade_overview_common_content(
                        manual_steps_required=manual_steps_required
                    )
                )
                + "\n<cyan>Upgrade Confirmation\n=====================\n</cyan>\nPlease consult the V3 API migration guide for instructions on how to complete any required manual steps or to learn more about the automated upgrade process:\n\n    <cyan>https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api</cyan>\n"
            )
            if confirmation_required:
                upgrade_overview += (
                    "\nWould you like to proceed with the project upgrade?"
                )
        else:
            upgrade_overview += (
                "Your project needs to be upgraded in order to be compatible with Great Expectations V3 API.\n"
                + self._upgrade_overview_common_content(
                    manual_steps_required=manual_steps_required
                )
            )
        return (upgrade_overview, confirmation_required)

    def _upgrade_overview_common_content(self, manual_steps_required: bool) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        stores_upgrade_checklist = list(
            self.upgrade_checklist["automatic"]["stores"].keys()
        )
        store_names_upgrade_checklist = list(
            self.upgrade_checklist["automatic"]["store_names"].keys()
        )
        checkpoint_config_upgrade_checklist = self.upgrade_checklist["manual"][
            "checkpoints"
        ]
        datasources_upgrade_checklist = self.upgrade_checklist["manual"]["datasources"]
        upgrade_overview = ""
        if (
            self.upgrade_log["skipped_checkpoint_store_upgrade"]
            and self.upgrade_log["skipped_checkpoint_config_upgrade"]
            and self.upgrade_log["skipped_datasources_upgrade"]
            and self.upgrade_log["skipped_validation_operators_upgrade"]
        ):
            upgrade_overview += "\n<green>Good news! No special upgrade steps are required to bring your project up to date.\nThe Upgrade Helper will simply increment the config_version of your great_expectations.yml for you.\n</green>\n"
        else:
            upgrade_overview += "\n<red>**WARNING**: Before proceeding, please make sure you have appropriate backups of your project.</red>\n"
            if not self.upgrade_log["skipped_checkpoint_store_upgrade"]:
                if stores_upgrade_checklist or store_names_upgrade_checklist:
                    upgrade_overview += "\n<cyan>Automated Steps\n================\n</cyan>\nThe following Stores and/or Store Names will be upgraded:\n\n"
                    upgrade_overview += (
                        f"""    - Stores: {', '.join(stores_upgrade_checklist)}
"""
                        if stores_upgrade_checklist
                        else ""
                    )
                    upgrade_overview += (
                        f"""    - Store Names: {', '.join(store_names_upgrade_checklist)}
"""
                        if store_names_upgrade_checklist
                        else ""
                    )
            if manual_steps_required:
                upgrade_overview += "\n<cyan>Manual Steps\n=============</cyan>\n"
                if not self.upgrade_log["skipped_checkpoint_config_upgrade"]:
                    upgrade_overview += "\nThe following Checkpoints must be upgraded manually, due to using the old Checkpoint format, which is being deprecated:\n\n"
                    upgrade_overview += (
                        f"""    - Checkpoints: {', '.join(checkpoint_config_upgrade_checklist)}
"""
                        if checkpoint_config_upgrade_checklist
                        else ""
                    )
                if not self.upgrade_log["skipped_datasources_upgrade"]:
                    upgrade_overview += "\nThe following Data Sources must be upgraded manually, due to using the old Datasource format, which is being deprecated:\n\n"
                    upgrade_overview += (
                        f"""    - Data Sources: {', '.join(datasources_upgrade_checklist)}
"""
                        if datasources_upgrade_checklist
                        else ""
                    )
                if not self.upgrade_log["skipped_validation_operators_upgrade"]:
                    upgrade_overview += "\nYour configuration uses validation_operators, which are being deprecated.  Please, manually convert validation_operators to use the new Checkpoint validation unit, since validation_operators will be deleted.\n\n"
            else:
                upgrade_overview += "\n<cyan>Manual Steps\n=============\n</cyan>\nNo manual upgrade steps are required.\n"
        return upgrade_overview

    def upgrade_project(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            self._upgrade_configuration_automatically()
        except Exception:
            pass
        (
            upgrade_report,
            increment_version,
            exception_occurred,
        ) = self._generate_upgrade_report()
        return (upgrade_report, increment_version, exception_occurred)

    def _upgrade_configuration_automatically(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not self.upgrade_log["skipped_checkpoint_store_upgrade"]:
            config_commented_map: CommentedMap = (
                self.data_context.get_config().commented_map
            )
            for (key, config) in self.upgrade_checklist["automatic"]["stores"].items():
                config_commented_map["stores"][key] = config
            for (key, value) in self.upgrade_checklist["automatic"][
                "store_names"
            ].items():
                config_commented_map[key] = value
            data_context_config: DataContextConfig = (
                DataContextConfig.from_commented_map(commented_map=config_commented_map)
            )
            self.data_context.set_config(project_config=data_context_config)
            self.data_context._save_project_config()
            checkpoint_log_entry = {
                "stores": {
                    DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value: data_context_config.stores[
                        DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value
                    ]
                },
                "checkpoint_store_name": data_context_config.checkpoint_store_name,
            }
            self.upgrade_log["added_checkpoint_store"].update(checkpoint_log_entry)

    def _generate_upgrade_report(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        upgrade_log_path = self._save_upgrade_log()
        increment_version = self.upgrade_log["update_version"]
        upgrade_report = "<cyan>++================++\n|| Upgrade Report ||\n++================++</cyan>\n"
        manual_steps_required = self.manual_steps_required()
        if increment_version:
            if manual_steps_required:
                upgrade_report += f"""
<yellow>The Upgrade Helper has performed the automated upgrade steps as part of upgrading your project to be compatible with Great Expectations V3 API, and the config_version of your great_expectations.yml has been automatically incremented to 3.0.  However, manual steps are required in order for the upgrade process to be completed successfully.

A log detailing the upgrade can be found here:

    - {upgrade_log_path}</yellow>"""
            else:
                upgrade_report += f"""
<green>Your project was successfully upgraded to be compatible with Great Expectations V3 API.  The config_version of your great_expectations.yml has been automatically incremented to 3.0.

A log detailing the upgrade can be found here:

    - {upgrade_log_path}</green>"""
        elif manual_steps_required:
            upgrade_report += f"""
<yellow>The Upgrade Helper does not have any automated upgrade steps to perform as part of upgrading your project to be compatible with Great Expectations V3 API, and the config_version of your great_expectations.yml is already set to 3.0.  However, manual steps are required in order for the upgrade process to be completed successfully.

A log detailing the upgrade can be found here:

    - {upgrade_log_path}</yellow>"""
        else:
            upgrade_report += f"""
<yellow>The Upgrade Helper finds your project to be compatible with Great Expectations V3 API, and the config_version of your great_expectations.yml is already set to 3.0.  There are no additional automatic or manual steps required, since the upgrade process has been completed successfully.

A log detailing the upgrade can be found here:

    - {upgrade_log_path}</yellow>"""
        exception_occurred = False
        return (upgrade_report, increment_version, exception_occurred)

    def _save_upgrade_log(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%dT%H%M%S.%fZ"
        )
        dest_path = os.path.join(
            self.data_context.root_directory,
            "uncommitted",
            "logs",
            "project_upgrades",
            f"UpgradeHelperV13_{current_time}.json",
        )
        (dest_dir, dest_filename) = os.path.split(dest_path)
        os.makedirs(dest_dir, exist_ok=True)
        with open(dest_path, "w") as outfile:
            json.dump(self.upgrade_log, outfile, indent=2)
        return dest_path
