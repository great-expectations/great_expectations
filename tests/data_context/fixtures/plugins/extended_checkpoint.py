import logging
from typing import List, Optional

from great_expectations.checkpoint import Checkpoint, LegacyCheckpoint, SimpleCheckpoint

logger = logging.getLogger(__name__)


class ExtendedCheckpoint(Checkpoint):
    def __init__(
        self,
        name: str,
        data_context,
        config_version: int = 1,
        expectation_suite_name: Optional[str] = None,
        action_list: Optional[List[dict]] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            config_version=config_version,
            expectation_suite_name=expectation_suite_name,
            action_list=action_list,
        )


class ExtendedSimpleCheckpoint(SimpleCheckpoint):
    def __init__(
        self,
        name: str,
        data_context,
        config_version: int = 1,
        expectation_suite_name: Optional[str] = None,
        action_list: Optional[List[dict]] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            config_version=config_version,
            expectation_suite_name=expectation_suite_name,
            action_list=action_list,
        )


class ExtendedLegacyCheckpoint(LegacyCheckpoint):
    def __init__(
        self,
        name: str,
        data_context,
        config_version: int = 1,
        expectation_suite_name: Optional[str] = None,
        action_list: Optional[List[dict]] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            config_version=config_version,
            expectation_suite_name=expectation_suite_name,
            action_list=action_list,
        )


class ExtendedCheckpointIllegalBaseClass:
    def __init__(
        self,
        name: str,
        data_context,
        config_version: int = 1,
        expectation_suite_name: Optional[str] = None,
        action_list: Optional[List[dict]] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            config_version=config_version,
            expectation_suite_name=expectation_suite_name,
            action_list=action_list,
        )
