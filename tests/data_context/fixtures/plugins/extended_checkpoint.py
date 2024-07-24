import logging
from typing import List, Optional

from great_expectations import Checkpoint

logger = logging.getLogger(__name__)


class ExtendedCheckpoint(Checkpoint):
    def __init__(
        self,
        name: str,
        data_context,
        expectation_suite_name: Optional[str] = None,
        action_list: Optional[List[dict]] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            expectation_suite_name=expectation_suite_name,
            action_list=action_list,
        )


class ExtendedCheckpointIllegalBaseClass:
    def __init__(
        self,
        name: str,
        data_context,
        expectation_suite_name: Optional[str] = None,
        action_list: Optional[List[dict]] = None,
    ):
        super().__init__(
            name=name,
            data_context=data_context,
            expectation_suite_name=expectation_suite_name,
            action_list=action_list,
        )
