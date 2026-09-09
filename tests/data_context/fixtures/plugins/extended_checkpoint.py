import logging

from great_expectations.checkpoint import Checkpoint

logger = logging.getLogger(__name__)


# `LegacyCheckpoint` no longer exists in `great_expectations.checkpoint` -- `Checkpoint` is now
# a pydantic model with its own generated `__init__`, so a custom subclass no longer needs (or
# can accept) the old `data_context`/`expectation_suite_name`/`action_list` constructor args.
class ExtendedCheckpoint(Checkpoint):
    pass


class ExtendedCheckpointIllegalBaseClass:
    def __init__(self, name: str, **kwargs):
        self.name = name
        self.kwargs = kwargs
