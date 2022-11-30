import logging
import os
from typing import Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)

logger = logging.getLogger(__name__)


class _SerializableDataContext(AbstractDataContext):

    GE_DIR = "great_expectations"
    GE_YML = "great_expectations.yml"

    def __init__(self, runtime_environment: Optional[dict] = None) -> None:
        super().__init__(runtime_environment=runtime_environment)

    @classmethod
    def find_context_root_dir(cls) -> str:
        result = None
        yml_path = None
        ge_home_environment = os.getenv("GE_HOME")
        if ge_home_environment:
            ge_home_environment = os.path.expanduser(ge_home_environment)
            if os.path.isdir(ge_home_environment) and os.path.isfile(
                os.path.join(ge_home_environment, "great_expectations.yml")
            ):
                result = ge_home_environment
        else:
            yml_path = cls.find_context_yml_file()
            if yml_path:
                result = os.path.dirname(yml_path)

        if result is None:
            raise ge_exceptions.ConfigNotFoundError()

        logger.debug(f"Using project config: {yml_path}")
        return result

    @classmethod
    def find_context_yml_file(
        cls, search_start_dir: Optional[str] = None
    ) -> Optional[str]:
        """Search for the yml file starting here and moving upward."""
        yml_path = None
        if search_start_dir is None:
            search_start_dir = os.getcwd()

        for i in range(4):
            logger.debug(
                f"Searching for config file {search_start_dir} ({i} layer deep)"
            )

            potential_ge_dir = os.path.join(search_start_dir, cls.GE_DIR)

            if os.path.isdir(potential_ge_dir):
                potential_yml = os.path.join(potential_ge_dir, cls.GE_YML)
                if os.path.isfile(potential_yml):
                    yml_path = potential_yml
                    logger.debug(f"Found config file at {str(yml_path)}")
                    break
            # move up one directory
            search_start_dir = os.path.dirname(search_start_dir)

        return yml_path
