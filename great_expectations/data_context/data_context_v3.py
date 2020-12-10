import logging

from great_expectations.data_context.data_context import DataContext

logger = logging.getLogger(__name__)


class DataContextV3(DataContext):
    """Class implementing the v3 spec for DataContext configs, plus API changes for the 0.13+ series."""

    pass
