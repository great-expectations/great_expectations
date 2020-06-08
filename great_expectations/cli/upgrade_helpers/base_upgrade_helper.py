from abc import ABC, abstractmethod


class BaseUpgradeHelper(ABC):
    """Base UpgradeHelper abstract class"""

    @abstractmethod
    def get_upgrade_overview(self):
        pass

    @abstractmethod
    def upgrade_project(self):
        pass
