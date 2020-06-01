from abc import ABC, abstractmethod


class BaseUpgradeHelper(ABC):
    """Base UpgradeHelper abstract class"""

    @abstractmethod
    def upgrade_project(self):
        pass
