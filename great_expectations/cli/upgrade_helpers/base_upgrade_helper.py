from abc import ABC, abstractmethod


class BaseUpgradeHelper(ABC):
    """Base UpgradeHelper abstract class"""

    @abstractmethod
    def manual_steps_required(self) -> None:
        pass

    @abstractmethod
    def get_upgrade_overview(self) -> None:
        pass

    @abstractmethod
    def upgrade_project(self) -> None:
        pass

    @abstractmethod
    def _generate_upgrade_report(self) -> None:
        pass

    @abstractmethod
    def _save_upgrade_log(self) -> None:
        pass
