from great_expectations.cli.upgrade_helpers.upgrade_helper_v11 import UpgradeHelperV11
from great_expectations.cli.upgrade_helpers.upgrade_helper_v13 import UpgradeHelperV13

GE_UPGRADE_HELPER_VERSION_MAP = {
    1: UpgradeHelperV11,
    2: UpgradeHelperV13,
    3: UpgradeHelperV13,  # Ensures that Manual Steps are highlighted (even if configuration is already at version 3.0).
}
