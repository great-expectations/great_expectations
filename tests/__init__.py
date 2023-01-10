import os
import sys

if sys.platform == "darwin":
    cloud_vars = [k for k in os.environ if k[:8] in ("GX_CLOUD", "GE_CLOUD")]
    if cloud_vars:
        print(
            "Running tests from a dev machine, so unsetting any CLOUD environment vars"
        )
        for var_name in cloud_vars:
            print(f"Unsetting environment variable {var_name}")
            del os.environ[var_name]
