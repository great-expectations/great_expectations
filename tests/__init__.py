import os
import sys

if sys.platform == "darwin":
    print(
        "Running tests from a dev machine, so unsetting any GE_CLOUD_* environment vars"
    )
    cloud_vars = [k for k in os.environ if k.startswith("GE_CLOUD")]
    for var_name in cloud_vars:
        print(f"Unsetting environment variable {var_name}")
        del os.environ[var_name]
