import os
import sys

# Maybe just check to see `if sys.platform == "darwin"` instead

if not os.path.isdir("/var/lib/cloud/instance"):
    print("Not using a cloud instance, so unsetting any GE_CLOUD_* vars")
    cloud_vars = [k for k in os.environ if k.startswith("GE_CLOUD")]
    for var_name in cloud_vars:
        print(f"Unsetting environment variable {var_name}")
        del os.environ[var_name]

sys.exit(1)
