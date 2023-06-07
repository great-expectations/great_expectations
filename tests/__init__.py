import os
import sys

from great_expectations.data_context.cloud_constants import GXCloudEnvironmentVariable

if sys.platform == "darwin":
    for var in GXCloudEnvironmentVariable:
        if var in os.environ:
            print(f"Unsetting environment variable {var}")
            os.environ.pop(var)
