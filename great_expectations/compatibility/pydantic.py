import pydantic

from great_expectations.compatibility.not_imported import (
    is_version_greater_or_equal,
)

if is_version_greater_or_equal(version=pydantic.VERSION, compare_version="2.0.0"):
    # from pydantic.v1 import BaseModel, Field, StrictStr
    from pydantic.v1 import *
    # from pydantic.v1 import Extra
else:
    # from pydantic import BaseModel, Field, StrictStr
    from pydantic import *
    # from pydantic import Extra


# from pydantic import StrictStr
# from pydantic.schema import default_ref_template
