import warnings

from great_expectations.render import (
    RenderedAtomicContent as rendered_atomic_content_render,
)
from great_expectations.render import RenderedContent as rendered_content_render


class RenderedAtomicContent(rendered_atomic_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedAtomicContent from great_expectations.render.types is deprecated as of v0.15.32\
in v0.18. Please import class RenderedAtomicContent from great_expectations.render.
""",
        DeprecationWarning,
    )


class RenderedContent(rendered_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedContent from great_expectations.render.types is deprecated as of v0.15.32\
in v0.18. Please import class RenderedContent from great_expectations.render.
""",
        DeprecationWarning,
    )
