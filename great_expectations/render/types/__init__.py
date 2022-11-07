import warnings

from great_expectations.render import (
    RenderedAtomicContent as rendered_atomic_content_render,
)
from great_expectations.render import (
    RenderedBulletListContent as rendered_bullet_list_content_render,
)
from great_expectations.render import (
    RenderedComponentContent as rendered_component_content_render,
)
from great_expectations.render import RenderedContent as rendered_content_render
from great_expectations.render import (
    RenderedGraphContent as rendered_graph_content_render,
)
from great_expectations.render import (
    RenderedStringTemplateContent as rendered_string_template_content_render,
)
from great_expectations.render import (
    RenderedTableContent as rendered_table_content_render,
)


class RenderedAtomicContent(rendered_atomic_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedAtomicContent from great_expectations.render is deprecated as of v0.15.32\
in v0.18. Please import class RenderedAtomicContent from great_expectations.render.
""",
        DeprecationWarning,
    )


class RenderedBulletListContent(rendered_bullet_list_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedBulletListContent from great_expectations.render is deprecated as of v0.15.32\
in v0.18. Please import class RenderedBulletListContent from great_expectations.render.
""",
        DeprecationWarning,
    )


class RenderedComponentContent(rendered_component_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedComponentContent from great_expectations.render is deprecated as of v0.15.32\
in v0.18. Please import class RenderedComponentContent from great_expectations.render.
""",
        DeprecationWarning,
    )


class RenderedContent(rendered_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedContent from great_expectations.render is deprecated as of v0.15.32\
in v0.18. Please import class RenderedContent from great_expectations.render.
""",
        DeprecationWarning,
    )


class RenderedGraphContent(rendered_graph_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedGraphContent from great_expectations.render is deprecated as of v0.15.32\
in v0.18. Please import class RenderedGraphContent from great_expectations.render.
""",
        DeprecationWarning,
    )


class RenderedStringTemplateContent(rendered_string_template_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedStringTemplateContent from great_expectations.render is deprecated as of v0.15.32\
in v0.18. Please import class RenderedStringTemplateContent from great_expectations.render.
""",
        DeprecationWarning,
    )


class RenderedTableContent(rendered_table_content_render):
    # deprecated-v0.15.32
    warnings.warn(
        """Importing the class RenderedTableContent from great_expectations.render is deprecated as of v0.15.32\
in v0.18. Please import class RenderedTableContent from great_expectations.render.
""",
        DeprecationWarning,
    )
