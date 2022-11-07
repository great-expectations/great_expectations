import warnings

from great_expectations.render import (
    RenderedAtomicContent as RenderedAtomicContentRender,
)
from great_expectations.render import (
    RenderedBulletListContent as RenderedBulletListContentRender,
)
from great_expectations.render import (
    RenderedComponentContent as RenderedComponentContentRender,
)
from great_expectations.render import RenderedContent as RenderedContentRender
from great_expectations.render import RenderedGraphContent as RenderedGraphContentRender
from great_expectations.render import (
    RenderedStringTemplateContent as RenderedStringTemplateContentRender,
)
from great_expectations.render import RenderedTableContent as RenderedTableContentRender


class RenderedAtomicContent(RenderedAtomicContentRender):
    # deprecated-v0.15.32
    warnings.warn(
        "Importing the class RenderedAtomicContent from great_expectations.render is deprecated as of v0.15.32 "
        "in v0.18. Please import class RenderedAtomicContent from great_expectations.render.",
        DeprecationWarning,
    )


class RenderedBulletListContent(RenderedBulletListContentRender):
    # deprecated-v0.15.32
    warnings.warn(
        "Importing the class RenderedBulletListContent from great_expectations.render is deprecated as of v0.15.32 "
        "in v0.18. Please import class RenderedBulletListContent from great_expectations.render.",
        DeprecationWarning,
    )


class RenderedComponentContent(RenderedComponentContentRender):
    # deprecated-v0.15.32
    warnings.warn(
        "Importing the class RenderedComponentContent from great_expectations.render is deprecated as of v0.15.32 "
        "in v0.18. Please import class RenderedComponentContent from great_expectations.render.",
        DeprecationWarning,
    )


class RenderedContent(RenderedContentRender):
    # deprecated-v0.15.32
    warnings.warn(
        "Importing the class RenderedContent from great_expectations.render is deprecated as of v0.15.32 "
        "in v0.18. Please import class RenderedContent from great_expectations.render.",
        DeprecationWarning,
    )


class RenderedGraphContent(RenderedGraphContentRender):
    # deprecated-v0.15.32
    warnings.warn(
        "Importing the class RenderedGraphContent from great_expectations.render is deprecated as of v0.15.32 "
        "in v0.18. Please import class RenderedGraphContent from great_expectations.render.",
        DeprecationWarning,
    )


class RenderedStringTemplateContent(RenderedStringTemplateContentRender):
    # deprecated-v0.15.32
    warnings.warn(
        "Importing the class RenderedStringTemplateContent from great_expectations.render is deprecated as of v0.15.32 "
        "in v0.18. Please import class RenderedStringTemplateContent from great_expectations.render.",
        DeprecationWarning,
    )


class RenderedTableContent(RenderedTableContentRender):
    # deprecated-v0.15.32
    warnings.warn(
        "Importing the class RenderedTableContent from great_expectations.render is deprecated as of v0.15.32 "
        "in v0.18. Please import class RenderedTableContent from great_expectations.render.",
        DeprecationWarning,
    )
