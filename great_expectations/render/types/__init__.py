import warnings

from great_expectations.render import CollapseContent as CollapseContentRender
from great_expectations.render import (
    RenderedAtomicContent as RenderedAtomicContentRender,
)
from great_expectations.render import (
    RenderedAtomicContentSchema as RenderedAtomicContentSchemaRender,
)
from great_expectations.render import RenderedAtomicValue as RenderedAtomicValueRender
from great_expectations.render import (
    RenderedAtomicValueGraph as RenderedAtomicValueGraphRender,
)
from great_expectations.render import (
    RenderedAtomicValueSchema as RenderedAtomicValueSchemaRender,
)
from great_expectations.render import (
    RenderedBootstrapTableContent as RenderedBootstrapTableContentRender,
)
from great_expectations.render import (
    RenderedBulletListContent as RenderedBulletListContentRender,
)
from great_expectations.render import (
    RenderedComponentContent as RenderedComponentContentRender,
)
from great_expectations.render import RenderedContent as RenderedContentRender
from great_expectations.render import (
    RenderedContentBlockContainer as RenderedContentBlockContainerRender,
)
from great_expectations.render import (
    RenderedDocumentContent as RenderedDocumentContentRender,
)
from great_expectations.render import RenderedGraphContent as RenderedGraphContentRender
from great_expectations.render import (
    RenderedHeaderContent as RenderedHeaderContentRender,
)
from great_expectations.render import (
    RenderedMarkdownContent as RenderedMarkdownContentRender,
)
from great_expectations.render import (
    RenderedSectionContent as RenderedSectionContentRender,
)
from great_expectations.render import (
    RenderedStringTemplateContent as RenderedStringTemplateContentRender,
)
from great_expectations.render import RenderedTableContent as RenderedTableContentRender
from great_expectations.render import RenderedTabsContent as RenderedTabsContentRender
from great_expectations.render import TextContent as TextContentRender
from great_expectations.render import ValueListContent as ValueListContentRender


# TODO: Remove this entire module for release 0.18.0
def _get_deprecation_warning_message(classname: str) -> str:
    return (
        f"Importing the class {classname} from great_expectations.render is deprecated as of v0.15.32 "
        f"in v0.18. Please import class {classname} from great_expectations.render."
    )


class CollapseContent(CollapseContentRender):
    # deprecated-v0.15.32
    classname = "CollapseContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedAtomicContent(RenderedAtomicContentRender):
    # deprecated-v0.15.32
    classname = "RenderedAtomicContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedAtomicContentSchema(RenderedAtomicContentSchemaRender):
    # deprecated-v0.15.32
    classname = "RenderedAtomicContentSchema"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedAtomicValue(RenderedAtomicValueRender):
    # deprecated-v0.15.32
    classname = "RenderedAtomicValue"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedAtomicValueGraph(RenderedAtomicValueGraphRender):
    # deprecated-v0.15.32
    classname = "RenderedAtomicValueGraph"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedAtomicValueSchema(RenderedAtomicValueSchemaRender):
    # deprecated-v0.15.32
    classname = "RenderedAtomicValueSchema"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedBootstrapTableContent(RenderedBootstrapTableContentRender):
    # deprecated-v0.15.32
    classname = "RenderedBootstrapTableContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedBulletListContent(RenderedBulletListContentRender):
    # deprecated-v0.15.32
    classname = "RenderedBulletListContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedComponentContent(RenderedComponentContentRender):
    # deprecated-v0.15.32
    classname = "RenderedComponentContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedContent(RenderedContentRender):
    # deprecated-v0.15.32
    classname = "RenderedContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedContentBlockContainer(RenderedContentBlockContainerRender):
    # deprecated-v0.15.32
    classname = "RenderedContentBlockContainer"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedDocumentContent(RenderedDocumentContentRender):
    # deprecated-v0.15.32
    classname = "RenderedDocumentContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedGraphContent(RenderedGraphContentRender):
    # deprecated-v0.15.32
    classname = "RenderedGraphContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedHeaderContent(RenderedHeaderContentRender):
    # deprecated-v0.15.32
    classname = "RenderedHeaderContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedMarkdownContent(RenderedMarkdownContentRender):
    # deprecated-v0.15.32
    classname = "RenderedMarkdownContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedSectionContent(RenderedSectionContentRender):
    # deprecated-v0.15.32
    classname = "RenderedSectionContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedStringTemplateContent(RenderedStringTemplateContentRender):
    # deprecated-v0.15.32
    classname = "RenderedStringTemplateContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedTableContent(RenderedTableContentRender):
    # deprecated-v0.15.32
    classname = "RenderedTableContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class RenderedTabsContent(RenderedTabsContentRender):
    # deprecated-v0.15.32
    classname = "RenderedTabsContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class TextContent(TextContentRender):
    # deprecated-v0.15.32
    classname = "TextContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )


class ValueListContent(ValueListContentRender):
    # deprecated-v0.15.32
    classname = "ValueListContent"
    warnings.warn(
        _get_deprecation_warning_message(classname=classname),
        DeprecationWarning,
    )
