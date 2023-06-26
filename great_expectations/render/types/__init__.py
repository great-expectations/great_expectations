import warnings
from typing import List, Optional, Union

from great_expectations.core._docs_decorators import deprecated_method_or_class
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
)
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
from great_expectations.render.renderer_configuration import (
    RendererTableValue as RendererTableValueRender,
)


# TODO: Remove this entire module for release 0.18.0
def _get_deprecation_warning_message(classname: str) -> str:
    return (
        f"Importing the class {classname} from great_expectations.render.types is deprecated as of v0.15.32 "
        f"in v0.18. Please import class {classname} from great_expectations.render."
    )


@deprecated_method_or_class(
    version="0.15.32",
    message="Importing the class CollapseContent from "
    "great_expectations.render.types is deprecated as of v0.15.32 "
    "in v0.18. Please import class CollapseContent "
    "from great_expectations.render.",
)
class CollapseContent(CollapseContentRender):
    # deprecated-v0.15.32
    """CollapseContent is RenderedComponentContent that can be collapsed.

    Args:
        collapse: The content to be collapsed. If a list is provided, it can recursively contain RenderedContent.
        collpase_toggle_link: The toggle link for this CollapseContent.
        header: The header for this content block.
        subheader: The subheader for this content block.
        styling: A dictionary containing styling information.
        content_block_type: The type of content block.
        inline_link: Whether to include a link inline.
    """

    def __init__(  # noqa: PLR0913
        self,
        collapse: Union[RenderedContentRender, list],
        collapse_toggle_link: Optional[Union[RenderedContentRender, dict]] = None,
        header: Optional[Union[RenderedContentRender, dict]] = None,
        subheader: Optional[Union[RenderedContentRender, dict]] = None,
        styling: Optional[dict] = None,
        content_block_type: str = "collapse",
        inline_link: bool = False,
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            collapse=collapse,
            collapse_toggle_link=collapse_toggle_link,
            header=header,
            subheader=subheader,
            styling=styling,
            content_block_type=content_block_type,
            inline_link=inline_link,
        )


class RenderedAtomicContent(RenderedAtomicContentRender):
    # deprecated-v0.15.32
    def __init__(
        self,
        name: Union[str, AtomicDiagnosticRendererType, AtomicPrescriptiveRendererType],
        value: RenderedAtomicValueRender,
        value_type: Optional[str] = None,
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            name=name,
            value=value,
            value_type=value_type,
        )


class RenderedAtomicContentSchema(RenderedAtomicContentSchemaRender):
    # deprecated-v0.15.32
    def __new__(cls):
        warnings.warn(
            _get_deprecation_warning_message(classname=cls.__name__),
            DeprecationWarning,
        )


class RenderedAtomicValue(RenderedAtomicValueRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self,
        schema: Optional[dict] = None,
        header: Optional[RenderedAtomicValueRender] = None,
        template: Optional[str] = None,
        params: Optional[dict] = None,
        header_row: Optional[List[RendererTableValueRender]] = None,
        table: Optional[List[List[RendererTableValueRender]]] = None,
        graph: Optional[dict] = None,
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            schema=schema,
            header=header,
            template=template,
            params=params,
            header_row=header_row,
            table=table,
            graph=graph,
        )


class RenderedAtomicValueGraph(RenderedAtomicValueGraphRender):
    # deprecated-v0.15.32
    def __init__(
        self,
        graph: Optional[dict] = None,
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            graph=graph,
        )


class RenderedAtomicValueSchema(RenderedAtomicValueSchemaRender):
    # deprecated-v0.15.32
    def __new__(cls):
        warnings.warn(
            _get_deprecation_warning_message(classname=cls.__name__),
            DeprecationWarning,
        )


class RenderedBootstrapTableContent(RenderedBootstrapTableContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self,
        table_data,
        table_columns,
        title_row=None,
        table_options=None,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="bootstrap_table",
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            table_data=table_data,
            table_columns=table_columns,
            title_row=title_row,
            table_options=table_options,
            header=header,
            subheader=subheader,
            styling=styling,
            content_block_type=content_block_type,
        )


class RenderedBulletListContent(RenderedBulletListContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self,
        bullet_list,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="bullet_list",
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            bullet_list=bullet_list,
            header=header,
            subheader=subheader,
            styling=styling,
            content_block_type=content_block_type,
        )


class RenderedComponentContent(RenderedComponentContentRender):
    # deprecated-v0.15.32
    def __init__(self, content_block_type, styling=None):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            content_block_type=content_block_type,
            styling=styling,
        )


class RenderedContent(RenderedContentRender):
    # deprecated-v0.15.32
    def __new__(cls):
        warnings.warn(
            _get_deprecation_warning_message(classname=cls.__name__),
            DeprecationWarning,
        )


class RenderedContentBlockContainer(RenderedContentBlockContainerRender):
    # deprecated-v0.15.32
    def __init__(
        self, content_blocks, styling=None, content_block_type="content_block_container"
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            content_blocks=content_blocks,
            styling=styling,
            content_block_type=content_block_type,
        )


class RenderedDocumentContent(RenderedDocumentContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self,
        sections,
        data_asset_name=None,
        full_data_asset_identifier=None,
        renderer_type=None,
        page_title=None,
        utm_medium=None,
        cta_footer=None,
        expectation_suite_name=None,
        batch_kwargs=None,
        batch_spec=None,
        ge_cloud_id=None,
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            sections=sections,
            data_asset_name=data_asset_name,
            full_data_asset_identifier=full_data_asset_identifier,
            renderer_type=renderer_type,
            page_title=page_title,
            utm_medium=utm_medium,
            cta_footer=cta_footer,
            expectation_suite_name=expectation_suite_name,
            batch_kwargs=batch_kwargs,
            batch_spec=batch_spec,
            ge_cloud_id=ge_cloud_id,
        )


class RenderedGraphContent(RenderedGraphContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self,
        graph,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="graph",
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            graph=graph,
            header=header,
            subheader=subheader,
            styling=styling,
            content_block_type=content_block_type,
        )


class RenderedHeaderContent(RenderedHeaderContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self,
        header,
        subheader=None,
        header_row=None,
        styling=None,
        content_block_type="header",
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            header=header,
            subheader=subheader,
            header_row=header_row,
            styling=styling,
            content_block_type=content_block_type,
        )


class RenderedMarkdownContent(RenderedMarkdownContentRender):
    # deprecated-v0.15.32
    def __init__(self, markdown, styling=None, content_block_type="markdown"):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            markdown=markdown,
            styling=styling,
            content_block_type=content_block_type,
        )


class RenderedSectionContent(RenderedSectionContentRender):
    # deprecated-v0.15.32
    def __init__(self, content_blocks, section_name=None):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            content_blocks=content_blocks,
            section_name=section_name,
        )


@deprecated_method_or_class(
    version="0.15.32",
    message="Importing the class RenderedStringTemplateContent from "
    "great_expectations.render.types is deprecated as of v0.15.32 "
    "in v0.18. Please import class RenderedStringTemplateContent "
    "from great_expectations.render.",
)
class RenderedStringTemplateContent(RenderedStringTemplateContentRender):
    # deprecated-v0.15.32
    """RenderedStringTemplateContent is RenderedComponentContent that represents a templated string.

    Args:
        string_template: A dictionary containing:

            template: The string to perform substitution on. Variables are denoted with a preceeding $.

            params: A dictionary with keys that match variable names and values which will be substituted.

            styling: A dictionary containing styling information.
        styling: A dictionary containing styling information.
        content_block_type: The type of content block.
    """

    def __init__(
        self, string_template: dict, styling=None, content_block_type="string_template"
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            string_template=string_template,
            styling=styling,
            content_block_type=content_block_type,
        )


@deprecated_method_or_class(
    version="0.15.32",
    message="Importing the class RenderedTableContent from "
    "great_expectations.render.types is deprecated as of v0.15.32 "
    "in v0.18. Please import class RenderedTableContent "
    "from great_expectations.render.",
)
class RenderedTableContent(RenderedTableContentRender):
    # deprecated-v0.15.32
    """RenderedTableContent is RenderedComponentContent that is a table.

    Args:
        table: The table to be rendered.
        header: The header for this content block.
        subheader: The subheader for this content block.
        header_row: The header row for the table.
        styling: A dictionary containing styling information.
        content_block_type: The type of content block.
        table_options: The options that can be set for the table.

            search: A boolean indicating whether to include search with the table.

            icon-size: The size of the icons in the table. One of "sm", "md", or "lg".
        header_row_options: The options that can be set for the header_row. A dictionary with the keys being the column
            name and the values being a dictionary with the following form:

            sortable: A boolean indicating whether the column is sortable.
    """

    def __init__(  # noqa: PLR0913
        self,
        table,
        header=None,
        subheader=None,
        header_row=None,
        styling=None,
        content_block_type="table",
        table_options=None,
        header_row_options=None,
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            table=table,
            header=header,
            subheader=subheader,
            header_row=header_row,
            styling=styling,
            content_block_type=content_block_type,
            table_options=table_options,
            header_row_options=header_row_options,
        )


class RenderedTabsContent(RenderedTabsContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self, tabs, header=None, subheader=None, styling=None, content_block_type="tabs"
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            tabs=tabs,
            header=header,
            subheader=subheader,
            styling=styling,
            content_block_type=content_block_type,
        )


class TextContent(TextContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self, text, header=None, subheader=None, styling=None, content_block_type="text"
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            text=text,
            header=header,
            subheader=subheader,
            styling=styling,
            content_block_type=content_block_type,
        )


class ValueListContent(ValueListContentRender):
    # deprecated-v0.15.32
    def __init__(  # noqa: PLR0913
        self,
        value_list,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="value_list",
    ):
        warnings.warn(
            _get_deprecation_warning_message(classname=self.__class__.__name__),
            DeprecationWarning,
        )
        super().__init__(
            value_list=value_list,
            header=header,
            subheader=subheader,
            styling=styling,
            content_block_type=content_block_type,
        )
