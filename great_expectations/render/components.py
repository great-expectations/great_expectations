from __future__ import annotations

import json
from copy import deepcopy
from enum import Enum
from string import Template as pTemplate
from typing import TYPE_CHECKING, Final, List, Optional, Union

from marshmallow import Schema, fields, post_dump, post_load

from great_expectations.alias_types import JSONValues  # noqa: TCH001
from great_expectations.core._docs_decorators import public_api
from great_expectations.render.exceptions import InvalidRenderedContentError
from great_expectations.types import DictDot

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import (
        MetaNotes,
        RendererTableValue,
    )


class RendererPrefix(str, Enum):
    """Available renderer prefixes"""

    LEGACY = "renderer"
    ATOMIC = "atomic"


class AtomicRendererType(str, Enum):
    """Available atomic renderer types"""

    PRESCRIPTIVE = ".".join([RendererPrefix.ATOMIC, "prescriptive"])
    DIAGNOSTIC = ".".join([RendererPrefix.ATOMIC, "diagnostic"])


class AtomicPrescriptiveRendererType(str, Enum):
    """Available atomic prescriptive renderer names"""

    FAILED = ".".join([AtomicRendererType.PRESCRIPTIVE, "failed"])
    SUMMARY = ".".join([AtomicRendererType.PRESCRIPTIVE, "summary"])

    def __str__(self):
        return self.value


class AtomicDiagnosticRendererType(str, Enum):
    """Available atomic diagnostic renderer names"""

    FAILED = ".".join([AtomicRendererType.DIAGNOSTIC, "failed"])
    OBSERVED_VALUE = ".".join([AtomicRendererType.DIAGNOSTIC, "observed_value"])

    def __str__(self):
        return self.value


class LegacyRendererType(str, Enum):
    """Available legacy renderer types"""

    ANSWER = ".".join([RendererPrefix.LEGACY, "answer"])
    DESCRIPTIVE = ".".join([RendererPrefix.LEGACY, "descriptive"])
    DIAGNOSTIC = ".".join([RendererPrefix.LEGACY, "diagnostic"])
    PRESCRIPTIVE = ".".join([RendererPrefix.LEGACY, "prescriptive"])
    QUESTION = ".".join([RendererPrefix.LEGACY, "question"])


class LegacyPrescriptiveRendererType(str, Enum):
    """Available legacy prescriptive renderer names"""

    SUMMARY = ".".join([LegacyRendererType.PRESCRIPTIVE, "summary"])


class LegacyDiagnosticRendererType(str, Enum):
    """Available legacy diagnostic renderer names"""

    META_PROPERTIES = ".".join([LegacyRendererType.DIAGNOSTIC, "meta_properties"])
    OBSERVED_VALUE = ".".join([LegacyRendererType.DIAGNOSTIC, "observed_value"])
    STATUS_ICON = ".".join([LegacyRendererType.DIAGNOSTIC, "status_icon"])
    SUMMARY = ".".join([LegacyRendererType.DIAGNOSTIC, "summary"])
    UNEXPECTED_STATEMENT = ".".join(
        [LegacyRendererType.DIAGNOSTIC, "unexpected_statement"]
    )
    UNEXPECTED_TABLE = ".".join([LegacyRendererType.DIAGNOSTIC, "unexpected_table"])


class LegacyDescriptiveRendererType(str, Enum):
    """Available legacy descriptive renderer names"""

    COLUMN_PROPERTIES_TABLE_DISTINCT_COUNT_ROW = ".".join(
        [
            LegacyRendererType.DESCRIPTIVE,
            "column_properties_table",
            "distinct_count_row",
        ]
    )
    COLUMN_PROPERTIES_TABLE_DISTINCT_PERCENT_ROW = ".".join(
        [
            LegacyRendererType.DESCRIPTIVE,
            "column_properties_table",
            "distinct_percent_row",
        ]
    )
    COLUMN_PROPERTIES_TABLE_MISSING_COUNT_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "column_properties_table", "missing_count_row"]
    )
    COLUMN_PROPERTIES_TABLE_MISSING_PERCENT_ROW = ".".join(
        [
            LegacyRendererType.DESCRIPTIVE,
            "column_properties_table",
            "missing_percent_row",
        ]
    )
    COLUMN_PROPERTIES_TABLE_REGEX_COUNT_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "column_properties_table", "regex_count_row"]
    )
    EXAMPLE_VALUES_BLOCK = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "example_values_block"]
    )
    HISTOGRAM = ".".join([LegacyRendererType.DESCRIPTIVE, "histogram"])
    QUANTILE_TABLE = ".".join([LegacyRendererType.DESCRIPTIVE, "quantile_table"])
    STATS_TABLE_MAX_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "stats_table", "max_row"]
    )
    STATS_TABLE_MEAN_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "stats_table", "mean_row"]
    )
    STATS_TABLE_MIN_ROW = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "stats_table", "min_row"]
    )
    VALUE_COUNTS_BAR_CHART = ".".join(
        [LegacyRendererType.DESCRIPTIVE, "value_counts_bar_chart"]
    )


class RenderedContent:
    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedContent.

        Returns:
            A JSON-serializable dict representation of this RenderedContent.
        """
        return {}

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.to_json_dict() == other.to_json_dict()

    @classmethod
    def rendered_content_list_to_json(cls, list_, check_dicts=False):
        result_list = []
        for item in list_:
            if isinstance(item, RenderedContent):
                result_list.append(item.to_json_dict())
            elif isinstance(item, list):
                result_list.append(
                    RenderedContent.rendered_content_list_to_json(
                        item, check_dicts=check_dicts
                    )
                )
            elif check_dicts and isinstance(item, dict):
                result_list.append(cls.rendered_content_dict_to_json(item))
            else:
                result_list.append(item)
        return result_list

    @classmethod
    def rendered_content_dict_to_json(cls, dict_, check_list_dicts=True):
        json_dict = deepcopy(dict_)
        for key, val in json_dict.items():
            if not isinstance(val, (RenderedContent, list, dict)):
                continue
            elif isinstance(val, RenderedContent):
                json_dict[key] = val.to_json_dict()
            elif isinstance(val, list):
                json_dict[key] = cls.rendered_content_list_to_json(
                    val, check_list_dicts
                )
            elif isinstance(val, dict):
                json_dict[key] = cls.rendered_content_dict_to_json(
                    val, check_list_dicts
                )
        return json_dict


class RenderedComponentContent(RenderedContent):
    def __init__(self, content_block_type, styling=None) -> None:
        self.content_block_type = content_block_type
        if styling is None:
            styling = {}
        self.styling = styling

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedComponentContent.

        Returns:
            A JSON-serializable dict representation of this RenderedComponentContent.
        """
        d = super().to_json_dict()
        d["content_block_type"] = self.content_block_type
        if len(self.styling) > 0:
            d["styling"] = self.styling
        return d


class RenderedHeaderContent(RenderedComponentContent):
    def __init__(  # noqa: PLR0913
        self,
        header,
        subheader=None,
        header_row=None,
        styling=None,
        content_block_type="header",
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.header_row = header_row
        self.subheader = subheader

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedHeaderContent.

        Returns:
            A JSON-serializable dict representation of this RenderedHeaderContent.
        """
        d = super().to_json_dict()
        if isinstance(self.header, RenderedContent):
            d["header"] = self.header.to_json_dict()
        else:
            d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        if self.header_row:
            d["header_row"] = self.header_row
        return d


class RenderedGraphContent(RenderedComponentContent):
    def __init__(  # noqa: PLR0913
        self,
        graph,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="graph",
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.graph = graph
        self.header = header
        self.subheader = subheader

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedGraphContent.

        Returns:
            A JSON-serializable dict representation of this RenderedGraphContent.
        """
        d = super().to_json_dict()
        d["graph"] = self.graph
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        return d


@public_api
class RenderedTableContent(RenderedComponentContent):
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
        table: list[RenderedContent],
        header: Optional[Union[RenderedContent, dict]] = None,
        subheader: Optional[Union[RenderedContent, dict]] = None,
        header_row: Optional[list[RenderedContent]] = None,
        styling: Optional[dict] = None,
        content_block_type: str = "table",
        table_options: Optional[dict] = None,
        header_row_options: Optional[dict] = None,
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.subheader = subheader
        self.table = table
        self.table_options = table_options
        self.header_row = header_row
        self.header_row_options = header_row_options

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedTableContent.

        Returns:
            A JSON-serializable dict representation of this RenderedTableContent.
        """
        d = super().to_json_dict()
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        d["table"] = RenderedContent.rendered_content_list_to_json(self.table)
        if self.header_row is not None:
            d["header_row"] = RenderedContent.rendered_content_list_to_json(
                self.header_row
            )
        if self.header_row_options is not None:
            d["header_row_options"] = self.header_row_options
        if self.table_options is not None:
            d["table_options"] = self.table_options
        return d


class RenderedTabsContent(RenderedComponentContent):
    def __init__(  # noqa: PLR0913
        self, tabs, header=None, subheader=None, styling=None, content_block_type="tabs"
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.tabs = tabs
        self.header = header
        self.subheader = subheader

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedTabsContent.

        Returns:
            A JSON-serializable dict representation of this RenderedTabsContent.
        """
        d = super().to_json_dict()
        d["tabs"] = RenderedContent.rendered_content_list_to_json(
            self.tabs, check_dicts=True
        )
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        return d


class RenderedBootstrapTableContent(RenderedComponentContent):
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
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.table_data = table_data
        self.table_columns = table_columns
        self.title_row = title_row
        self.table_options = table_options
        self.header = header
        self.subheader = subheader

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedBootstrapTableContent.

        Returns:
            A JSON-serializable dict representation of this RenderedBootstrapTableContent.
        """
        d = super().to_json_dict()
        d["table_data"] = RenderedContent.rendered_content_list_to_json(
            self.table_data, check_dicts=True
        )
        d["table_columns"] = RenderedContent.rendered_content_list_to_json(
            self.table_columns, check_dicts=True
        )
        if self.table_options is not None:
            d["table_options"] = self.table_options
        if self.title_row is not None:
            if isinstance(self.title_row, RenderedContent):
                d["title_row"] = self.title_row.to_json_dict()
            else:
                d["title_row"] = self.title_row
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        return d


class RenderedContentBlockContainer(RenderedComponentContent):
    def __init__(
        self, content_blocks, styling=None, content_block_type="content_block_container"
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.content_blocks = content_blocks

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedContentBlockContainer.

        Returns:
            A JSON-serializable dict representation of this RenderedContentBlockContainer.
        """
        d = super().to_json_dict()
        d["content_blocks"] = RenderedContent.rendered_content_list_to_json(
            self.content_blocks
        )
        return d


class RenderedMarkdownContent(RenderedComponentContent):
    def __init__(self, markdown, styling=None, content_block_type="markdown") -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.markdown = markdown

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedMarkdownContent.

        Returns:
            A JSON-serializable dict representation of this RenderedMarkdownContent.
        """
        d = super().to_json_dict()
        d["markdown"] = self.markdown
        return d


@public_api
class RenderedStringTemplateContent(RenderedComponentContent):
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
        self,
        string_template: dict,
        styling: Optional[dict] = None,
        content_block_type: str = "string_template",
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.string_template = string_template

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedStringTemplateContent.

        Returns:
            A JSON-serializable dict representation of this RenderedStringTemplateContent.
        """
        d = super().to_json_dict()
        d["string_template"] = self.string_template
        return d

    def __str__(self):
        string = pTemplate(self.string_template["template"]).safe_substitute(
            self.string_template["params"]
        )
        return string

    def __eq__(self, other):
        return str(self) == str(other)


class RenderedBulletListContent(RenderedComponentContent):
    def __init__(  # noqa: PLR0913
        self,
        bullet_list,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="bullet_list",
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.subheader = subheader
        self.bullet_list = bullet_list

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedBulletListContent.

        Returns:
            A JSON-serializable dict representation of this RenderedBulletListContent.
        """
        d = super().to_json_dict()
        d["bullet_list"] = RenderedContent.rendered_content_list_to_json(
            self.bullet_list
        )
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        return d


class ValueListContent(RenderedComponentContent):
    def __init__(  # noqa: PLR0913
        self,
        value_list,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="value_list",
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.subheader = subheader
        self.value_list = value_list

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this ValueListContent.

        Returns:
            A JSON-serializable dict representation of this ValueListContent.
        """
        d = super().to_json_dict()
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        d["value_list"] = RenderedContent.rendered_content_list_to_json(self.value_list)
        return d


class TextContent(RenderedComponentContent):
    def __init__(  # noqa: PLR0913
        self, text, header=None, subheader=None, styling=None, content_block_type="text"
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.text = text
        self.header = header
        self.subheader = subheader

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this TextContent.

        Returns:
            A JSON-serializable dict representation of this TextContent.
        """
        d = super().to_json_dict()
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        d["text"] = RenderedContent.rendered_content_list_to_json(self.text)

        return d


@public_api
class CollapseContent(RenderedComponentContent):
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
        collapse: Union[RenderedContent, list],
        collapse_toggle_link: Optional[Union[RenderedContent, dict]] = None,
        header: Optional[Union[RenderedContent, dict]] = None,
        subheader: Optional[Union[RenderedContent, dict]] = None,
        styling: Optional[dict] = None,
        content_block_type: str = "collapse",
        inline_link: bool = False,
    ) -> None:
        super().__init__(content_block_type=content_block_type, styling=styling)
        self.collapse_toggle_link = collapse_toggle_link
        self.header = header
        self.subheader = subheader
        self.collapse = collapse
        self.inline_link = inline_link

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this CollapseContent.

        Returns:
            A JSON-serializable dict representation of this CollapseContent.
        """
        d = super().to_json_dict()
        if self.header is not None:
            if isinstance(self.header, RenderedContent):
                d["header"] = self.header.to_json_dict()
            else:
                d["header"] = self.header
        if self.subheader is not None:
            if isinstance(self.subheader, RenderedContent):
                d["subheader"] = self.subheader.to_json_dict()
            else:
                d["subheader"] = self.subheader
        if self.collapse_toggle_link is not None:
            if isinstance(self.collapse_toggle_link, RenderedContent):
                d["collapse_toggle_link"] = self.collapse_toggle_link.to_json_dict()
            else:
                d["collapse_toggle_link"] = self.collapse_toggle_link
        d["collapse"] = RenderedContent.rendered_content_list_to_json(self.collapse)
        d["inline_link"] = self.inline_link

        return d


class RenderedDocumentContent(RenderedContent):
    # NOTE: JPC 20191028 - review these keys to consolidate and group
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
    ) -> None:
        if not isinstance(sections, list) and all(
            isinstance(section, RenderedSectionContent) for section in sections
        ):
            raise InvalidRenderedContentError(
                "RenderedDocumentContent requires a list of RenderedSectionContent for "
                "sections."
            )
        self.sections = sections
        self.data_asset_name = data_asset_name
        self.full_data_asset_identifier = full_data_asset_identifier
        self.renderer_type = renderer_type
        self.page_title = page_title
        self.utm_medium = utm_medium
        self.cta_footer = cta_footer
        self.expectation_suite_name = expectation_suite_name
        self.batch_kwargs = batch_kwargs
        self.batch_spec = batch_spec
        self.ge_cloud_id = ge_cloud_id

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedDocumentContent.

        Returns:
            A JSON-serializable dict representation of this RenderedDocumentContent.
        """
        d = super().to_json_dict()
        d["sections"] = RenderedContent.rendered_content_list_to_json(self.sections)
        d["data_asset_name"] = self.data_asset_name
        d["full_data_asset_identifier"] = self.full_data_asset_identifier
        d["renderer_type"] = self.renderer_type
        d["page_title"] = self.page_title
        d["utm_medium"] = self.utm_medium
        d["cta_footer"] = self.cta_footer
        d["expectation_suite_name"] = self.expectation_suite_name
        d["batch_kwargs"] = self.batch_kwargs
        d["batch_spec"] = self.batch_spec
        d["ge_cloud_id"] = self.ge_cloud_id
        return d


class RenderedSectionContent(RenderedContent):
    def __init__(self, content_blocks, section_name=None) -> None:
        if not isinstance(content_blocks, list) and all(
            isinstance(content_block, RenderedComponentContent)
            for content_block in content_blocks
        ):
            raise InvalidRenderedContentError(
                "Rendered section content requires a list of RenderedComponentContent "
                "for content blocks."
            )
        self.content_blocks = content_blocks
        self.section_name = section_name

    @public_api
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedSectionContent.

        Returns:
            A JSON-serializable dict representation of this RenderedSectionContent.
        """
        d = super().to_json_dict()
        d["content_blocks"] = RenderedContent.rendered_content_list_to_json(
            self.content_blocks
        )
        d["section_name"] = self.section_name
        return d


class RenderedAtomicValue(DictDot):
    def __init__(  # noqa: PLR0913
        self,
        schema: Optional[dict] = None,
        header: Optional[RenderedAtomicValue] = None,
        template: Optional[str] = None,
        params: Optional[dict] = None,
        header_row: Optional[List[RendererTableValue]] = None,
        table: Optional[List[List[RendererTableValue]]] = None,
        graph: Optional[dict] = None,
        meta_notes: Optional[MetaNotes] = None,
    ) -> None:
        self.schema: Optional[dict] = schema
        self.header: Optional[RenderedAtomicValue] = header

        # StringValueType
        self.template: Optional[str] = template
        self.params: Optional[dict] = params

        # TableType
        self.header_row: Optional[List[RendererTableValue]] = header_row
        self.table: Optional[List[List[RendererTableValue]]] = table

        # GraphType
        self.graph = RenderedAtomicValueGraph(graph=graph)

        self.meta_notes: Optional[MetaNotes] = meta_notes

    def __repr__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    @public_api
    def to_json_dict(self, remove_null_attrs: bool = True) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedAtomicValue.

        Returns:
            A JSON-serializable dict representation of this RenderedAtomicValue.
        """
        json_dict = super().to_dict()
        if remove_null_attrs:
            json_dict = RenderedAtomicValueSchema.remove_null_attrs(data=json_dict)
        for key in json_dict:
            value = getattr(self, key)
            if key == "graph":
                json_dict[key] = value.to_json_dict()
            elif key == "params":
                for param_name, param in value.items():
                    if not isinstance(param["schema"]["type"], str):
                        json_dict[key][param_name]["schema"]["type"] = param["schema"][
                            "type"
                        ].value
        return json_dict


class RenderedAtomicValueGraph(DictDot):
    def __init__(
        self,
        graph: Optional[dict] = None,
    ):
        self.graph = graph

    def __repr__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    @public_api
    def to_json_dict(self) -> Optional[dict[str, JSONValues]]:
        """Returns a JSON-serializable dict representation of this RenderedAtomicValueGraph.

        Returns:
            A JSON-serializable dict representation of this RenderedAtomicValueGraph.
        """
        return self.graph


class RenderedAtomicValueSchema(Schema):
    schema = fields.Dict(required=False, allow_none=True)
    header = fields.Dict(required=False, allow_none=True)

    # for StringValueType
    template = fields.String(required=False, allow_none=True)
    params = fields.Dict(required=False, allow_none=True)

    # for TableType
    header_row = fields.List(fields.Dict, required=False, allow_none=True)
    table = fields.List(fields.List(fields.Dict, required=False, allow_none=True))

    # for GraphType
    graph = fields.Dict(required=False, allow_none=True)

    meta_notes = fields.Dict(required=False, allow_none=True)

    @post_load
    def create_value_obj(self, data, **kwargs):
        return RenderedAtomicValue(**data)

    REMOVE_KEYS_IF_NONE: Final[tuple[str, ...]] = (
        "header",
        "template",
        "table",
        "params",
        "header_row",
        "table",
        "graph",
        "meta_notes",
    )

    @staticmethod
    def remove_null_attrs(data: dict) -> dict:
        """Removes the attributes in RenderedAtomicValueSchema.REMOVE_KEYS_IF_NONE if
        their values are None."""
        cleaned_serialized_dict = deepcopy(data)
        for key in RenderedAtomicValueSchema.REMOVE_KEYS_IF_NONE:
            if (
                key == "graph"
                and key in cleaned_serialized_dict
                and cleaned_serialized_dict.get(key, {}).get("graph") is None
            ):
                cleaned_serialized_dict.pop(key)
            elif key == "meta_notes" and key in cleaned_serialized_dict:
                meta_notes = cleaned_serialized_dict.get(key, {})
                if meta_notes is None or not meta_notes.get("content"):
                    cleaned_serialized_dict.pop(key)
            elif (
                key in cleaned_serialized_dict and cleaned_serialized_dict[key] is None
            ):
                cleaned_serialized_dict.pop(key)
        return cleaned_serialized_dict

    @post_dump
    def clean_null_attrs(self, data, **kwargs: dict):
        return RenderedAtomicValueSchema.remove_null_attrs(data=data)


class RenderedAtomicContent(RenderedContent):
    def __init__(
        self,
        name: Union[str, AtomicDiagnosticRendererType, AtomicPrescriptiveRendererType],
        value: RenderedAtomicValue,
        value_type: Optional[str] = None,
        exception: Optional[str] = None,
    ) -> None:
        # str conversion is performed to ensure Enum value is what is serialized
        self.name = str(name)
        self.value = value
        self.value_type = value_type
        self.exception = exception

    def __repr__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self) -> str:
        return json.dumps(self.to_json_dict(), indent=2)

    @public_api
    def to_json_dict(self, remove_null_attrs: bool = True) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this RenderedAtomicContent.

        Returns:
            A JSON-serializable dict representation of this RenderedAtomicContent.
        """
        """Returns RenderedAtomicContent as a json dictionary."""
        d = renderedAtomicContentSchema.dump(self)
        d["value"] = self.value.to_json_dict(remove_null_attrs=remove_null_attrs)
        return d


class RenderedAtomicContentSchema(Schema):
    name = fields.String(required=False, allow_none=True)
    value = fields.Nested(RenderedAtomicValueSchema(), required=True, allow_none=False)
    value_type = fields.String(required=True, allow_none=False)
    exception = fields.String(required=False, allow_none=True)

    REMOVE_KEYS_IF_NONE: Final[tuple[str, ...]] = ("exception",)

    @post_load
    def make_rendered_atomic_content(self, data, **kwargs):
        return RenderedAtomicContent(**data)

    @post_dump
    def clean_null_attrs(self, data: dict, **kwargs: dict) -> dict:
        """Removes the attributes in RenderedAtomicContentSchema.REMOVE_KEYS_IF_NONE during serialization if
        their values are None."""
        data = deepcopy(data)
        for key in RenderedAtomicContentSchema.REMOVE_KEYS_IF_NONE:
            if key in data and data[key] is None:
                data.pop(key)
        return data


renderedAtomicContentSchema = RenderedAtomicContentSchema()
renderedAtomicValueSchema = RenderedAtomicValueSchema()
