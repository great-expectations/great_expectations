from great_expectations.render.exceptions import InvalidRenderedContentError


class RenderedContent(object):
    def to_json_dict(self):
        return {}

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.to_json_dict() == other.to_json_dict()

    @classmethod
    def rendered_content_list_to_json(cls, list_):
        result_list = []
        for item in list_:
            if isinstance(item, RenderedContent):
                result_list.append(item.to_json_dict())
            elif isinstance(item, list):
                result_list.append(RenderedContent.rendered_content_list_to_json(item))
            else:
                result_list.append(item)
        return result_list


class RenderedComponentContent(RenderedContent):
    def __init__(self, content_block_type, styling=None):
        self.content_block_type = content_block_type
        if styling is None:
            styling = {}
        self.styling = styling

    def to_json_dict(self):
        d = super(RenderedComponentContent, self).to_json_dict()
        d["content_block_type"] = self.content_block_type
        if len(self.styling) > 0:
            d["styling"] = self.styling
        return d


class RenderedHeaderContent(RenderedComponentContent):
    def __init__(
        self,
        header,
        subheader=None,
        header_row=None,
        styling=None,
        content_block_type="header",
    ):
        super(RenderedHeaderContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.header = header
        self.header_row = header_row
        self.subheader = subheader

    def to_json_dict(self):
        d = super(RenderedHeaderContent, self).to_json_dict()
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
    def __init__(
        self,
        graph,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="graph",
    ):
        super(RenderedGraphContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.graph = graph
        self.header = header
        self.subheader = subheader

    def to_json_dict(self):
        d = super(RenderedGraphContent, self).to_json_dict()
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


class RenderedTableContent(RenderedComponentContent):
    def __init__(
        self,
        table,
        header=None,
        subheader=None,
        header_row=None,
        styling=None,
        content_block_type="table",
    ):
        super(RenderedTableContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.header = header
        self.subheader = subheader
        self.table = table
        self.header_row = header_row

    def to_json_dict(self):
        d = super(RenderedTableContent, self).to_json_dict()
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
        return d


class RenderedContentBlockContainer(RenderedComponentContent):
    def __init__(
        self, content_blocks, styling=None, content_block_type="content_block_container"
    ):
        super(RenderedContentBlockContainer, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.content_blocks = content_blocks

    def to_json_dict(self):
        d = super(RenderedContentBlockContainer, self).to_json_dict()
        d["content_blocks"] = RenderedContent.rendered_content_list_to_json(
            self.content_blocks
        )
        return d


class RenderedMarkdownContent(RenderedComponentContent):
    def __init__(self, markdown, styling=None, content_block_type="markdown"):
        super(RenderedMarkdownContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.markdown = markdown

    def to_json_dict(self):
        d = super(RenderedMarkdownContent, self).to_json_dict()
        d["markdown"] = self.markdown
        return d


class RenderedStringTemplateContent(RenderedComponentContent):
    def __init__(
        self, string_template, styling=None, content_block_type="string_template"
    ):
        super(RenderedStringTemplateContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.string_template = string_template

    def to_json_dict(self):
        d = super(RenderedStringTemplateContent, self).to_json_dict()
        d["string_template"] = self.string_template
        return d


class RenderedBulletListContent(RenderedComponentContent):
    def __init__(
        self,
        bullet_list,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="bullet_list",
    ):
        super(RenderedBulletListContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.header = header
        self.subheader = subheader
        self.bullet_list = bullet_list

    def to_json_dict(self):
        d = super(RenderedBulletListContent, self).to_json_dict()
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
    def __init__(
        self,
        value_list,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="value_list",
    ):
        super(ValueListContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.header = header
        self.subheader = subheader
        self.value_list = value_list

    def to_json_dict(self):
        d = super(ValueListContent, self).to_json_dict()
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
    def __init__(
        self, text, header=None, subheader=None, styling=None, content_block_type="text"
    ):
        super(TextContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.text = text
        self.header = header
        self.subheader = subheader

    def to_json_dict(self):
        d = super(TextContent, self).to_json_dict()
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


class CollapseContent(RenderedComponentContent):
    def __init__(
        self,
        collapse,
        collapse_toggle_link=None,
        header=None,
        subheader=None,
        styling=None,
        content_block_type="collapse",
        inline_link=False,
    ):
        super(CollapseContent, self).__init__(
            content_block_type=content_block_type, styling=styling
        )
        self.collapse_toggle_link = collapse_toggle_link
        self.header = header
        self.subheader = subheader
        self.collapse = collapse
        self.inline_link = inline_link

    def to_json_dict(self):
        d = super(CollapseContent, self).to_json_dict()
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
    def __init__(
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
    ):
        if not isinstance(sections, list) and all(
            [isinstance(section, RenderedSectionContent) for section in sections]
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

    def to_json_dict(self):
        d = super(RenderedDocumentContent, self).to_json_dict()
        d["sections"] = RenderedContent.rendered_content_list_to_json(self.sections)
        d["data_asset_name"] = self.data_asset_name
        d["full_data_asset_identifier"] = self.full_data_asset_identifier
        d["renderer_type"] = self.renderer_type
        d["page_title"] = self.page_title
        d["utm_medium"] = self.utm_medium
        d["cta_footer"] = self.cta_footer
        d["expectation_suite_name"] = self.expectation_suite_name
        d["batch_kwargs"] = self.batch_kwargs
        return d


class RenderedSectionContent(RenderedContent):
    def __init__(self, content_blocks, section_name=None):
        if not isinstance(content_blocks, list) and all(
            [
                isinstance(content_block, RenderedComponentContent)
                for content_block in content_blocks
            ]
        ):
            raise InvalidRenderedContentError(
                "Rendered section content requires a list of RenderedComponentContent "
                "for content blocks."
            )
        self.content_blocks = content_blocks
        self.section_name = section_name

    def to_json_dict(self):
        d = super(RenderedSectionContent, self).to_json_dict()
        d["content_blocks"] = RenderedContent.rendered_content_list_to_json(
            self.content_blocks
        )
        d["section_name"] = self.section_name
        return d
