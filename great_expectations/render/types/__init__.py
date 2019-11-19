from great_expectations.core import DataAssetIdentifier
from great_expectations.types import DictDot
from great_expectations.render.exceptions import InvalidRenderedContentError

# from great_expectations.types import (
#     AllowedKeysDotDict,
#     ListOf,
# )

# TODO: Rename to this:
# class RenderedContent(AllowedKeysDotDict):
    # class RenderedComponentContent(RenderedContent):
    # class RenderedSectionContent(RenderedContent):
    # class RenderedDocumentContentContent(RenderedContent):
    # class RenderedComponentContentWrapper(RenderedContent):


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
    def __init__(self, header, subheader=None, header_row=None, styling=None, content_block_type="header"):
        super(RenderedHeaderContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.header_row = header_row
        self.subheader = subheader

    def to_json_dict(self):
        d = super(RenderedHeaderContent, self).to_json_dict()
        d["header"] = self.header
        if self.subheader:
            d["subheader"] = self.subheader
        if self.header_row:
            d["header_row"] = self.header_row
        return d


class RenderedGraphContent(RenderedComponentContent):
    def __init__(self, graph, header=None, styling=None, content_block_type="graph"):
        super(RenderedGraphContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.graph = graph
        self.header = header

    def to_json_dict(self):
        d = super(RenderedGraphContent, self).to_json_dict()
        d["graph"] = self.graph
        d["header"] = self.header
        return d


class RenderedTableContent(RenderedComponentContent):
    def __init__(self, table, header=None, subheader=None, header_row=None, styling=None, content_block_type="table"):
        super(RenderedTableContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.subheader = subheader
        self.table = table
        self.header_row = header_row

    def to_json_dict(self):
        d = super(RenderedTableContent, self).to_json_dict()
        if self.header is not None:
            d["header"] = self.header
        if self.subheader is not None:
            d["subheader"] = self.subheader
        d["table"] = RenderedContent.rendered_content_list_to_json(self.table)
        if self.header_row is not None:
            d["header_row"] = self.header_row
        return d


class RenderedStringTemplateContent(RenderedComponentContent):
    def __init__(self, string_template, styling=None, content_block_type="string_template"):
        super(RenderedStringTemplateContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.string_template = string_template

    def to_json_dict(self):
        d = super(RenderedStringTemplateContent, self).to_json_dict()
        d["string_template"] = self.string_template
        return d


class RenderedBulletListContent(RenderedComponentContent):
    def __init__(self, bullet_list, header=None, styling=None, content_block_type="bullet_list"):
        super(RenderedBulletListContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.bullet_list = bullet_list

    def to_json_dict(self):
        d = super(RenderedBulletListContent, self).to_json_dict()
        d["bullet_list"] = RenderedContent.rendered_content_list_to_json(self.bullet_list)
        if self.header is not None:
            d["header"] = self.header
        return d


class ValueListContent(RenderedComponentContent):
    def __init__(self, value_list, header=None, styling=None, content_block_type="value_list"):
        super(ValueListContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.value_list = value_list

    def to_json_dict(self):
        d = super(ValueListContent, self).to_json_dict()
        d["header"] = self.header
        d["value_list"] = RenderedContent.rendered_content_list_to_json(self.value_list)
        return d


class TextContent(RenderedComponentContent):
    def __init__(self, text, header=None, styling=None, content_block_type="text"):
        super(TextContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.text = text
        self.header = header

    def to_json_dict(self):
        d = super(TextContent, self).to_json_dict()
        d["header"] = self.header
        d["text"] = self.text
        return d


class RenderedDocumentContent(RenderedContent):
    # NOTE: JPC 20191028 - review these keys to consolidate and group
    def __init__(self, sections, data_asset_name=None, full_data_asset_identifier=None, renderer_type=None,
                 page_title=None, utm_medium=None, cta_footer=None):
        if not isinstance(sections, list) and all([isinstance(section, RenderedSectionContent) for section in
                                                   sections]):
            raise InvalidRenderedContentError("RenderedDocumentContent requires a list of RenderedSectionContent for "
                                              "sections.")
        self.sections = sections
        self.data_asset_name = data_asset_name
        self.full_data_asset_identifier = full_data_asset_identifier
        self.renderer_type = renderer_type
        self.page_title = page_title
        self.utm_medium = utm_medium
        self.cta_footer = cta_footer

    def to_json_dict(self):
        d = super(RenderedDocumentContent, self).to_json_dict()
        d["sections"] = RenderedContent.rendered_content_list_to_json(self.sections)
        d["data_asset_name"] = self.data_asset_name
        d["full_data_asset_identifier"] = self.full_data_asset_identifier.to_path() if isinstance(
            self.full_data_asset_identifier, DataAssetIdentifier) else self.full_data_asset_identifier
        d["renderer_type"] = self.renderer_type
        d["page_title"] = self.page_title
        d["utm_medium"] = self.utm_medium
        d["cta_footer"] = self.cta_footer
        return d


class RenderedSectionContent(RenderedContent):
    def __init__(self, content_blocks, section_name=None):
        if not isinstance(content_blocks, list) and all([isinstance(content_block, RenderedComponentContent) for
                                                         content_block in content_blocks]):
            raise InvalidRenderedContentError("Rendered section content requires a list of RenderedComponentContent "
                                              "for content blocks.")
        self.content_blocks = content_blocks
        self.section_name = section_name

    def to_json_dict(self):
        d = super(RenderedSectionContent, self).to_json_dict()
        d["content_blocks"] = RenderedContent.rendered_content_list_to_json(self.content_blocks)
        d["section_name"] = self.section_name
        return d
