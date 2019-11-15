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
    # # TODO: complete refactor of rendered types to avoid dictionary base
    # def to_json_dict(self):
    #     d = {}
    #     for k, v in self.__dict__.items():
    #         # Since we have many internal property names
    #         if k.startswith('_'):
    #             k = k[1:]
    #         if isinstance(v, list):
    #             d[k] = self.render_list_to_json(v)
    #         elif isinstance(v, RenderedContent):
    #             d[k] = v.to_json_dict()
    #         else:
    #             d[k] = v
    #     return d
    #
    # def render_list_to_json(self, list_):
    #     new_list = []
    #     for item in list_:
    #         if isinstance(item, RenderedContent):
    #             new_list.append(item.to_json_dict())
    #         elif isinstance(item, list):
    #             new_list.append(self.render_list_to_json(item))
    #         else:
    #             new_list.append(item)
    #     return new_list

    def to_json_dict(self):
        return {}

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
        d["subheader"] = self.subheader
        d["header_row"] = self.header_row
        return d


class RenderedGraphContent(RenderedComponentContent):
    def __init__(self, graph, styling=None, content_block_type="graph"):
        super(RenderedGraphContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.graph = graph

    def to_json_dict(self):
        d = super(RenderedGraphContent, self).to_json_dict()
        d["graph"] = self.graph
        return d


class RenderedTableContent(RenderedComponentContent):
    def __init__(self, table, header=None, header_row=None, styling=None, content_block_type="table"):
        super(RenderedTableContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.header = header
        self.table = table
        self.header_row = header_row

    def to_json_dict(self):
        d = super(RenderedTableContent, self).to_json_dict()
        d["header"] = self.header
        d["table"] = RenderedContent.rendered_content_list_to_json(self.table)
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
    def __init__(self, bullet_list, styling=None, content_block_type="bullet_list"):
        super(RenderedBulletListContent, self).__init__(content_block_type=content_block_type, styling=styling)
        self.bullet_list = bullet_list

    def to_json_dict(self):
        d = super(RenderedBulletListContent, self).to_json_dict()
        d["bullet_list"] = RenderedContent.rendered_content_list_to_json(self.bullet_list)
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
                 page_title=None, utm_medium=None):
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

    def to_json_dict(self):
        d = super(RenderedDocumentContent, self).to_json_dict()
        d["sections"] = RenderedContent.rendered_content_list_to_json(self.sections)
        d["data_asset_name"] = self.data_asset_name
        d["full_data_asset_identifier"] = self.full_data_asset_identifier
        d["renderer_type"] = self.renderer_type
        d["page_title"] = self.page_title
        d["utm_medium"] = self.utm_medium
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


#
# class RenderedComponentContent(RenderedContent):
#     #TODO: It's weird that 'text'-type blocks are called "content" when all of the other types are named after their types
#     _allowed_keys = set([
#         "content_block_type",
#         "header",
#         "subheader",
#         "styling",
#
#         "string_template",
#         "content",
#         "graph",
#         "value_list",
#         "header_row",
#         "table",
#         "bullet_list",
#     ])
#     _required_keys = set({
#         "content_block_type"
#     })


    # _allowed_keys = set([
    #     "section_name",
    #     "content_blocks",
    # ])
    # _required_keys = set({
    #     "content_blocks",
    # })
    # _key_types = {
    #     "content_blocks" : ListOf(RenderedComponentContent),
    # }


# class RenderedDocumentContent(RenderedContent):
#     _allowed_keys = set([
#         "renderer_type",
#         "data_asset_name",
#         "full_data_asset_identifier",
#         "page_title",
#         "utm_medium",
#         "sections",
#     ])
#     _required_keys = set({
#         "sections"
#     })
#     _key_types = {
#         "sections" : ListOf(RenderedSectionContent),
#     }

# class RenderedComponentContentWrapper(RenderedContent):
#     _allowed_keys = set([
#         "section",
#         "content_block",
#         "section_loop",
#         "content_block_loop",
#     ])
#     _required_keys = set([])
#     _key_types = {
#         "content_block": RenderedComponentContent,
#         "section": RenderedSectionContent,
#     }

# NOTE: The types below are rendering-related classes that we will probably want to implement eventually.

# class DomStylingInfo(object):
#     """Basically a struct type for:
#     {
#         "classes": ["root_foo"],
#         "styles": {"root": "bar"},
#         "attributes": {"root": "baz"},
#     }
#     """
#     pass


# class UnstyledStringTemplate(object):
#     """Basically a struct type for:
#     {
#         "template": "$var1 $var2 $var3",
#         "params": {
#             "var1": "aaa",
#             "var2": "bbb",
#             "var3": "ccc",
#         }
#     }
#     """
#     pass


# class StylableStringTemplate(object):
#     """Basically a struct type for:
#     {
#         "template": "$var1 $var2 $var3",
#         "params": {
#             "var1": "aaa",
#             "var2": "bbb",
#             "var3": "ccc",
#         },
#         "styling": {
#             **DomStylingInfo, #Styling for the whole templated string
#             "default" : DomStylingInfo, #Default styling for parameters
#             "params" : { #Styling overrides on an individual parameter basis
#                 "var1" : DomStylingInfo,
#                 "var2" : DomStylingInfo,
#             },
#         }
#     }
#     """

#     def __init__(self):
#         pass

#     def validate(self):
#         pass

#     @classmethod
#     def render_styling(cls, styling):
#         """Adds styling information suitable for an html tag

#         styling = {
#             "classes": ["alert", "alert-warning"],
#             "attributes": {
#                 "role": "alert",
#                 "data-toggle": "popover",
#             },
#             "styles" : {
#                 "padding" : "10px",
#                 "border-radius" : "2px",
#             }
#         }

#         returns a string similar to:
#         'class="alert alert-warning" role="alert" data-toggle="popover" style="padding: 10px; border-radius: 2px"'

#         (Note: `render_styling` makes no guarantees about)

#         "classes", "attributes" and "styles" are all optional parameters.
#         If they aren't present, they simply won't be rendered.

#         Other dictionary keys are also allowed and ignored.
#         This makes it possible for styling objects to be nested, so that different DOM elements

#         #NOTE: We should add some kind of type-checking to styling
#         """

#         class_list = styling.get("classes", None)
#         if class_list == None:
#             class_str = ""
#         else:
#             if type(class_list) == str:
#                 raise TypeError("classes must be a list, not a string.")
#             class_str = 'class="'+' '.join(class_list)+'" '

#         attribute_dict = styling.get("attributes", None)
#         if attribute_dict == None:
#             attribute_str = ""
#         else:
#             attribute_str = ""
#             for k, v in attribute_dict.items():
#                 attribute_str += k+'="'+v+'" '

#         style_dict = styling.get("styles", None)
#         if style_dict == None:
#             style_str = ""
#         else:
#             style_str = 'style="'
#             style_str += " ".join([k+':'+v+';' for k, v in style_dict.items()])
#             style_str += '" '

#         styling_string = pTemplate('$classes$attributes$style').substitute({
#             "classes": class_str,
#             "attributes": attribute_str,
#             "style": style_str,
#         })

#         return styling_string

#     def render_styling_from_string_template(template):
#         # NOTE: We should add some kind of type-checking to template
#         """This method is a thin wrapper use to call `render_styling` from within jinja templates.
#         """
#         if type(template) != dict:
#             return template

#         if "styling" in template:
#             return render_styling(template["styling"])

#         else:
#             return ""

#     def render_string_template(template):
#         # NOTE: We should add some kind of type-checking to template
#         if type(template) != dict:
#             return template

#         if "styling" in template:

#             params = template["params"]

#             # Apply default styling
#             if "default" in template["styling"]:
#                 default_parameter_styling = template["styling"]["default"]

#                 for parameter in template["params"].keys():

#                     # If this param has styling that over-rides the default, skip it here and get it in the next loop.
#                     if "params" in template["styling"]:
#                         if parameter in template["styling"]["params"]:
#                             continue

#                     params[parameter] = pTemplate('<span $styling>$content</span>').substitute({
#                         "styling": render_styling(default_parameter_styling),
#                         "content": params[parameter],
#                     })

#             # Apply param-specific styling
#             if "params" in template["styling"]:
#                 # params = template["params"]
#                 for parameter, parameter_styling in template["styling"]["params"].items():

#                     params[parameter] = pTemplate('<span $styling>$content</span>').substitute({
#                         "styling": render_styling(parameter_styling),
#                         "content": params[parameter],
#                     })

#             string = pTemplate(template["template"]).substitute(params)
#             return string

#         return pTemplate(template["template"]).substitute(template["params"])


# class DefaultJinjaCmponentStylingInfo(object):
#     """Basically a struct type for:
#         {
#             **DomStylingInfo,
#             "header": DomStylingInfo,
#             "subheader": DomStylingInfo,
#             "body": DomStylingInfo,
#         }

#     EX: {
#             "classes": ["root_foo"],
#             "styles": {"root": "bar"},
#             "attributes": {"root": "baz"},
#             "header": {
#                 "classes": ["header_foo"],
#                 "styles": {"header": "bar"},
#                 "attributes": {"header": "baz"},
#             },
#             "subheader": {
#                 "classes": ["subheader_foo"],
#                 "styles": {"subheader": "bar"},
#                 "attributes": {"subheader": "baz"},
#             },
#             "body": {
#                 "classes": ["body_foo"],
#                 "styles": {"body": "bar"},
#                 "attributes": {"body": "baz"},
#             }
#         }
# """
#     pass
