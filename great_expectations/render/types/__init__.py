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


class RenderedContent(dict):
    # TODO: complete refactor of rendered types to avoid dictionary base
    pass


class RenderedComponentContent(RenderedContent):
    def __init__(self, content_block_type, styling=None, **kwargs):
        self._content_block_type = content_block_type
        self._styling = styling
        self.update(kwargs)

    @property
    def content_block_type(self):
        return self._content_block_type

    @property
    def styling(self):
        return self._styling


class RenderedHeaderContent(RenderedComponentContent):
    def __init__(self, header, subheader=None, header_row=None, styling=None):
        super(RenderedHeaderContent, self).__init__(content_block_type="header", styling=styling)
        self._header = header
        self._header_row = header_row
        self._subheader = subheader

    @property
    def header(self):
        return self._header

    @property
    def subheader(self):
        return self._subheader

    @property
    def header_row(self):
        return self._header_row


class RenderedGraphContent(RenderedComponentContent):
    def __init__(self, graph, styling=None):
        super(RenderedGraphContent, self).__init__(content_block_type="graph", styling=styling)
        self._graph = graph

    @property
    def graph(self):
        return self._graph


class RenderedTableContent(RenderedComponentContent):
    def __init__(self, table, header_row=None, styling=None):
        super(RenderedTableContent, self).__init__(content_block_type="table", styling=styling)
        self._table = table
        self._header_row = header_row

    @property
    def table(self):
        return self._table

    @property
    def header_row(self):
        return self._header_row


class RenderedStringTemplateContent(RenderedComponentContent):
    def __init__(self, string_template, styling=None):
        super(RenderedStringTemplateContent, self).__init__(content_block_type="string_template", styling=styling)
        self._string_template = string_template

    @property
    def string_template(self):
        return self._string_template


class RenderedBulletListContent(RenderedComponentContent):
    def __init__(self, bullet_list, styling=None):
        super(RenderedBulletListContent, self).__init__(content_block_type="bullet_list", styling=styling)
        self._bullet_list = bullet_list

    @property
    def bullet_list(self):
        return self._bullet_list


class ValueListContent(RenderedComponentContent):
    def __init__(self, value_list, header=None, styling=None):
        super(ValueListContent, self).__init__(content_block_type="value_list", styling=styling)
        self._value_list = value_list
        self._header = header

    @property
    def value_list(self):
        return self._value_list

    @property
    def header(self):
        return self._header

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

# TODO: REMOVE
#### REMOVE ME########
class RenderedComponentContentWrapper(dict):
    pass


class RenderedSectionContent(RenderedContent):
    def __init__(self, content_blocks, section_name=None):
        if not isinstance(content_blocks, list) and all([isinstance(content_block, RenderedComponentContent) for
                                                         content_block in content_blocks]):
            raise InvalidRenderedContentError("Rendered section content requires a list of RenderedComponentContent "
                                              "for content blocks.")
        self._content_blocks = content_blocks
        self._section_name = section_name

    @property
    def content_blocks(self):
        return self._content_blocks

    @property
    def section_name(self):
        return self._section_name


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


class RenderedDocumentContent(RenderedContent):
    # NOTE: JPC 20191028 - review these keys to consolidate and group
    def __init__(self, sections, data_asset_name=None, full_data_asset_identifier=None, renderer_type=None,
                 page_title=None, utm_medium=None):
        if not isinstance(sections, list) and all([isinstance(section, RenderedSectionContent) for section in
                                                   sections]):
            raise InvalidRenderedContentError("RenderedDocumentContent requires a list of RenderedSectionContent for "
                                              "sections.")
        self._sections = sections
        self._data_asset_name = data_asset_name
        self._full_data_asset_identifier = full_data_asset_identifier
        self._renderer_type = renderer_type
        self._page_title = page_title
        self._utm_medium = utm_medium

    @property
    def sections(self):
        return self._sections

    @property
    def data_asset_name(self):
        return self._data_asset_name

    @property
    def full_data_asset_identifier(self):
        return self._full_data_asset_identifier

    @property
    def renderer_type(self):
        return self._renderer_type

    @property
    def page_title(self):
        return self._page_title

    @property
    def utm_medium(self):
        return self._utm_medium


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
