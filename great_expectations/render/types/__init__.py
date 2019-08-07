from great_expectations.types import (
    LooselyTypedDotDict,
    ListOf,
)

# TODO: Rename to this:
# class RenderedContent(LooselyTypedDotDict):
    # class RenderedComponentContent(RenderedContent):
    # class RenderedSectionContent(RenderedContent):
    # class RenderedDocumentContentContent(RenderedContent):
    # class RenderedComponentContentWrapper(RenderedContent):

class RenderedContent(LooselyTypedDotDict):
    pass

class RenderedComponentContent(RenderedContent):
    #TODO: It's weird that 'text'-type blocks are called "content" when all of the other types are named after their types
    _allowed_keys = set([
        "content_block_type",
        "header",
        #TODO: There can be only one!
        "subheader",
        "sub_header",
        "styling",

        "string_template",
        "content",
        "graph",
        "value_list",
        "header_row",
        "table",
        "bullet_list",
    ])
    _required_keys = set({
        "content_block_type"
    })

class RenderedSectionContent(RenderedContent):
    _allowed_keys = set([
        "section_name",
        "content_blocks",
    ])
    _required_keys = set({
        "content_blocks",
    })
    _key_types = {
        "content_blocks" : ListOf(RenderedComponentContent),
    }

class RenderedDocumentContent(RenderedContent):
    _allowed_keys = set([
        "renderer_type",
        "data_asset_name",
        "full_data_asset_identifier",
        "page_title",
        "utm_medium",
        "sections",
    ])
    _required_keys = set({
        "sections"
    })
    _key_types = {
        "sections" : ListOf(RenderedSectionContent),
    }

class RenderedComponentContentWrapper(RenderedContent):
    _allowed_keys = set([
        "section",
        "content_block",
        "section_loop",
        "content_block_loop",
    ])
    _required_keys = set([])
    _key_types = {
        "content_block": RenderedComponentContent,
        "section": RenderedSectionContent,
    }

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
