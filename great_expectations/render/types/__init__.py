from collections import Iterable
from collections import namedtuple

class ListOf(object):
    def __init__(self, type_):
        self.type_ = type_

class DotDict(dict):
    """dot.notation access to dictionary attributes"""

    def __getattr__(self, attr):
        return self.get(attr)

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __dir__(self):
        return self.keys()

    # Cargo-cultishly copied from: https://github.com/spindlelabs/pyes/commit/d2076b385c38d6d00cebfe0df7b0d1ba8df934bc
    def __deepcopy__(self, memo):
        return DotDict([(copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for k, v in self.items()])


#Inspiration : https://codereview.stackexchange.com/questions/81794/dictionary-with-restricted-keys
class LimitedDotDict(DotDict):
    """dot.notation access to dictionary attributes, with limited keys
    

    Note: this class is pretty useless on its own.
    You need to subclass it like so:

    class MyLimitedDotDict(LimitedDotDict):
        _allowed_keys = set([
            "x", "y", "z"
        ])

    """

    _allowed_keys = set()
    _required_keys = set()
    _key_types = {}

    def __init__(self, coerce_types=False, **kwargs):
        # print(kwargs)
        # print(self._allowed_keys)
        # print(self._required_keys)

        if not self._required_keys.issubset(self._allowed_keys):
            raise ValueError("_required_keys : {!r} must be a subset of _allowed_keys {!r}".format(
                self._required_keys,
                self._allowed_keys,
            ))

        for key, value in kwargs.items():
            if key not in self._allowed_keys:
                raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                    key,
                    self._allowed_keys
                ))

            # if key in self._key_types and not isinstance(key, self._key_types[key]):
            if key in self._key_types:
                # print(value)

                #Update values if coerce_types==True
                if coerce_types:
                    #TODO: Catch errors and raise more informative error messages here

                    #If the given type is an instance of LimitedDotDict, apply coerce_types recursively
                    if isinstance(self._key_types[key], ListOf):
                        # assert isinstance(self._key_types[key], Iterable)
                        if issubclass(self._key_types[key], LimitedDotDict):
                            value = [self._key_types[key].type_(coerce_types=True, **v) for v in value]
                        else:
                            value = [self._key_types[key].type_(v) for v in value]

                    else:
                        if issubclass(self._key_types[key], LimitedDotDict):
                            value = self._key_types[key](coerce_types=True, **value)
                        else:
                            value = self._key_types[key](value)
                
                # print(value)
                
                #Validate types
                if type(value) != self._key_types[key]:

                    #TODO: Catch errors and raise more informative error messages here
                    if isinstance(self._key_types[key], ListOf):
                        assert isinstance(value, Iterable)
                        for v in value:
                            # print(v)
                            # print(self._key_types[key].type_)
                            assert isinstance(v, self._key_types[key].type_)

                    else:
                        raise TypeError("key: {!r} must be of type {!r}, not {!r}".format(
                            key,
                            self._key_types[key],
                            type(value),
                        ))

            self[key] = value

        for key in self._required_keys:
            if key not in kwargs:
                raise KeyError("key: {!r} is missing even though it's in the required keys: {!r}".format(
                    key,
                    self._required_keys
                ))

    def __setitem__(self, key, val):
        if key not in self._allowed_keys:
            raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                key,
                self._allowed_keys
            ))
        dict.__setitem__(self, key, val)

    def __setattr__(self, key, val):
        if key not in self._allowed_keys:
            raise KeyError("key: {!r} not in allowed keys: {!r}".format(
                key,
                self._allowed_keys
            ))
        dict.__setitem__(self, key, val)

# TODO: Rename to this:
# class RenderedContent(LimitedDotDict):
# class RenderedComponentContent(RenderedContent):
# class RenderedSectionContent(RenderedContent):
# class RenderedDocumentContent(RenderedContent):
# class RenderedContentBlockWrapper(RenderedContent):

class Rendered(object):
    pass

class RenderedContentBlock(LimitedDotDict):
    #TODO: It's weird that 'text'-type blocks are called "content" when all of the other types are named after their types
    _allowed_keys = set([
        "content_block_type",
        "header",
        "subheader",
        "styling",

        "content",
        "graph",
        "value_list",
        "table_rows",
    ])
    _required_keys = set({
        "content_block_type"
    })

class RenderedSection(LimitedDotDict):
    _allowed_keys = set([
        "section_name",
        "content_blocks",
    ])
    _required_keys = set({
    })

class RenderedDocument(LimitedDotDict):
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
        # "sections" : ListOf(RenderedSection),
    }

class RenderedContentBlockWrapper(LimitedDotDict):
    _allowed_keys = set([
        "section",
        "content_block",
        "section_loop",
        "content_block_loop",
    ])
    _required_keys = set([
    ])
    _key_types = {
        "content_block": RenderedContentBlock,
        #TODO: "section": RenderedSection,
    }


# class ValidationResult(LimitedDotDict):
#     _allowed_keys = set([
#         "expectation_config",
#         "",
#         ""
#     ])
#     _required_keys = set([
#     ])
#     _key_types = {
#         "expectation_config": ExpectationConfig,
#         # "": ...,
#     }

# class ValidationResultSuite(LimitedDotDict):
#     _allowed_keys = set([
#         "results",
#         "meta",
#     ])
#     _required_keys = set([
#         "results",
#         "meta",
#     ])
#     _key_types = {
#         "results": ListOf(ValidationResult),
#         "meta": SuiteMeta,
#     }





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
