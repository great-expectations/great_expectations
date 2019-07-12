import json
from string import Template as pTemplate
import datetime
from collections import OrderedDict

from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape
)


def render_styling(styling):
    """Adds styling information suitable for an html tag

    styling = {
        "classes": ["alert", "alert-warning"],
        "attributes": {
            "role": "alert",
            "data-toggle": "popover",
        },
        "styles" : {
            "padding" : "10px",
            "border-radius" : "2px",
        }
    }

    returns a string similar to:
    'class="alert alert-warning" role="alert" data-toggle="popover" style="padding: 10px; border-radius: 2px"'

    (Note: `render_styling` makes no guarantees about)

    "classes", "attributes" and "styles" are all optional parameters.
    If they aren't present, they simply won't be rendered.

    Other dictionary keys are also allowed and ignored.
    This makes it possible for styling objects to be nested, so that different DOM elements

    # NOTE: We should add some kind of type-checking to styling
    """

    class_list = styling.get("classes", None)
    if class_list == None:
        class_str = ""
    else:
        if type(class_list) == str:
            raise TypeError("classes must be a list, not a string.")
        class_str = 'class="'+' '.join(class_list)+'" '

    attribute_dict = styling.get("attributes", None)
    if attribute_dict == None:
        attribute_str = ""
    else:
        attribute_str = ""
        for k, v in attribute_dict.items():
            attribute_str += k+'="'+v+'" '

    style_dict = styling.get("styles", None)
    if style_dict == None:
        style_str = ""
    else:
        style_str = 'style="'
        style_str += " ".join([k+':'+v+';' for k, v in style_dict.items()])
        style_str += '" '

    styling_string = pTemplate('$classes$attributes$style').substitute({
        "classes": class_str,
        "attributes": attribute_str,
        "style": style_str,
    })

    return styling_string


def render_styling_from_string_template(template):
    # NOTE: We should add some kind of type-checking to template
    """This method is a thin wrapper use to call `render_styling` from within jinja templates.
    """
    if not isinstance(template, (dict, OrderedDict)):
        return template

    if "styling" in template:
        return render_styling(template["styling"])

    else:
        return ""


def render_string_template(template):
    # NOTE: We should add some kind of type-checking to template
    if not isinstance(template, (dict, OrderedDict)):
        return template

    tag = template.get("tag")
    base_template_string = "<{tag} $styling>$content</{tag}>".format(
        tag=tag) if tag else "<span $styling>$content</span>"

    if "styling" in template:
        params = template["params"]

        # Apply default styling
        if "default" in template["styling"]:
            default_parameter_styling = template["styling"]["default"]

            for parameter in template["params"].keys():

                # If this param has styling that over-rides the default, skip it here and get it in the next loop.
                if "params" in template["styling"]:
                    if parameter in template["styling"]["params"]:
                        continue

                params[parameter] = pTemplate(base_template_string).substitute({
                    "styling": render_styling(default_parameter_styling),
                    "content": params[parameter],
                })

        # Apply param-specific styling
        if "params" in template["styling"]:
            # params = template["params"]
            for parameter, parameter_styling in template["styling"]["params"].items():
                if parameter not in params:
                    continue
                params[parameter] = pTemplate(base_template_string).substitute({
                    "styling": render_styling(parameter_styling),
                    "content": params[parameter],
                })

        string = pTemplate(template["template"]).substitute(params)
        return string

    if tag:
        template_string = "<{tag}>{template}</{tag}>".format(
            template=template["template"], tag=tag)
        return pTemplate(template_string).substitute(template["params"])
    else:
        return pTemplate(template["template"]).substitute(template["params"])


class NoOpTemplate(object):
    @classmethod
    def render(cls, document):
        return document


class PrettyPrintTemplate(object):
    @classmethod
    def render(cls, document, indent=2):
        print(json.dumps(document, indent=indent))


# Abe 2019/06/26: This View should probably actually be called JinjaView or something similar.
# Down the road, I expect to wind up with class hierarchy along the lines of:
#   View > JinjaView > GEContentBlockJinjaView
class DefaultJinjaView(object):
    """Defines a method for converting a document to human-consumable form"""

    _template = NoOpTemplate

    @classmethod
    def render(cls, document, template=None, **kwargs):
        if template is None:
            template = cls._template

        t = cls._get_template(template)
        return t.render(document, **kwargs)

    @classmethod
    def _get_template(cls, template):
        if template is None:
            return NoOpTemplate

        env = Environment(
            loader=PackageLoader(
                'great_expectations',
                'render/view/templates'
            ),
            autoescape=select_autoescape(['html', 'xml'])
        )
        env.filters['render_string_template'] = render_string_template
        env.filters['render_styling_from_string_template'] = render_styling_from_string_template
        env.filters['render_styling'] = render_styling

        template = env.get_template(template)
        template.globals['now'] = datetime.datetime.utcnow

        return template


class DefaultJinjaPageView(DefaultJinjaView):
    _template = "page.j2"

class DefaultJinjaIndexPageView(DefaultJinjaView):
    _template = "index_page.j2"

class DefaultJinjaSectionView(DefaultJinjaView):
    _template = "section.j2"


class DefaultJinjaComponentView(DefaultJinjaView):
    _template = "component.j2"
