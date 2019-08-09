# -*- coding: utf-8 -*-

import json
from string import Template as pTemplate
import datetime
from collections import OrderedDict

from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape, contextfilter
)

from great_expectations.version import __version__
from great_expectations.render.types import (
    RenderedDocumentContent,
    RenderedSectionContent,
    RenderedComponentContent,
    RenderedComponentContentWrapper,
)

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
        cls._validate_document(document)

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
        env.filters['render_string_template'] = cls.render_string_template
        env.filters['render_styling_from_string_template'] = cls.render_styling_from_string_template
        env.filters['render_styling'] = cls.render_styling
        env.filters['render_content_block'] = cls.render_content_block
        env.globals['ge_version'] = __version__

        template = env.get_template(template)
        template.globals['now'] = datetime.datetime.utcnow

        return template

    @classmethod
    @contextfilter
    def render_content_block(cls, context, content_block):
        if type(content_block) is str:
            return "<span>{content_block}</span>".format(content_block=content_block)
        elif content_block is None:
            return ""
        elif type(content_block) is list:
            return "".join([cls.render_content_block(context, content_block_el) for content_block_el in content_block])
        elif not isinstance(content_block, (dict, OrderedDict)):
            return content_block
        content_block_type = content_block.get("content_block_type")
        template = cls._get_template(template="{content_block_type}.j2".format(content_block_type=content_block_type))
        return template.render(context, content_block=content_block)

    @classmethod
    def render_styling(cls, styling):
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
            class_str = 'class="' + ' '.join(class_list) + '" '
    
        attribute_dict = styling.get("attributes", None)
        if attribute_dict == None:
            attribute_str = ""
        else:
            attribute_str = ""
            for k, v in attribute_dict.items():
                attribute_str += k + '="' + v + '" '
    
        style_dict = styling.get("styles", None)
        if style_dict == None:
            style_str = ""
        else:
            style_str = 'style="'
            style_str += " ".join([k + ':' + v + ';' for k, v in style_dict.items()])
            style_str += '" '
    
        styling_string = pTemplate('$classes$attributes$style').substitute({
            "classes": class_str,
            "attributes": attribute_str,
            "style": style_str,
        })
    
        return styling_string

    @classmethod
    def render_styling_from_string_template(cls, template):
        # NOTE: We should add some kind of type-checking to template
        """This method is a thin wrapper use to call `render_styling` from within jinja templates.
        """
        if not isinstance(template, (dict, OrderedDict)):
            return template
    
        if "styling" in template:
            return cls.render_styling(template["styling"])
    
        else:
            return ""

    @classmethod
    def render_string_template(cls, template):
        #NOTE: Using this line for debugging. This should probably be logged...? 
        # print(template)

        # NOTE: We should add some kind of type-checking to template
        if not isinstance(template, (dict, OrderedDict)):
            return template
    
        tag = template.get("tag", "span")
        template["template"] = template.get("template", "").replace("\n", "<br>")
    
        if "tooltip" in template:
            tooltip_content = template["tooltip"]["content"]
            tooltip_content.replace("\n", "<br>")
            placement = template["tooltip"].get("placement", "top")
            base_template_string = """
                <{tag} class="cooltip" $styling>
                    $template
                    <span class={placement}>
                        {tooltip_content}
                    </span>
                </{tag}>
            """.format(placement=placement, tooltip_content=tooltip_content, tag=tag)
        else:
            base_template_string = """
                <{tag} $styling>
                    $template
                </{tag}>
            """.format(tag=tag)
    
        if "styling" in template:
            params = template.get("params", {})
        
            # Apply default styling
            if "default" in template["styling"]:
                default_parameter_styling = template["styling"]["default"]
                default_param_tag = default_parameter_styling.get("tag", "span")
                base_param_template_string = "<{param_tag} $styling>$content</{param_tag}>".format(
                    param_tag=default_param_tag)
            
                for parameter in template["params"].keys():
                
                    # If this param has styling that over-rides the default, skip it here and get it in the next loop.
                    if "params" in template["styling"]:
                        if parameter in template["styling"]["params"]:
                            continue
                
                    params[parameter] = pTemplate(base_param_template_string).substitute({
                        "styling": cls.render_styling(default_parameter_styling),
                        "content": params[parameter],
                    })
        
            # Apply param-specific styling
            if "params" in template["styling"]:
                # params = template["params"]
                for parameter, parameter_styling in template["styling"]["params"].items():
                    if parameter not in params:
                        continue
                    param_tag = parameter_styling.get("tag", "span")
                    param_template_string = "<{param_tag} $styling>$content</{param_tag}>".format(param_tag=param_tag)
                    params[parameter] = pTemplate(param_template_string).substitute({
                        "styling": cls.render_styling(parameter_styling),
                        "content": params[parameter],
                    })
        
            string = pTemplate(
                pTemplate(base_template_string).substitute(
                    {"template": template["template"], "styling": cls.render_styling(template.get("styling", {}))})
            ).substitute(params)
            return string

        return pTemplate(
            pTemplate(base_template_string).substitute(
                {"template": template.get("template", ""), "styling": cls.render_styling(template.get("styling", {}))})
        ).substitute(template.get("params", {}))
    
    
class DefaultJinjaPageView(DefaultJinjaView):
    _template = "page.j2"

    @classmethod
    def _validate_document(cls, document):
        assert isinstance(document, RenderedDocumentContent)

class DefaultJinjaIndexPageView(DefaultJinjaPageView):
    _template = "index_page.j2"


class DefaultJinjaSectionView(DefaultJinjaView):
    _template = "section.j2"

    @classmethod
    def _validate_document(cls, document):
        assert isinstance(document, RenderedComponentContentWrapper)
        assert isinstance(document.section, RenderedSectionContent)

class DefaultJinjaComponentView(DefaultJinjaView):
    _template = "component.j2"

    @classmethod
    def _validate_document(cls, document):
        assert isinstance(document, RenderedComponentContentWrapper)
        assert isinstance(document.content_block, RenderedComponentContent)
