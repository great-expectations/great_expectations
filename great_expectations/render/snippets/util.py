import json


def render_parameter(var, format_str, mode="span", classes=["param-span"]):
    format_str_2 = '<span class="' + \
        (" ".join(classes))+'">{0:'+format_str+'}</span>'
    return (format_str_2).format(var)
