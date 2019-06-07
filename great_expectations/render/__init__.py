import importlib

from .view_models import (
    default,
    slack,
)

from .snippets import (
    expectation_bullet_point,
    evr_table_row,
    evr_content_block,
)


# def render(expectations=None, input_inspectable=None, renderer_class=None, output_inspectable=None):
#     renderer = renderer_class(
#         expectations=expectations,
#         inspectable=input_inspectable,
#     )
#     results = renderer.render()
#     return results

#!!! This would be a nicer API. Skipping for now
# def compile_to_documentation(input_, renderer_class_name, output_inspectable=None):
#     class_ = getattr(
#         importlib.import_module("view_models", "."),
#         renderer_class_name
#     )
#     instance = class_()

#     results = instance.render(input_)
#     return results

def compile_to_documentation(input_, renderer_class, output_inspectable=None):
    results = renderer_class().render(input_)
    return results
