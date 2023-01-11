from great_expectations.render import RenderedBulletListContent
from great_expectations.render.renderer.content_block.expectation_string import (
    ExpectationStringRenderer,
)


class ExpectationSuiteBulletListContentBlockRenderer(ExpectationStringRenderer):
    _rendered_component_type = RenderedBulletListContent
    _content_block_type = "bullet_list"

    _default_element_styling = {
        "default": {"classes": ["badge", "badge-secondary"]},
        "params": {"column": {"classes": ["badge", "badge-primary"]}},
    }
