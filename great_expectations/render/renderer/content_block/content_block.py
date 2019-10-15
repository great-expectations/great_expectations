from ..renderer import Renderer
from ...types import (
    RenderedComponentContent,
)


class ContentBlockRenderer(Renderer):

    _content_block_type = "text"

    _default_header = ""

    _default_content_block_styling = {
        "classes": ["col-12"]
    }

    _default_element_styling = {}

    @classmethod
    def validate_input(cls, render_object):
        pass

    @classmethod
    def render(cls, render_object, **kwargs):
        cls.validate_input(render_object)
        object_type = cls._find_ge_object_type(render_object)

        if object_type in ["validation_report", "expectations"]:
            raise ValueError(
                "Provide an evr_list, expectation_list, expectation or evr to a content block")

        if object_type in ["evr_list", "expectation_list"]:
            blocks = []
            for obj_ in render_object:
                expectation_type = cls._get_expectation_type(obj_)

                content_block_fn = cls._get_content_block_fn(expectation_type)

                if content_block_fn is not None:
                    result = content_block_fn(
                        obj_,
                        styling=cls._get_element_styling(),
                        **kwargs
                    )
                    blocks += result

                else:
                    result = cls._missing_content_block_fn(
                        obj_,
                        cls._get_element_styling(),
                        **kwargs
                    )
                    if result is not None:
                        blocks += result

            if len(blocks) > 0:
                content_block = RenderedComponentContent(**{
                    "content_block_type": cls._content_block_type,
                    cls._content_block_type: blocks,
                    "styling": cls._get_content_block_styling(),
                })
                cls._process_content_block(content_block)

                return content_block
                
            else:
                return None
        else:
            expectation_type = cls._get_expectation_type(render_object)

            content_block_fn = getattr(cls, expectation_type, None)
            if content_block_fn is not None:
                return content_block_fn(render_object,
                                        styling=cls._get_element_styling(),
                                        **kwargs)
            else:
                return None

    @classmethod
    def _process_content_block(cls, content_block):
        header = cls._get_header()
        if header != "":
            content_block.update({
                "header": header
            })

    @classmethod
    def _get_content_block_fn(cls, expectation_type):
        return getattr(cls, expectation_type, None)

    @classmethod
    def list_available_expectations(cls):
        expectations = [attr for attr in dir(cls) if attr[:7] == "expect_"]
        return expectations

    @classmethod
    def _missing_content_block_fn(cls, obj, styling, **kwargs):
        return []

    @classmethod
    def _get_content_block_styling(cls):
        return cls._default_content_block_styling

    @classmethod
    def _get_element_styling(cls):
        return cls._default_element_styling

    @classmethod
    def _get_header(cls):
        return cls._default_header
