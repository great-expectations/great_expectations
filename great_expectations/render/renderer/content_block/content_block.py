import logging

from ..renderer import Renderer
from ...types import (
    RenderedComponentContent,
    TextContent)
from ....core import ExpectationValidationResult

logger = logging.getLogger(__name__)


class ContentBlockRenderer(Renderer):

    _rendered_component_type = TextContent
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

        if isinstance(render_object, list):
            blocks = []
            for obj_ in render_object:
                expectation_type = cls._get_expectation_type(obj_)

                content_block_fn = cls._get_content_block_fn(expectation_type)

                if content_block_fn is not None:
                    try:
                        result = content_block_fn(
                            obj_,
                            styling=cls._get_element_styling(),
                            **kwargs
                        )
                        blocks += result
                    except Exception as e:
                        logger.error("Exception occurred during data docs rendering: ", e, exc_info=True)
                        
                        if type(obj_) == ExpectationValidationResult:
                            content_block_fn = cls._get_content_block_fn("_missing_content_block_fn")
                        else:
                            content_block_fn = cls._missing_content_block_fn
                        result = content_block_fn(
                            obj_,
                            cls._get_element_styling(),
                            **kwargs
                        )
                        if result is not None:
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
                content_block = cls._rendered_component_type(**{
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
                try:
                    result = content_block_fn(render_object,
                                            styling=cls._get_element_styling(),
                                            **kwargs)
                    return result
                except Exception as e:
                    logger.error("Exception occurred during data docs rendering: ", e, exc_info=True)
                    
                    if type(render_object) == ExpectationValidationResult:
                        content_block_fn = cls._get_content_block_fn("_missing_content_block_fn")
                    else:
                        content_block_fn = cls._missing_content_block_fn
                    return content_block_fn(
                        render_object,
                        cls._get_element_styling(),
                        **kwargs
                    )
            else:
                return cls._missing_content_block_fn(
                            render_object,
                            cls._get_element_styling(),
                            **kwargs
                        )

    @classmethod
    def _process_content_block(cls, content_block):
        header = cls._get_header()
        if header != "":
            content_block.header = header

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
