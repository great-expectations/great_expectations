from .renderer import Renderer

class ContentBlock(Renderer):
    
    _content_block_type = "text"

    @classmethod
    def validate_input(cls, render_object):
        pass

    @classmethod
    def render(cls, render_object, **kwargs):
        cls.validate_input(render_object)
        object_type, data_asset_name = cls._find_ge_object_type(render_object)

        if object_type.startswith("list"):
            blocks = []
            for obj_ in render_object:
                expectation_type = cls._get_expectation_type(obj_)

                content_block_fn = getattr(cls, expectation_type, None)
                if content_block_fn is not None:
                    blocks += content_block_fn(render_object, **kwargs)
            return {
                "content_block_type": cls._content_block_type,
                cls._content_block_type: blocks
            }
        else:
            expectation_type = cls._get_expectation_type(render_object)

            content_block_fn = getattr(cls, expectation_type, None)
            if content_block_fn is not None:
                return content_block_fn(render_object, **kwargs)
            else:
                return None

class HeaderContentBlock(ContentBlock):
    pass

class ColumnTypeContentBlock(ContentBlock):
    pass
