from .model import Model

class ContentBlock(Model):
    
    @classmethod
    def validate_input(cls, ge_object):
        pass

    @classmethod
    def render(cls, ge_object, result_key):
        cls.validate_input(ge_object)
        expectation_type = cls._get_expectation_type(ge_object)

        content_block_fn = getattr(cls, expectation_type, None)
        if content_block_fn is not None:
            return content_block_fn(ge_object, result_key)
        else:
            return None

class HeaderContentBlock(ContentBlock):
    pass

class ColumnTypeContentBlock(ContentBlock):
    pass

class BulletListContentBlock(ContentBlock):
    pass