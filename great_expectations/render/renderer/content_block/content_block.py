from ..renderer import Renderer

class ContentBlock(Renderer):
    
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

    @classmethod
    def list_available_expectations(cls):
        expectations = [attr for attr in dir(cls) if attr[:7] == "expect_"]
        return expectations

class HeaderContentBlock(ContentBlock):
    pass

class ColumnTypeContentBlock(ContentBlock):
    pass

class BulletListContentBlock(ContentBlock):
    pass