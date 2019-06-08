from ..renderer import Renderer


class ContentBlock(Renderer):

    _content_block_type = "text"

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

        # print("??????")
        if object_type in ["evr_list", "expectation_list"]:
            # print("xxxxxxxxx")
            blocks = []
            for obj_ in render_object:
                # print("yyyyyyyyyy")
                expectation_type = cls._get_expectation_type(obj_)

                content_block_fn = getattr(cls, expectation_type, None)
                # print(expectation_type, content_block_fn)
                if content_block_fn is not None:
                    # print("zzzzzz")
                    result = content_block_fn(obj_, **kwargs)
                    # print(result)
                    blocks += result
                    # print("B", blocks)

            return {
                "content_block_type": cls._content_block_type,
                cls._content_block_type: blocks
            }
        else:
            # print("-----")
            expectation_type = cls._get_expectation_type(render_object)

            content_block_fn = getattr(cls, expectation_type, None)
            if content_block_fn is not None:
                return content_block_fn(render_object, **kwargs)
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
