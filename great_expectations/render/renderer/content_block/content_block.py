from ..renderer import Renderer


class ContentBlockRenderer(Renderer):

    _content_block_type = "text"

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

                content_block_fn = getattr(cls, expectation_type, None)
                if content_block_fn is not None:
                    result = content_block_fn(
                        obj_,
                        styling=cls._get_element_styling(),
                        **kwargs
                    )
                    blocks += result

                else:
                    result = cls._missing_content_block_fn(obj_, **kwargs)
                    blocks += result

            return {
                "content_block_type": cls._content_block_type,
                cls._content_block_type: blocks,
                # TODO: This should probably be overridable via a parameter
                "styling": cls._get_content_block_styling(),
            }
        else:
            # TODO: Styling is not currently applied to non-list objects. It should be.
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

    @classmethod
    def _missing_content_block_fn(cls, obj, **kwargs):
        return []

    @classmethod
    def _get_content_block_styling(cls):
        return cls._default_content_block_styling

    @classmethod
    def _get_element_styling(cls):
        return cls._default_element_styling


class HeaderContentBlockRenderer(ContentBlockRenderer):
    pass


class ColumnTypeContentBlockRenderer(ContentBlockRenderer):
    pass
