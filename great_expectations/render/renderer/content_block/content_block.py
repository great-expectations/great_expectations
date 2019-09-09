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

    def validate_input(self, render_object):
        pass

    def render(self, render_object, **kwargs):
        self.validate_input(render_object)
        object_type = self._find_ge_object_type(render_object)

        if object_type in ["validation_report", "expectations"]:
            raise ValueError(
                "Provide an evr_list, expectation_list, expectation or evr to a content block")

        if object_type in ["evr_list", "expectation_list"]:
            blocks = []
            for obj_ in render_object:
                expectation_type = self._get_expectation_type(obj_)

                content_block_fn = self._get_content_block_fn(expectation_type)

                if content_block_fn is not None:
                    result = content_block_fn(
                        obj_,
                        styling=self._get_element_styling(),
                        **kwargs
                    )
                    blocks += result

                else:
                    result = self._missing_content_block_fn(
                        obj_,
                        self._get_element_styling(),
                        **kwargs
                    )
                    if result is not None:
                        blocks += result

            if len(blocks) > 0:
                content_block = RenderedComponentContent(**{
                    "content_block_type": self._content_block_type,
                    self._content_block_type: blocks,
                    # TODO: This should probably be overridable via a parameter
                    "styling": self._get_content_block_styling(),
                })
                self._process_content_block(content_block)

                return content_block
                
            else:
                return None
        else:
            # TODO: Styling is not currently applied to non-list objects. It should be.
            expectation_type = self._get_expectation_type(render_object)

            content_block_fn = getattr(self, expectation_type, None)
            if content_block_fn is not None:
                return content_block_fn(render_object, **kwargs)
            else:
                return None

    def _process_content_block(self, content_block):
        header = self._get_header()
        if header != "":
            content_block.update({
                "header": header
            })

    def _get_content_block_fn(self, expectation_type):
        return getattr(self, expectation_type, None)

    def list_available_expectations(self):
        expectations = [attr for attr in dir(self) if attr[:7] == "expect_"]
        return expectations

    def _missing_content_block_fn(self, obj, styling, **kwargs):
        return []

    def _get_content_block_styling(self):
        return self._default_content_block_styling

    def _get_element_styling(self):
        return self._default_element_styling

    def _get_header(self):
        return self._default_header
