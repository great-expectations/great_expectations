import json

from ..base import Renderer


class SnippetRenderer(Renderer):

    @classmethod
    def validate_input(cls, expectation=None, evr=None):
        return True

    # @classmethod
    # def render(cls, expectation=None, evr=None):
    #     cls.validate_input(*args, **kwargs)
    #     cls._render_to_json(*args, **kwargs)
    #     raise NotImplementedError
