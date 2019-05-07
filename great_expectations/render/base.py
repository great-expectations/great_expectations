class Renderer(object):

    @classmethod
    def validate_input(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def _render_to_json(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def render(cls, *args, **kwargs):
        raise NotImplementedError


class ViewModelRenderer(Renderer):

    @classmethod
    def render(cls, *args, **kwargs):
        cls.validate_input(*args, **kwargs)
        cls._render_to_json(*args, **kwargs)
        raise NotImplementedError
