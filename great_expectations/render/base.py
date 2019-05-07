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
        class.validate_input(*args, **kwargs)
        class._render_to_json(*args, **kwargs)
        raise NotImplementedError
