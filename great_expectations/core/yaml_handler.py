import io
from pathlib import Path
from typing import Optional, Union

from ruamel.yaml import YAML


class YAMLHandler:
    '\n    Facade class designed to be a lightweight wrapper around YAML serialization.\n    For all YAML-related activities in Great Expectations, this is the entry point.\n\n    Note that this is meant to be library agnostic - the underlying implementation does not\n    matter as long as we fulfill the following contract:\n        * load\n        * dump\n\n    Typical usage example:\n\n        simple_yaml: str = f"""\n            name: test\n            class_name: test_class\n            module_name: test.test_class\n        """\n        yaml_handler: YAMLHandler = YAMLHandler()\n        res: dict = yaml_handler.load(simple_yaml)\n\n        example_dict: dict = dict(abc=1)\n        yaml_handler: YAMLHandler = YAMLHandler()\n        yaml_handler.dump(example_dict)\n\n'

    def __init__(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._handler: YAML = YAML(typ="safe")
        self._handler.indent(mapping=2, sequence=4, offset=2)
        self._handler.default_flow_style = False

    def load(self, stream: Union[(io.TextIOWrapper, str)]) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Converts a YAML input stream into a Python dictionary.\n        Args:\n            stream: The input stream to read in. Although this function calls ruamel's load(), we\n                use a slightly more restrictive type-hint than ruamel (which uses Any). This is in order to tightly\n                bind the behavior of the YamlHandler class with expected YAML-related activities of Great Expectations.\n\n        Returns:\n            The deserialized dictionary form of the input stream.\n        "
        return self._handler.load(stream=stream)

    def dump(
        self,
        data: dict,
        stream: Optional[Union[(io.TextIOWrapper, io.StringIO, Path)]] = None,
        **kwargs,
    ) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Converts a Python dictionary into a YAML string.\n\n        Dump code has been adopted from:\n        https://yaml.readthedocs.io/en/latest/example.html#output-of-dump-as-a-string\n\n        Args:\n            data: The dictionary to serialize into a Python object.\n            stream: The output stream to modify. If not provided, we default to io.StringIO.\n\n        Returns:\n            If no stream argument is provided, the str that results from _handler.dump().\n            Otherwise, None as the _handler.dump() works in place and will exercise the handler accordingly.\n        "
        if stream:
            return self._dump(data=data, stream=stream, **kwargs)
        return self._dump_and_return_value(data=data, **kwargs)

    def _dump(self, data: dict, stream, **kwargs) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "If an input stream has been provided, modify it in place."
        self._handler.dump(data=data, stream=stream, **kwargs)

    def _dump_and_return_value(self, data: dict, **kwargs) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "If an input stream hasn't been provided, generate one and return the value."
        stream = io.StringIO()
        self._handler.dump(data=data, stream=stream, **kwargs)
        return stream.getvalue()
