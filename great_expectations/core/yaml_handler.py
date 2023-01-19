import io
from pathlib import Path
from typing import Optional, Union

from ruamel.yaml import YAML


class YAMLHandler:
    """
    Facade class designed to be a lightweight wrapper around YAML serialization.
    For all YAML-related activities in Great Expectations, this is the entry point.

    Note that this is meant to be library agnostic - the underlying implementation does not
    matter as long as we fulfill the following contract:
        * load
        * dump

    Typical usage example:

        simple_yaml: str = f\"""
            name: test
            class_name: test_class
            module_name: test.test_class
        \"""
        yaml_handler = YAMLHandler()
        res: dict = yaml_handler.load(simple_yaml)

        example_dict: dict = dict(abc=1)
        yaml_handler = YAMLHandler()
        yaml_handler.dump(example_dict)

    """

    def __init__(self) -> None:
        self._handler = YAML(typ="safe")
        # TODO: ensure this does not break all usage of ruamel in GX codebase.
        self._handler.indent(mapping=2, sequence=4, offset=2)
        self._handler.default_flow_style = False

    def load(self, stream: Union[io.TextIOWrapper, str]) -> dict:
        """Converts a YAML input stream into a Python dictionary.
        Args:
            stream: The input stream to read in. Although this function calls ruamel's load(), we
                use a slightly more restrictive type-hint than ruamel (which uses Any). This is in order to tightly
                bind the behavior of the YamlHandler class with expected YAML-related activities of Great Expectations.

        Returns:
            The deserialized dictionary form of the input stream.
        """
        return self._handler.load(stream=stream)

    def dump(
        self,
        data: dict,
        stream: Optional[Union[io.TextIOWrapper, io.StringIO, Path]] = None,
        **kwargs
    ) -> Optional[str]:
        """Converts a Python dictionary into a YAML string.

        Dump code has been adopted from:
        https://yaml.readthedocs.io/en/latest/example.html#output-of-dump-as-a-string

        Args:
            data: The dictionary to serialize into a Python object.
            stream: The output stream to modify. If not provided, we default to io.StringIO.

        Returns:
            If no stream argument is provided, the str that results from _handler.dump().
            Otherwise, None as the _handler.dump() works in place and will exercise the handler accordingly.
        """
        if stream:
            return self._dump(data=data, stream=stream, **kwargs)  # type: ignore[func-returns-value]
        return self._dump_and_return_value(data=data, **kwargs)

    def _dump(self, data: dict, stream, **kwargs) -> None:
        """If an input stream has been provided, modify it in place."""
        self._handler.dump(data=data, stream=stream, **kwargs)

    def _dump_and_return_value(self, data: dict, **kwargs) -> str:
        """If an input stream hasn't been provided, generate one and return the value."""
        stream = io.StringIO()
        self._handler.dump(data=data, stream=stream, **kwargs)
        return stream.getvalue()
