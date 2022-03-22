import io
from pathlib import Path
from typing import Optional, Union

from ruamel.yaml import YAML


class YamlHandler:
    """
    Facade class designed to be a lightweight wrapper around YAML serialization.
    For all YAML-related activities in Great Expectations, this is the entry point.

    Note that this is meant to be library agnostic - the underlying implementation does not
    matter as long as we fulfill the following contract:
        * load
        * dump
    """

    _handler: YAML = YAML(typ="safe")

    @staticmethod
    def load(stream: Union[io.TextIOWrapper, str]) -> dict:
        """Converts a YAML input stream into a Python dictionary.

        Args:
            stream: The input stream to read in.

        Returns:
            The deserialized dictionary form of the input stream.
        """
        return YamlHandler._handler.load(stream=stream)

    @staticmethod
    def dump(
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
            return YamlHandler._dump(data=data, stream=stream, **kwargs)
        return YamlHandler._dump_and_return_value(data=data, **kwargs)

    @staticmethod
    def _dump(data: dict, stream, **kwargs) -> None:
        """If an input stream has been provided, modify it in place."""
        YamlHandler._handler.dump(data=data, stream=stream, **kwargs)

    @staticmethod
    def _dump_and_return_value(data: dict, **kwargs) -> str:
        """If an input stream hasn't been provided, generate one and return the value."""
        stream = io.StringIO()
        YamlHandler._handler.dump(data=data, stream=stream, **kwargs)
        return stream.getvalue()
