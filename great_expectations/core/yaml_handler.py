import io
from pathlib import Path
from typing import Optional, Union

from ruamel.yaml import YAML


class YamlHandler:
    """
    Facade class designed to be a lightweight wrapper around YAML serialization.
    For all YAML-related activities in Great Expectations, this is the entry point.
    """

    _handler = YAML(typ="safe")

    @staticmethod
    def load(stream: Union[io.TextIOWrapper, str]) -> dict:
        """Converts a YAML input stream into a Python dictionary.

        Args:
            stream: The input stream to read in.

        Returns:
            The deserialized dictionary form of the input stream.
        """
        return YamlHandler._handler.load(stream)

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
            For StringIO streams, the str that results from _handler.dump(), None otherwise, as the _handler.dump()
            will exercise the handler accordingly.
        """
        inefficient = False
        if stream is None:
            inefficient = True
            stream = io.StringIO()

        YamlHandler._handler.dump(data, stream, **kwargs)

        if inefficient:
            return stream.getvalue()
