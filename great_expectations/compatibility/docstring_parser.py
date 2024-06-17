from __future__ import annotations

import warnings

from great_expectations.compatibility.not_imported import NotImported

DOCSTRING_PARSER_NOT_IMPORTED = NotImported(
    "docstring_parser is not installed, please 'pip install docstring-parser'"
)
with warnings.catch_warnings():
    # Hide deprecation warnings emitted by compatibility dependencies, users have no control here.
    warnings.simplefilter("ignore", category=DeprecationWarning)
    try:
        import docstring_parser
    except ImportError:
        docstring_parser = DOCSTRING_PARSER_NOT_IMPORTED  # type: ignore[assignment]

    try:
        from docstring_parser import DocstringStyle
    except ImportError:
        DocstringStyle = DOCSTRING_PARSER_NOT_IMPORTED  # type: ignore[assignment,misc]
