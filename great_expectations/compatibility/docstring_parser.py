from great_expectations.compatibility.not_imported import NotImported

DOCSTRING_PARSER_NOT_IMPORTED = NotImported(
    "docstring_parser is not installed, please 'pip install docstring-parser'"
)

try:
    import docstring_parser
except ImportError:
    docstring_parser = DOCSTRING_PARSER_NOT_IMPORTED

try:
    from docstring_parser import DocstringStyle
except ImportError:
    DocstringStyle = DOCSTRING_PARSER_NOT_IMPORTED
