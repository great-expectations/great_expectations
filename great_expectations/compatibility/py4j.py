from great_expectations.compatibility.not_imported import NotImported

PY4J_NOT_IMPORTED = NotImported("py4j is not installed, please 'pip install py4j'")

try:
    import py4j
except ImportError:
    py4j = PY4J_NOT_IMPORTED
