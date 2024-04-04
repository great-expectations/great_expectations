import importlib
import pkgutil

# We import each module directly since we miss a lot of modules if we just
# `import great_expectations`
for module_info in pkgutil.walk_packages(["great_expectations"], prefix="great_expectations."):
    importlib.import_module(module_info.name)

from great_expectations._docs_decorators import public_api_introspector

print(public_api_introspector)
