
'A registry of :class:`Schema <marshmallow.Schema>` classes. This allows for string\nlookup of schemas, which may be used with\nclass:`fields.Nested <marshmallow.fields.Nested>`.\n\n.. warning::\n\n    This module is treated as private API.\n    Users should not need to use this module directly.\n'
import typing
from great_expectations.marshmallow__shade.exceptions import RegistryError
if typing.TYPE_CHECKING:
    from great_expectations.marshmallow__shade import Schema
    SchemaType = typing.Type[Schema]
_registry = {}

def register(classname: str, cls: 'SchemaType') -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Add a class to the registry of serializer classes. When a class is\n    registered, an entry for both its classname and its full, module-qualified\n    path are added to the registry.\n\n    Example: ::\n\n        class MyClass:\n            pass\n\n        register('MyClass', MyClass)\n        # Registry:\n        # {\n        #   'MyClass': [path.to.MyClass],\n        #   'path.to.MyClass': [path.to.MyClass],\n        # }\n\n    "
    module = cls.__module__
    fullpath = '.'.join([module, classname])
    if ((classname in _registry) and (not any(((each.__module__ == module) for each in _registry[classname])))):
        _registry[classname].append(cls)
    elif (classname not in _registry):
        _registry[classname] = [cls]
    if (fullpath not in _registry):
        _registry.setdefault(fullpath, []).append(cls)
    else:
        _registry[fullpath] = [cls]
    return None

def get_class(classname: str, all: bool=False) -> typing.Union[(typing.List['SchemaType'], 'SchemaType')]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Retrieve a class from the registry.\n\n    :raises: marshmallow.exceptions.RegistryError if the class cannot be found\n        or if there are multiple entries for the given class name.\n    '
    try:
        classes = _registry[classname]
    except KeyError as error:
        raise RegistryError('Class with name {!r} was not found. You may need to import the class.'.format(classname)) from error
    if (len(classes) > 1):
        if all:
            return _registry[classname]
        raise RegistryError('Multiple classes with name {!r} were found. Please use the full, module-qualified path.'.format(classname))
    else:
        return _registry[classname][0]
