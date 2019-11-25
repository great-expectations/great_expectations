import logging
import os
import errno
import six
import importlib
import copy
import re
from collections import OrderedDict

from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.exceptions import (
    PluginModuleNotFoundError,
    PluginClassNotFoundError,
    DataContextError)

logger = logging.getLogger(__name__)


def safe_mmkdir(directory, exist_ok=True):
    """Simple wrapper since exist_ok is not available in python 2"""
    if not isinstance(directory, six.string_types):
        raise TypeError("directory must be of type str, not {0}".format({
            "directory_type": str(type(directory))
        }))

    if not exist_ok:
        raise ValueError(
            "This wrapper should only be used for exist_ok=True; it is designed to make porting easier later")
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


# # TODO : Consider moving this into types.resource_identifiers.DataContextKey.
# # NOTE : We **don't** want to encourage stringification of keys, other than in tests, etc.
# # TODO : Rename to parse_string_to_data_context_key
# def parse_string_to_data_context_resource_identifier(string, separator="."):
#     string_elements = string.split(separator)
#
#     loaded_module = importlib.import_module("great_expectations.data_context.types.resource_identifiers")
#     class_ = getattr(loaded_module, string_elements[0])
#
#     class_instance = class_(*(string_elements[1:]))
#
#     return class_instance


def load_class(class_name, module_name):
    """Dynamically load a class from strings or raise a helpful error."""

    # TODO remove this nasty python 2 hack
    try:
        ModuleNotFoundError
    except NameError:
        ModuleNotFoundError = ImportError

    try:
        loaded_module = importlib.import_module(module_name)
        class_ = getattr(loaded_module, class_name)
    except ModuleNotFoundError as e:
        raise PluginModuleNotFoundError(module_name=module_name)
    except AttributeError as e:
        raise PluginClassNotFoundError(
            module_name=module_name,
            class_name=class_name
        )
    return class_


# TODO: Rename runtime_config to runtime_environment and pass it through as a typed object, rather than unpacking it.
# TODO: Rename config to constructor_kwargs and config_defaults -> constructor_kwarg_default
# TODO: Improve error messages in this method. Since so much of our workflow is config-driven, this will be a *super* important part of DX.
def instantiate_class_from_config(config, runtime_config, config_defaults=None):
    """Build a GE class from configuration dictionaries."""

    if config_defaults is None:
        config_defaults = {}

    config = copy.deepcopy(config)

    module_name = config.pop("module_name", None)
    if module_name is None:
        try:
            module_name = config_defaults.pop("module_name")
        except KeyError as e:
            raise KeyError("Neither config : {} nor config_defaults : {} contains a module_name key.".format(
                config, config_defaults,
            ))
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("module_name", None)

    class_name = config.pop("class_name", None)
    if class_name is None:
        logger.warning("Instantiating class from config without an explicit class_name is dangerous. Consider adding "
                       "an explicit class_name for %s" % config.get("name"))
        try:
            class_name = config_defaults.pop("class_name")
        except KeyError as e:
            raise KeyError("Neither config : {} nor config_defaults : {} contains a class_name key.".format(
                config, config_defaults,
            ))
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("class_name", None)

    class_ = load_class(class_name=class_name, module_name=module_name)

    config_with_defaults = copy.deepcopy(config_defaults)
    config_with_defaults.update(config)
    config_with_defaults.update(runtime_config)

    try:
        class_instance = class_(**config_with_defaults)
    except TypeError as e:
        raise TypeError("Couldn't instantiate class : {} with config : \n\t{}\n \n".format(
            class_name,
            format_dict_for_error_message(config_with_defaults)
        ) + str(e))

    return class_instance


def format_dict_for_error_message(dict_):
    # TODO : Tidy this up a bit. Indentation isn't fully consistent.

    return '\n\t'.join('\t\t'.join((str(key), str(dict_[key]))) for key in dict_)


def substitute_config_variable(template_str, config_variables_dict):
    """
    This method takes a string, and if it contains a pattern ${SOME_VARIABLE} or $SOME_VARIABLE,
    returns a string where the pattern is replaced with the value of SOME_VARIABLE,
    otherwise returns the string unchanged.

    If the environment variable SOME_VARIABLE is set, the method uses its value for substitution.
    If it is not set, the value of SOME_VARIABLE is looked up in the config variables store (file).
    If it is not found there, the input string is returned as is.

    :param template_str: a string that might or might not be of the form ${SOME_VARIABLE}
            or $SOME_VARIABLE
    :param config_variables_dict: a dictionary of config variables. It is loaded from the
            config variables store (by default, "uncommitted/config_variables.yml file)
    :return:
    """
    if template_str is None:
        return template_str

    try:
        match = re.search(r'\$\{(.*?)\}', template_str) or re.search(r'\$([_a-z][_a-z0-9]*)', template_str)
    except TypeError:
        # If the value is not a string (e.g., a boolean), we should return it as is
        return template_str

    ret = template_str

    if match:
        config_variable_value = os.getenv(match.group(1))
        if not config_variable_value:
            config_variable_value = config_variables_dict.get(match.group(1))

        if config_variable_value:
                if match.start() == 0 and match.end() == len(template_str):
                    ret = config_variable_value
                else:
                    ret = template_str[:match.start()] + config_variable_value + template_str[match.end():]

    return ret


def substitute_all_config_variables(data, replace_variables_dict):
    """
    Substitute all config variables of the form ${SOME_VARIABLE} in a dictionary-like
    config object for their values.

    The method traverses the dictionary recursively.

    :param data:
    :param replace_variables_dict:
    :return: a dictionary with all the variables replaced with their values
    """
    if isinstance(data, DataContextConfig):
        data = data.as_dict()

    if isinstance(data, dict) or isinstance(data, OrderedDict):
        return {k: substitute_all_config_variables(v, replace_variables_dict) for k, v in data.items()}
    elif isinstance(data, list):
        return [substitute_all_config_variables(v, replace_variables_dict) for v in data]
    return substitute_config_variable(data, replace_variables_dict)


def file_relative_path(dunderfile, relative_path):
    """
    This function is useful when one needs to load a file that is
    relative to the position of the current file. (Such as when
    you encode a configuration file path in source file and want
    in runnable in any current working directory)

    It is meant to be used like the following:
    file_relative_path(__file__, 'path/relative/to/file')

    H/T https://github.com/dagster-io/dagster/blob/8a250e9619a49e8bff8e9aa7435df89c2d2ea039/python_modules/dagster/dagster/utils/__init__.py#L34
    """
    return os.path.join(os.path.dirname(dunderfile), relative_path)