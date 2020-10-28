import copy
import inspect
import logging
import os
import re
from collections import OrderedDict

from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigSchema,
)
from great_expectations.exceptions import MissingConfigVariableError
from great_expectations.util import load_class, verify_dynamic_loading_support

logger = logging.getLogger(__name__)


# TODO: Rename config to constructor_kwargs and config_defaults -> constructor_kwarg_default
# TODO: Improve error messages in this method. Since so much of our workflow is config-driven, this will be a *super* important part of DX.
def instantiate_class_from_config(config, runtime_environment, config_defaults=None):
    """Build a GE class from configuration dictionaries."""

    if config_defaults is None:
        config_defaults = {}

    config = copy.deepcopy(config)

    module_name = config.pop("module_name", None)
    if module_name is None:
        try:
            module_name = config_defaults.pop("module_name")
        except KeyError:
            raise KeyError(
                "Neither config : {} nor config_defaults : {} contains a module_name key.".format(
                    config, config_defaults,
                )
            )
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("module_name", None)

    verify_dynamic_loading_support(module_name=module_name)

    class_name = config.pop("class_name", None)
    if class_name is None:
        logger.warning(
            "Instantiating class from config without an explicit class_name is dangerous. Consider adding "
            "an explicit class_name for %s" % config.get("name")
        )
        try:
            class_name = config_defaults.pop("class_name")
        except KeyError:
            raise KeyError(
                "Neither config : {} nor config_defaults : {} contains a class_name key.".format(
                    config, config_defaults,
                )
            )
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("class_name", None)

    class_ = load_class(class_name=class_name, module_name=module_name)

    config_with_defaults = copy.deepcopy(config_defaults)
    config_with_defaults.update(config)
    if runtime_environment is not None:
        # If there are additional kwargs available in the runtime_environment requested by a
        # class to be instantiated, provide them
        argspec = inspect.getfullargspec(class_.__init__)[0][1:]

        missing_args = set(argspec) - set(config_with_defaults.keys())
        config_with_defaults.update(
            {
                missing_arg: runtime_environment[missing_arg]
                for missing_arg in missing_args
                if missing_arg in runtime_environment
            }
        )
        # Add the entire runtime_environment as well if it's requested
        if "runtime_environment" in missing_args:
            config_with_defaults.update({"runtime_environment": runtime_environment})

    try:
        class_instance = class_(**config_with_defaults)
    except TypeError as e:
        raise TypeError(
            "Couldn't instantiate class : {} with config : \n\t{}\n \n".format(
                class_name, format_dict_for_error_message(config_with_defaults)
            )
            + str(e)
        )

    return class_instance


def format_dict_for_error_message(dict_):
    # TODO : Tidy this up a bit. Indentation isn't fully consistent.

    return "\n\t".join("\t\t".join((str(key), str(dict_[key]))) for key in dict_)


def substitute_config_variable(
    template_str, config_variables_dict, dollar_sign_escape_string: str = r"\$"
):
    """
    This method takes a string, and if it contains a pattern ${SOME_VARIABLE} or $SOME_VARIABLE,
    returns a string where the pattern is replaced with the value of SOME_VARIABLE,
    otherwise returns the string unchanged. These patterns are case sensitive. There can be multiple
    patterns in a string, e.g. all 3 will be substituted in the following:
    $SOME_VARIABLE${some_OTHER_variable}$another_variable

    If the environment variable SOME_VARIABLE is set, the method uses its value for substitution.
    If it is not set, the value of SOME_VARIABLE is looked up in the config variables store (file).
    If it is not found there, the input string is returned as is.

    If the value to substitute is not a string, it is returned as-is.

    If the value to substitute begins with dollar_sign_escape_string it is not substituted.

    :param template_str: a string that might or might not be of the form ${SOME_VARIABLE}
            or $SOME_VARIABLE
    :param config_variables_dict: a dictionary of config variables. It is loaded from the
            config variables store (by default, "uncommitted/config_variables.yml file)
    :param dollar_sign_escape_string: a string that will be used in place of a `$` when substitution
            is not desired.

    :return: a string with values substituted, or the same object if template_str is not a string.
    """

    if template_str is None:
        return template_str

    # 1. Make substitutions for non-escaped patterns
    try:
        match = re.finditer(
            r"(?<!\\)\$\{(.*?)\}|(?<!\\)\$([_a-zA-Z][_a-zA-Z0-9]*)", template_str
        )
    except TypeError:
        # If the value is not a string (e.g., a boolean), we should return it as is
        return template_str

    for m in match:
        # Match either the first group e.g. ${Variable} or the second e.g. $Variable
        config_variable_name = m.group(1) or m.group(2)
        config_variable_value = config_variables_dict.get(config_variable_name)

        if config_variable_value is not None:
            if not isinstance(config_variable_value, str):
                return config_variable_value
            template_str = template_str.replace(m.group(), config_variable_value)
        else:
            raise MissingConfigVariableError(
                f"""\n\nUnable to find a match for config substitution variable: `{config_variable_name}`.
Please add this missing variable to your `uncommitted/config_variables.yml` file or your environment variables.
See https://great-expectations.readthedocs.io/en/latest/reference/data_context_reference.html#managing-environment-and-secrets""",
                missing_config_variable=config_variable_name,
            )

    # 2. Replace the "$"'s that had been escaped
    return template_str.replace(dollar_sign_escape_string, "$")


def substitute_all_config_variables(
    data, replace_variables_dict, dollar_sign_escape_string: str = r"\$"
):
    """
    Substitute all config variables of the form ${SOME_VARIABLE} in a dictionary-like
    config object for their values.

    The method traverses the dictionary recursively.

    :param data:
    :param replace_variables_dict:
    :return: a dictionary with all the variables replaced with their values
    """
    if isinstance(data, DataContextConfig):
        data = DataContextConfigSchema().dump(data)

    if isinstance(data, dict) or isinstance(data, OrderedDict):
        return {
            k: substitute_all_config_variables(v, replace_variables_dict)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [
            substitute_all_config_variables(v, replace_variables_dict) for v in data
        ]
    return substitute_config_variable(
        data, replace_variables_dict, dollar_sign_escape_string
    )


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
