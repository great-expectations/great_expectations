import collections
from functools import reduce
import operator

def acts_as_a_number(var):
    try:
        0 + var
    except TypeError:
        return False
    else:
        return True

def make_dictionary_key(d):
    """

    :param d:
    :return:
    """
    return tuple(sorted([item for item in flatten_nested_dictionary_to_hashable_tuple_list(d)],\
      key=lambda item: '.'.join(item[0]) if type(item[0]) is tuple else item[0]))

def flatten_nested_dictionary_to_hashable_tuple_list(d, nested_key_tuple=()):
    """
    Assumption: leaf values can be either lists or primitive

    :param d:
    :param nested_key_tuple:
    :return:
    """
    for key, value in d.items():
        if isinstance(value, collections.Mapping):
            for inner_key, inner_value in flatten_nested_dictionary_to_hashable_tuple_list(value, nested_key_tuple=nested_key_tuple + (key,)):
                yield inner_key, inner_value
        else:
            yield (key if nested_key_tuple==() else (nested_key_tuple + (key,)), tuple(value) if type(value) is list else value)


def result_contains_numeric_observed_value(result):
    """

    :param result:
    :return:
    """
    return ('observed_value' in result['result'] \
            and acts_as_a_number(result['result'].get('observed_value'))) \
           and set(result['result'].keys()) <= set(
        ['observed_value', 'element_count', 'missing_count', 'missing_percent'])


def result_contains_unexpected_pct(result):
    """

    :param result:
    :return:
    """
    return 'unexpected_percent' in result['result'] \
           and result['expectation_config']['expectation_type'] != 'expect_column_values_to_be_in_set'



def get_nested_value_from_dict(d, key_path):
    return reduce(operator.getitem, key_path, d)

def set_nested_value_in_dict(d, key_path, value):
    for key in key_path[:-1]:
        d = d.setdefault(key, {})
    d[key_path[-1]] = value

