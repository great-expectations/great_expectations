def is_value_greater_than_threshold(value, threshold):
    return value > threshold


def is_value_greater_than_or_equal_to_threshold(value, threshold):
    return value >= threshold


def is_value_less_than_threshold(value, threshold):
    return value < threshold


def is_value_less_than_or_equal_to_threshold(value, threshold):
    return value <= threshold


def is_value_between_bounds(
    value,
    lower_bound,
    upper_bound,
    inclusive=True,
):
    if inclusive:
        return is_value_less_than_or_equal_to_threshold(
            value, upper_bound
        ) and is_value_greater_than_or_equal_to_threshold(value, lower_bound)
    else:
        return is_value_less_than_threshold(
            value, upper_bound
        ) and is_value_greater_than_threshold(value, lower_bound)


def is_value_outside_bounds(value, lower_bound, upper_bound, inclusive=True):
    if inclusive:
        return is_value_less_than_or_equal_to_threshold(
            value, lower_bound
        ) and is_value_greater_than_or_equal_to_threshold(value, upper_bound)
    else:
        return is_value_less_than_threshold(
            value, lower_bound
        ) and is_value_greater_than_threshold(value, upper_bound)


def replace_generic_operator_in_report_keys(
    provided_keys_dict,
    all_keys,
):
    if "*" in provided_keys_dict:
        general_value = provided_keys_dict.pop("*")
        for key in all_keys:
            if key not in provided_keys_dict:
                provided_keys_dict[key] = general_value
    return provided_keys_dict
