from freezegun import freeze_time

from great_expectations.core.util import substitute_all_strftime_format_strings


@freeze_time("11/05/1955")
def test_substitute_all_strftime_format_strings():
    input_dict = {
        "month_no": "%m",
        "just_a_string": "Bloopy!",
        "string_with_month_word": "Today we are in the month %B!",
        "number": "90210",
        "escaped_percent": "'%%m' is the format string for month number",
        "inner_dict": {"day_word_full": "%A"},
        "list": ["a", 123, "%a"],
    }
    expected_output_dict = {
        "month_no": "11",
        "just_a_string": "Bloopy!",
        "string_with_month_word": "Today we are in the month November!",
        "number": "90210",
        "escaped_percent": "'%m' is the format string for month number",
        "inner_dict": {"day_word_full": "Saturday"},
        "list": ["a", 123, "Sat"],
    }
    assert substitute_all_strftime_format_strings(input_dict) == expected_output_dict
