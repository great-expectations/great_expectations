import re
import sys
from typing import Final, List, Optional, Tuple

import click

SUPPORTED_CLI_COLORS: Final[Tuple[str, ...]] = (
    "blue",
    "cyan",
    "green",
    "yellow",
    "red",
)


def cli_message(string: str) -> None:
    print(cli_colorize_string(string))


def cli_colorize_string(string: str) -> str:
    for color in SUPPORTED_CLI_COLORS:
        string = re.sub(
            f"<{color}>(.*?)</{color}>",
            click.style(r"\g<1>", fg=color),
            string,
            flags=re.DOTALL,  # the DOTALL flag means that `.` includes newlines for multiline comments inside these tags
        )
    return string


def display_not_implemented_message_and_exit() -> None:
    cli_message(
        "<red>This command is not yet implemented for the v3 (Batch Request) API</red>"
    )
    sys.exit(1)


def cli_message_list(
    string_list: List[str], list_intro_string: Optional[str] = None
) -> None:
    """Simple util function for displaying simple lists in cli"""
    if list_intro_string:
        cli_message(list_intro_string)
    for string in string_list:
        cli_message(string)


def action_list_to_string(action_list: list) -> str:
    """Util function for turning an action list into pretty string"""
    action_list_string = ""
    for idx, action in enumerate(action_list):
        action_list_string += f"{action['name']} ({action['action']['class_name']})"
        if idx == len(action_list) - 1:
            continue
        action_list_string += " => "
    return action_list_string


def cli_message_dict(
    dict_: dict,
    indent: int = 3,
    bullet_char: str = "-",
    message_list: Optional[list] = None,
    recursion_flag: bool = False,
) -> None:
    """Util function for displaying nested dicts representing ge objects in cli"""
    if message_list is None:
        message_list = []
    if dict_.get("name"):
        name = dict_.pop("name")
        message = f"{' ' * indent}<cyan>name:</cyan> {name}"
        message_list.append(message)
    if dict_.get("module_name"):
        module_name = dict_.pop("module_name")
        message = f"{' ' * indent}<cyan>module_name:</cyan> {module_name}"
        message_list.append(message)
    if dict_.get("class_name"):
        class_name = dict_.pop("class_name")
        message = f"{' ' * indent}<cyan>class_name:</cyan> {class_name}"
        message_list.append(message)
    if dict_.get("action_list"):
        action_list = dict_.pop("action_list")
        action_list_string = action_list_to_string(action_list)
        message = f"{' ' * indent}<cyan>action_list:</cyan> {action_list_string}"
        message_list.append(message)
    sorted_keys = sorted(dict_.keys())
    for key in sorted_keys:
        if key == "password":
            message = f"{' ' * indent}<cyan>password:</cyan> ******"
            message_list.append(message)
            continue
        if isinstance(dict_[key], dict):
            message = f"{' ' * indent}<cyan>{key}:</cyan>"
            message_list.append(message)
            cli_message_dict(
                dict_[key],
                indent=indent + 2,
                message_list=message_list,
                recursion_flag=True,
            )
        else:
            message = f"{' ' * indent}<cyan>{key}:</cyan> {str(dict_[key])}"
            message_list.append(message)
    if not recursion_flag:
        if bullet_char and indent > 1:
            first = message_list[0]
            new_first = first[:1] + bullet_char + first[2:]
            message_list[0] = new_first
        cli_message_list(message_list)
