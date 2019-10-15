import six
import re

try:
    from termcolor import colored
except ImportError:
    colored = None


def cli_message(string):
    # the DOTALL flag means that `.` includes newlines for multiline comments inside these tags
    flags=re.DOTALL
    mod_string = re.sub(
        "<blue>(.*?)</blue>",
        colored("\g<1>", "blue"),
        string,
        flags=flags
    )
    mod_string = re.sub(
        "<cyan>(.*?)</cyan>",
        colored("\g<1>", "cyan"),
        mod_string,
        flags=flags
    )
    mod_string = re.sub(
        "<green>(.*?)</green>",
        colored("\g<1>", "green"),
        mod_string,
        flags=flags
    )
    mod_string = re.sub(
        "<yellow>(.*?)</yellow>",
        colored("\g<1>", "yellow"),
        mod_string,
        flags=flags
    )
    mod_string = re.sub(
        "<red>(.*?)</red>",
        colored("\g<1>", "red"),
        mod_string,
        flags=flags
    )

    six.print_(colored(mod_string))
