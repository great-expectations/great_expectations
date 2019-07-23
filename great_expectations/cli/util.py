import six
import re

try:
    from termcolor import colored
except ImportError:
    colored = None


def cli_message(string):
    mod_string = re.sub(
        "<blue>(.*?)</blue>",
        colored("\g<1>", "blue"),
        string
    )
    mod_string = re.sub(
        "<green>(.*?)</green>",
        colored("\g<1>", "green"),
        mod_string
    )
    mod_string = re.sub(
        "<yellow>(.*?)</yellow>",
        colored("\g<1>", "yellow"),
        mod_string
    )
    mod_string = re.sub(
        "<red>(.*?)</red>",
        colored("\g<1>", "red"),
        mod_string
    )

    six.print_(colored(mod_string))
