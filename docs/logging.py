class Logger:
    """Simple logger for printing to console during docs build"""

    @staticmethod
    def print_header(string: str) -> None:
        LINE = "================================================================================"
        Logger.print(LINE)
        Logger.print(string)
        Logger.print(LINE)

    @staticmethod
    def print(string: str) -> None:
        ORANGE = "\033[38;5;208m"
        END = "\033[1;37;0m"
        print(ORANGE + string + END)
