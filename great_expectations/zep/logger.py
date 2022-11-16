import logging
import sys


def init_logger(level: int = logging.WARNING):
    try:
        from loguru import logger

        class InterceptHandler(logging.Handler):
            def emit(self, record):
                # Get corresponding Loguru level if it exists.
                try:
                    level = logger.level(record.levelname).name
                except ValueError:
                    level = record.levelno

                # Find caller from where originated the logged message.
                frame, depth = sys._getframe(6), 6
                while frame and frame.f_code.co_filename == logging.__file__:
                    frame = frame.f_back
                    depth += 1

                logger.opt(depth=depth, exception=record.exc_info).log(
                    level, record.getMessage()
                )

        logging.basicConfig(handlers=[InterceptHandler()], level=level, force=True)  # type: ignore[call-arg]
    except ImportError:
        logging.basicConfig(level=level, format="%(levelname)s:%(name)s | %(message)s")
