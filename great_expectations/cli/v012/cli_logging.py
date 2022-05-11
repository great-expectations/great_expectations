import logging
import warnings

warnings.filterwarnings("ignore")
logger = logging.getLogger("great_expectations.cli")


def _set_up_logger():
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    formatter = logging.Formatter("%(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    module_logger = logging.getLogger("great_expectations")
    module_logger.addHandler(handler)
    module_logger.setLevel(level=logging.WARNING)
    return module_logger
