import logging
import warnings

warnings.filterwarnings("ignore")

logging.getLogger(
    "great_expectations.datasource.generator.in_memory_generator"
).setLevel(logging.CRITICAL)
logging.getLogger(
    "great_expectations.dataset.sqlalchemy_dataset"
).setLevel(logging.CRITICAL)
logging.getLogger(
    "great_expectations.profile.sample_expectations_dataset_profiler"
).setLevel(logging.CRITICAL)

# Take over the entire GE module logging namespace when running CLI
logger = logging.getLogger("great_expectations")


def _set_up_logger():
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
