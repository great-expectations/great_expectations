import logging
import warnings

warnings.filterwarnings("ignore")


###
# REVIEWER NOTE: THE ORIGINAL IMPLEMENTATION WAS HEAVY HANDED AND I BELIEVE WAS A TEMPORARY WORKAROUND.
# PLEASE CAREFULLY REVIEW TO ENSURE REMOVING THIS DOES NOT AFFECT DESIRED BEHAVIOR
###

def _set_up_logger():
    # Log to console with a simple formatter; used by CLI
    formatter = logging.Formatter("%(message)s")
    handler = logging.StreamHandler()
    handler.setLevel(level=logging.WARNING)
    handler.setFormatter(formatter)
    logger = logging.getLogger("great_expectations")
    logger.addHandler(handler)
