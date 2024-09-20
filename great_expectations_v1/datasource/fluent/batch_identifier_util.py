from great_expectations_v1.constants import DATAFRAME_REPLACEMENT_STR
from great_expectations_v1.core import IDDict


def make_batch_identifier(identifer_dict: dict) -> IDDict:
    batch_id: IDDict
    if "dataframe" in identifer_dict:
        batch_id = IDDict(identifer_dict, dataframe=DATAFRAME_REPLACEMENT_STR)
    else:
        batch_id = IDDict(identifer_dict)
    return batch_id
