import time
import logging

from ..data_asset import DataAsset
from ..dataset import Dataset
from great_expectations.exceptions import GreatExpectationsError

logger = logging.getLogger(__name__)

class DataAssetProfiler(object):

    @classmethod
    def validate(cls, data_asset):
        return isinstance(data_asset, DataAsset)


class DatasetProfiler(DataAssetProfiler):

    @classmethod
    def validate(cls, dataset):
        return isinstance(dataset, Dataset)

    @classmethod
    def add_expectation_meta(cls, expectation):
        expectation.meta[str(cls.__name__)] = {
            "confidence": "very low"
        }
        return expectation

    @classmethod
    def add_meta(cls, expectation_suite, batch_kwargs=None):
        class_name = str(cls.__name__)
        expectation_suite.meta[class_name] = {
            "created_by": class_name,
            "created_at": time.time(),
        }

        if batch_kwargs is not None:
            expectation_suite.meta[class_name]["batch_kwargs"] = batch_kwargs

        new_expectations = [cls.add_expectation_meta(
            exp) for exp in expectation_suite.expectations]
        expectation_suite.expectations = new_expectations

        if not "notes" in expectation_suite.meta:
            expectation_suite.meta["notes"] = {
                "format": "markdown",
                "content": [
                    "_To add additional notes, edit the <code>meta.notes.content</code> field in the appropriate Expectation json file._"
                    #TODO: be more helpful to the user by piping in the filename.
                    #This will require a minor refactor to make more DataContext information accessible from this method.
                    # "_To add additional notes, edit the <code>meta.notes.content</code> field in <code>expectations/mydb/default/movies/BasicDatasetProfiler.json</code>_"
                ]
            }
        return expectation_suite

    @classmethod
    def profile(cls, data_asset, run_id=None):
        if not cls.validate(data_asset):
            raise GreatExpectationsError("Invalid data_asset for profiler; aborting")

        expectation_suite = cls._profile(data_asset)

        batch_kwargs = data_asset.batch_kwargs
        expectation_suite = cls.add_meta(expectation_suite, batch_kwargs)
        validation_results = data_asset.validate(expectation_suite, run_id=run_id, result_format="SUMMARY")
        return expectation_suite, validation_results

    @classmethod
    def _profile(cls, dataset):
        raise NotImplementedError
