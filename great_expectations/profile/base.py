import logging
import time
import warnings
from enum import Enum

from dateutil.parser import ParserError, parse

from great_expectations.core import RunIdentifier
from great_expectations.exceptions import GreatExpectationsError

from ..data_asset import DataAsset
from ..dataset import Dataset

logger = logging.getLogger(__name__)


class ProfilerDataType(Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    UNKNOWN = "unknown"


class ProfilerCardinality(Enum):
    NONE = "none"
    ONE = "one"
    TWO = "two"
    FEW = "few"
    VERY_FEW = "very few"
    MANY = "many"
    VERY_MANY = "very many"
    UNIQUE = "unique"


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
        expectation.meta[str(cls.__name__)] = {"confidence": "very low"}
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

        new_expectations = [
            cls.add_expectation_meta(exp) for exp in expectation_suite.expectations
        ]
        expectation_suite.expectations = new_expectations

        if "notes" not in expectation_suite.meta:
            expectation_suite.meta["notes"] = {
                "format": "markdown",
                "content": [
                    "_To add additional notes, edit the <code>meta.notes.content</code> field in the appropriate Expectation json file._"
                    # TODO: be more helpful to the user by piping in the filename.
                    # This will require a minor refactor to make more DataContext information accessible from this method.
                    # "_To add additional notes, edit the <code>meta.notes.content</code> field in <code>expectations/mydb/default/movies/BasicDatasetProfiler.json</code>_"
                ],
            }
        return expectation_suite

    @classmethod
    def profile(
        cls,
        data_asset,
        run_id=None,
        profiler_configuration=None,
        run_name=None,
        run_time=None,
    ):
        assert not (run_id and run_name) and not (
            run_id and run_time
        ), "Please provide either a run_id or run_name and/or run_time."
        if isinstance(run_id, str) and not run_name:
            warnings.warn(
                "String run_ids will be deprecated in the future. Please provide a run_id of type "
                "RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name "
                "and run_time (both optional). Instead of providing a run_id, you may also provide"
                "run_name and run_time separately.",
                DeprecationWarning,
            )
            try:
                run_time = parse(run_id)
            except ParserError:
                pass
            run_id = RunIdentifier(run_name=run_id, run_time=run_time)
        elif isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif not isinstance(run_id, RunIdentifier):
            run_name = run_name or "profiling"
            run_id = RunIdentifier(run_name=run_name, run_time=run_time)

        if not cls.validate(data_asset):
            raise GreatExpectationsError("Invalid data_asset for profiler; aborting")

        expectation_suite = cls._profile(
            data_asset, configuration=profiler_configuration
        )

        batch_kwargs = data_asset.batch_kwargs
        expectation_suite = cls.add_meta(expectation_suite, batch_kwargs)
        validation_results = data_asset.validate(
            expectation_suite, run_id=run_id, result_format="SUMMARY"
        )
        expectation_suite.add_citation(
            comment=str(cls.__name__) + " added a citation based on the current batch.",
            batch_kwargs=data_asset.batch_kwargs,
            batch_markers=data_asset.batch_markers,
            batch_parameters=data_asset.batch_parameters,
        )
        return expectation_suite, validation_results

    @classmethod
    def _profile(cls, dataset, configuration=None):
        raise NotImplementedError
