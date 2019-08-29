import logging
from great_expectations.exceptions import GreatExpectationsError

logger = logging.getLogger(__name__)


# FIXME: update inheritance and review naming
class BasicMultiBatchValidationMetaAnalysisProfiler(object):

    @classmethod
    def _estimate_normal_mean(series):
        mean = series.mean()
        std = series.std()
        # samples = 4000
        samples = len(series)
        return (stats.t(samples - 1).rvs(size=samples) * std / (samples ** (1 / 2))) + mean

    @classmethod
    def profile(cls, meta_analysis_metrics):
        logger.debug("Starting BasicMultiBatchValidationMetaAnalysisProfiler profile")
        if not cls.validate(meta_analysis_metrics):
            raise GreatExpectationsError("Invalid meta_analysis_metrics; aborting;")


        return expectation_suite, None