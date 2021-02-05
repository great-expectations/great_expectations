from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.util import num_to_str, substitute_none_for_missing


class ExpectColumnBootstrappedKsTestPValueToBeGreaterThan(TableExpectation):
    """Expect column values to be distributed similarly to the provided continuous partition. This expectation
    compares continuous distributions using a bootstrapped Kolmogorov-Smirnov test. It returns `success=True`
    if values in the column match the distribution of the provided partition.

    The expected cumulative density function (CDF) is constructed as a linear interpolation between the bins,
    using the provided weights. Consequently the test expects a piecewise uniform distribution using the bins
    from the provided partition object.

    ``expect_column_bootstrapped_ks_test_p_value_to_be_greater_than`` is a \
    :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.

    Args:
        column (str): \
            The column name.
        partition_object (dict): \
            The expected partition object (see :ref:`partition_object`).
        p (float): \
            The p-value threshold for the Kolmogorov-Smirnov test.
            For values below the specified threshold the expectation will return `success=False`,
            rejecting the \
            null hypothesis that the distributions are the same. \
            Defaults to 0.05.

    Keyword Args:
        bootstrap_samples (int): \
            The number bootstrap rounds. Defaults to 1000.
        bootstrap_sample_size (int): \
            The number of samples to take from the column for each bootstrap. A larger sample will increase
            the \
            specificity of the test. Defaults to 2 * len(partition_object['weights'])

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    Notes:
        These fields in the result object are customized for this expectation:
        ::

            {
                "observed_value": (float) The true p-value of the KS test
                "details": {
                    "bootstrap_samples": The number of bootstrap rounds used
                    "bootstrap_sample_size": The number of samples taken from
                        the column in each bootstrap round
                    "observed_cdf": The cumulative density function observed
                        in the data, a dict containing 'x' values and cdf_values
                        (suitable for plotting)
                    "expected_cdf" (dict):
                        The cumulative density function expected based on the
                        partition object, a dict containing 'x' values and
                        cdf_values (suitable for plotting)
                    "observed_partition" (dict):
                        The partition observed on the data, using the provided
                        bins but also expanding from min(column) to max(column)
                    "expected_partition" (dict):
                        The partition expected from the data. For KS test,
                        this will always be the partition_object parameter
                }
            }

            """

    library_metadata = {
        "maturity": "production",
        "package": "great_expectations",
        "tags": [
            "core expectation",
            "column aggregate expectation",
            "needs migration to modular expectations api",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
    }

    metric_dependencies = tuple()
    success_keys = ()
    default_kwarg_values = {}

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        pass

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        pass
