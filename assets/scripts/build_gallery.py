import importlib
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_gallery(include_core=True, include_contrib_experimental=True):
    logger.info("Getting base registered expectations list")
    import great_expectations

    core_expectations = (
        great_expectations.expectations.registry.list_registered_expectation_implementations()
    )

    if include_contrib_experimental:
        logger.info("Finding contrib modules")
        contrib_experimental_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "contrib",
            "experimental",
            "great_expectations_experimental",
        )
        sys.path.append(contrib_experimental_dir)
        expectations_module = importlib.import_module(
            "expectations", "great_expectations_experimental"
        )
        for expectation_module in expectations_module.__all__:
            logger.debug(f"Importing {expectation_module}")
            importlib.import_module(
                f"expectations.{expectation_module}", "great_expectations_experimental"
            )
        metrics_module = importlib.import_module(
            "metrics", "great_expectations_experimental"
        )
        for metrics_module in metrics_module.__all__:
            logger.debug(f"Importing {metrics_module}")
            importlib.import_module(
                f"metrics.{metrics_module}", "great_expectations_experimental"
            )

    # Above imports may have added additional expectations from contrib
    all_expectations = (
        great_expectations.expectations.registry.list_registered_expectation_implementations()
    )

    if include_core:
        build_expectations = set(all_expectations)
    else:
        build_expectations = set(all_expectations) - set(core_expectations)

    logger.info(
        f"Preparing to build gallery metadata for expectations: {build_expectations}"
    )
    gallery_info = dict()
    for expectation in build_expectations:
        logger.debug(f"Running diagnostics for expectation: {expectation}")
        impl = great_expectations.expectations.registry.get_expectation_impl(
            expectation
        )
        diagnostics = impl().run_diagnostics()
        gallery_info[expectation] = diagnostics

    return gallery_info


if __name__ == "__main__":
    gallery_info = build_gallery(include_core=True, include_contrib_experimental=True)
    with open("./expectation_library.json", "w") as outfile:
        json.dump(gallery_info, outfile)
