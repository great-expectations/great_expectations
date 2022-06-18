class GxExperimentalWarning(Warning):
    """Warns when a given feature is experimental

    Example usage:

        warnings.warn(
            "\n================================================================================\n" \
            "ConfiguredPandasDatasource is an experimental feature of Great Expectations\n" \
            "You should consider the API to be unstable.\n" \
            "\n" \
            "You can disable this warning by calling: \n" \
            "from great_expectations.warnings import GxExperimentalWarning\n" \
            "warnings.simplefilter(action=\"ignore\", category=GxExperimentalWarning)\n" \
            "\n" \
            "If you have questions or feedback, please chime in at\n" \
            "https://github.com/great-expectations/great_expectations/discussions/DISCUSSION-ID-GOES-HERE\n" \
            "================================================================================\n",
            GxExperimentalWarning,
        )
    
    """

    pass


class GxRuntimeAssetWarning(Warning):
    pass