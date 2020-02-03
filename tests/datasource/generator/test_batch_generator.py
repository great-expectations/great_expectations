from great_expectations.datasource.generator import GlobReaderGenerator


def test_batch_generator_class_name(basic_pandas_datasource):
    generator = GlobReaderGenerator(datasource=basic_pandas_datasource)
    generator_config = generator.get_config()

    assert generator_config["class_name"] == "GlobReaderGenerator"
