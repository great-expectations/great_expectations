from great_expectations.datasource.generator import GlobReaderGenerator


def test_batch_generator_class_name():
    generator = GlobReaderGenerator()
    generator_config = generator.get_config()

    assert generator_config["class_name"] == "GlobReaderGenerator"