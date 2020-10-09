from great_expectations.datasource.batch_kwargs_generator import (
    GlobReaderBatchKwargsGenerator,
)


def test_batch_kwargs_generator_class_name(basic_pandas_datasource):
    generator = GlobReaderBatchKwargsGenerator(datasource=basic_pandas_datasource)
    generator_config = generator.get_config()

    assert generator_config["class_name"] == "GlobReaderBatchKwargsGenerator"
