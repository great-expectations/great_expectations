import great_expectations as gx


def test_foo():
    context = gx.get_context()
    breakpoint()
    context.sources.add_pandas(
        "my_pandas",
    )
