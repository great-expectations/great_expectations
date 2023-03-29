import great_expectations as gx

if __name__ == "__main__":
    context = gx.get_context(cloud_mode=False)
    assert context, "get_context() failed or result evaluated falsey"
    print(f"Hello {type(context).__name__}!!")
