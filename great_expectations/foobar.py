def my_function(v: str) -> str:
    if v == "foo":
        return "bar"
    elif v == "bar":
        return "foo"
    raise ValueError("wrong")
