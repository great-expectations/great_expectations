library_module_name = "dlt_mock_library_injected"


def expect(name: str, condition: str):
    print(
        f'dlt_mock_library_injected.expect("{name}", "{condition}") from inside dlt_mock_library_injected'
    )
