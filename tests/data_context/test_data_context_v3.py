def test_config_variables(empty_data_context_v3):
    context = empty_data_context_v3
    assert type(context.config_variables) == dict
    assert set(context.config_variables.keys()) == {"instance_id"}
