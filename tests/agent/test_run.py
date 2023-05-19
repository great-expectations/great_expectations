from great_expectations.agent import run


def test_run_calls_gx_agent(mocker):
    agent = mocker.patch("great_expectations.agent.run.GXAgent")
    run()
    agent.assert_called_with()
    agent().run.assert_called_with()
