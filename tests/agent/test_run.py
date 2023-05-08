from unittest.mock import patch

from great_expectations.agent import run_agent


@patch("great_expectations.agent.run.GXAgent")
def test_run_calls_cloud_agent(agent):
    run_agent()
    agent.assert_called_with()
    agent().run.assert_called_with()
