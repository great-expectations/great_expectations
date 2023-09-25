from great_expectations_cloud.agent import GXAgent


def run_agent():
    """Run an instance of the GXAgent."""
    agent = GXAgent()
    agent.run()


if __name__ == "__main__":
    run_agent()
