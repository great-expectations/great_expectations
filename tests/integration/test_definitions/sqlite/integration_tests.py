from typing import List

from tests.integration.integration_test_fixture import IntegrationTestFixture

sqlite_integration_tests = []

connecting_to_your_data: List[IntegrationTestFixture] = []

partition_data: List[IntegrationTestFixture] = []

sample_data: List[IntegrationTestFixture] = []

sqlite_integration_tests += connecting_to_your_data
sqlite_integration_tests += partition_data
sqlite_integration_tests += sample_data
