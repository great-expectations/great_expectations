import pytest

from tests.integration.conftest import DataSourceType, parameterize_batch_for_data_sources


@pytest.mark.parametrize("even_more", [pytest.param("a", id="a"), pytest.param("b", id="b")])
@parameterize_batch_for_data_sources(
    types=[DataSourceType.FOO, DataSourceType.BAR],
    data=[1, 2],
    # description="test_stuff",
)
@pytest.mark.parametrize("more", [pytest.param(1, id="one"), pytest.param(2, id="two")])
def test_stuff(batch_for_datasource, more, even_more) -> None:
    assert batch_for_datasource == [1, 2, 3]
    assert more in (1, 2)
    assert even_more in ("a, b")
