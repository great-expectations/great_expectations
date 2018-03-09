import pytest


@pytest.fixture(scope="module",
                params=['PandasDataSet', 'SqlAlchemyDataSet'])
def dataset_type(request):
    return request.param