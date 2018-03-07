import unittest

class TestContainerIntergration(unittest.TestCase):

    def test_postgresql(self):
        import sqlalchemy
        from sqlalchemy.engine import create_engine

        E = create_engine('postgresql://test:test@docker.for.mac.host.internal:5432/test')
        E.connect()

    def test_pyspark(self):
        import pyspark

        sc = pyspark.SparkContext()
