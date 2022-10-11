"""
POC for bootstrapping context.sources using a module namespace instead of a Datasource class.
"""


class DataContext:
    _context = None

    @classmethod
    def get_context(cls) -> "DataContext":
        if not cls._context:
            cls._context = DataContext()

        return cls._context

    @property
    def sources(self):
        from great_expectations.zep import sources_module_poc

        return sources_module_poc


def get_context() -> DataContext:
    context = DataContext.get_context()
    return context


if __name__ == "__main__":
    context = get_context()
    context.sources.add_pandas("taxi")
    context.sources.add_sql("sql_taxi")
