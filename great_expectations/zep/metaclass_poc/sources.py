from typing import Callable, Dict, List

from great_expectations.zep.interfaces import Datasource

SourceFactoryFn = Callable[..., Datasource]


class _SourceFactories:

    __sources: Dict[str, Callable] = {}

    @classmethod
    def add_factory(cls, name: str, fn: SourceFactoryFn) -> None:
        """Add/Register a datasource factory function."""
        print(f"2. Adding {name} factory")
        prexisting = cls.__sources.get(name, None)
        if not prexisting:
            cls.__sources[name] = fn  # type: ignore[assignment]
        else:
            raise ValueError(f"{name} already exists")

    @property
    def factories(self) -> List[str]:
        return list(self.__sources.keys())

    def __getattr__(self, name):
        try:
            return self.__sources[name]
        except KeyError:
            raise AttributeError(name)

    # def __dir__(self) -> List[str]:
    #     # TODO: update to work for standard methods too
    #     # TODO: doesn't seem to work for jupyter-autocompletions
    #     return [k for k in self.__sources.keys()]
