from abc import abstractmethod
from typing import Any, AsyncGenerator, Dict, Iterable, Optional

from ..model import InterpreterContext, StaticValueOrValueProvider, ValueProvider
from .step import Step


class Filter(Step):
    """A `Filter` takes a given record and evaluates whether or not it should continue downstream.

    `Filter` steps generally make up the middle of an ETL pipeline and are responsible
    for ensuring only relevant records make it through.
    """

    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        async for record in record_stream:
            if not self.should_filter(record):
                yield record

    @abstractmethod
    async def filter_record(self, record: Any) -> bool:
        raise NotImplementedError


class ValueMatcher:
    @classmethod
    def from_file_arguments(
        cls,
        value: StaticValueOrValueProvider,
        possibilities: Iterable[StaticValueOrValueProvider],
        normalization: Optional[Dict[str, Any]] = None,
    ):
        return cls(
            value_provider=ValueProvider.garuntee_value_provider(value),
            possibilities=ValueProvider.garuntee_provider_list(possibilities),
            normalization=(normalization or {}),
        )

    def __init__(
        self,
        value_provider: ValueProvider,
        possibilities: Iterable[ValueProvider],
        normalization: Dict[str, Any],
    ) -> None:
        self.value_provider = value_provider
        self.possibilities = possibilities
        self.normalization = normalization

    def does_match(self, context: InterpreterContext):
        actual_value = self.value_provider.normalize_single_value(
            context, **self.normalization
        )

        return any(
            possiblity_provider.normalize_single_value(context, **self.normalization)
            == actual_value
            for possiblity_provider in self.possibilities
        )


class ValuesMatchPossiblitiesFilter(Filter):
    @classmethod
    def __declarative_init__(cls, *, fields: Iterable[Dict[str, Any]]):
        value_matchers = [ValueMatcher.from_file_arguments(**field) for field in fields]
        return cls(value_matchers=value_matchers)

    def __init__(self, value_matchers: Iterable[ValueMatcher]):
        self.value_matchers = value_matchers

    async def filter_record(self, item):
        context_from_record = InterpreterContext(item, None)
        return not all(
            matcher.does_match(context_from_record) for matcher in self.value_matchers
        )