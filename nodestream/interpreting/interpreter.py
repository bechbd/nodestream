from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Iterable

from ..model import (
    AggregatedIntrospectionMixin,
    InterpreterContext,
    IntrospectableIngestionComponent,
)
from ..pipeline import Flush, Step
from .interpretation import Interpretation
from .record_decomposers import RecordDecomposer


class InterpretationPass(IntrospectableIngestionComponent, ABC):
    @classmethod
    def from_file_arguments(self, args):
        if args is None:
            return NullInterpretationPass()

        if len(args) > 0 and isinstance(args[0], list):
            return MultiSequenceInterpretationPass.from_file_arguments(args)

        return SingleSequenceIntepretationPass.from_file_arguments(args)

    @abstractmethod
    def apply_interpretations(self, context: InterpreterContext):
        pass


class NullInterpretationPass(AggregatedIntrospectionMixin, InterpretationPass):
    def apply_interpretations(self, context: InterpreterContext):
        yield context

    def all_subordinate_components(
        self,
    ) -> Iterable[IntrospectableIngestionComponent]:
        return []


class MultiSequenceInterpretationPass(AggregatedIntrospectionMixin, InterpretationPass):
    @classmethod
    def from_file_arguments(cls, args):
        return cls(*(InterpretationPass.from_file_arguments(arg) for arg in args))

    def __init__(self, *passes: InterpretationPass) -> None:
        self.passes = passes

    def apply_interpretations(self, context: InterpreterContext):
        for interpretation_pass in self.passes:
            provided_subcontext = deepcopy(context)
            for res in interpretation_pass.apply_interpretations(provided_subcontext):
                yield res

    def all_subordinate_components(self) -> Iterable[IntrospectableIngestionComponent]:
        yield from self.passes


class SingleSequenceIntepretationPass(AggregatedIntrospectionMixin, InterpretationPass):
    @classmethod
    def from_file_arguments(cls, interpretation_arg_list):
        interpretations = (
            Interpretation.from_file_arguments(**args)
            for args in interpretation_arg_list
        )
        return cls(*interpretations)

    def __init__(self, *interpretations: Interpretation):
        self.interpretations = interpretations

    def apply_interpretations(self, context: InterpreterContext):
        for interpretation in self.interpretations:
            interpretation.interpret(context)
        yield context

    def all_subordinate_components(self) -> Iterable[IntrospectableIngestionComponent]:
        yield from self.interpretations


class Interpreter(Step, AggregatedIntrospectionMixin, IntrospectableIngestionComponent):
    __slots__ = (
        "before_iteration",
        "interpretations",
        "decomposer",
    )

    @classmethod
    def __declarative_init__(
        cls, interpretations, before_iteration=None, iterate_on=None
    ):
        return cls(
            before_iteration=InterpretationPass.from_file_arguments(before_iteration),
            interpretations=InterpretationPass.from_file_arguments(interpretations),
            decomposer=RecordDecomposer.from_iteration_arguments(iterate_on),
        )

    def __init__(
        self,
        before_iteration: InterpretationPass,
        interpretations: InterpretationPass,
        decomposer: RecordDecomposer,
    ) -> None:
        self.before_iteration = before_iteration
        self.interpretations = interpretations
        self.decomposer = decomposer

    async def handle_async_record_stream(self, record_stream):
        # Step 1: Emit any indexes that need to be created.
        # Step 2: Iterate through the stream and emit the appropriate ingestable objects.
        # NOTE: If any record is a flush, do nothing and pass it down stream.
        for index in self.gather_used_indexes():
            yield index

        async for record in record_stream:
            if record is Flush:
                yield record
                continue

            for output_context in self.interpret_record(record):
                yield output_context.desired_ingest

    def interpret_record(self, record):
        context = InterpreterContext.fresh(record)
        self.before_iteration.apply_interpretations(context)
        for sub_context in self.decomposer.decompose_record(context):
            yield from self.interpretations.apply_interpretations(sub_context)

    def all_subordinate_components(self) -> Iterable[IntrospectableIngestionComponent]:
        yield self.before_iteration
        yield self.interpretations
