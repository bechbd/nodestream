from abc import ABC, abstractmethod

from .ttl import TimeToLiveConfiguration
from .graph_objects import Node
from .indexes import KeyIndex, FieldIndex
from .desired_ingest import RelationshipWithNodes
from .ingestion_hooks import IngestionHookRunRequest


class IngestionStrategy(ABC):
    """An IngestionStrategy represents the methods taken to commit data to a Graph Database.

    Within nodestream, many components gather and intend to commit changes to a Graph Database.
    How that data is committed and how it is stored internally is decoupled through the `IngestionStrategy` interface.

    Generally, your usage of nodestream is decoupled from `IngestionStrategy` unless you intend to provide an implementation
    of your own database writer. The writer API will give you a `DesiredIngestion` or other `Ingestable` object that needs a
    instance of an `IngestionStrategy` to apply operations to.
    """

    @abstractmethod
    def ingest_source_node(self, source: Node):
        """Given a provided instance of `Node`, ensure that it is commited to the GraphDatabase."""
        raise NotImplementedError

    @abstractmethod
    def ingest_relationship(self, relationship: RelationshipWithNodes):
        """Given a provided instance of `SourceNode`, ensure that the provided `Relationship` is commited to the database."""
        raise NotImplementedError

    @abstractmethod
    def run_hook(self, request: IngestionHookRunRequest):
        raise NotImplementedError

    @abstractmethod
    def upsert_key_index(self, index: "KeyIndex"):
        raise NotImplementedError

    @abstractmethod
    def upsert_field_index(self, index: "FieldIndex"):
        raise NotImplementedError

    @abstractmethod
    def perform_ttl_operation(self, config: "TimeToLiveConfiguration"):
        raise NotImplementedError