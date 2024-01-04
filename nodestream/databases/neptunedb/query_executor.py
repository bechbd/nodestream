from logging import getLogger
from typing import Iterable
import boto3

from ...model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ...schema.indexes import FieldIndex, KeyIndex
from ..query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)
from .ingest_query_builder import NeptuneDBIngestQueryBuilder
from .query import Query


class NeptuneQueryExecutor(QueryExecutor):
    def __init__(
        self,
        client: boto3.client,
        ingest_query_builder: NeptuneDBIngestQueryBuilder
    ) -> None:
        self.client = client
        self.ingest_query_builder = ingest_query_builder
        self.logger = getLogger(self.__class__.__name__)

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation, nodes
            )
        )
        await self.execute(
            batched_query.as_query(self.ingest_query_builder.apoc_iterate),
            log_result=True,
        )

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_relationship_query_batch(
                shape, relationships
            )
        )
        await self.execute(
            batched_query.as_query(self.ingest_query_builder.apoc_iterate),
            log_result=True,
        )

    async def upsert_key_index(self, index: KeyIndex):
        pass

    async def upsert_field_index(self, index: FieldIndex):
        pass

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        query = self.ingest_query_builder.generate_ttl_query_from_configuration(config)
        await self.execute(query)

    async def execute_hook(self, hook: IngestionHook):
        query_string, params = hook.as_cypher_query_and_parameters()
        await self.execute(Query(query_string, params))

    async def execute(self, query: Query, log_result: bool = False):
        self.logger.debug(
            "Executing Cypher Query to Neptune",
            extra={
                "query": query.query_statement,
                "uri": self.driver._pool.address.host,
            },
        )

        result = await self.driver.execute_query(
            query.query_statement,
            query.parameters,
            database_=self.database_name,
        )
        if log_result:
            for record in result.records:
                self.logger.info(
                    "Gathered Query Results",
                    extra=dict(**record, query=query.query_statement),
                )
