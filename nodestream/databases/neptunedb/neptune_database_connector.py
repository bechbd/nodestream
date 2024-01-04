import boto3

from ..copy import TypeRetriever
from ..query_executor import QueryExecutor
from .ingest_query_builder import NeptuneDBIngestQueryBuilder
from typing import Any, Dict, List, Optional, Tuple, Union
from ..database_connector import DatabaseConnector

class NeptuneDBDatabaseConnector(DatabaseConnector, alias="neptunedb"):
    @classmethod
    def from_file_data(
        cls,
        host: str,
        region: str,
        database_name: str = "neptunedb",
        **kwargs
    ):
        # Make this use boto3
        return cls(
            client = boto3.client('neptunedata', region_name = region, endpoint_url = host),
            ingest_query_builder=NeptuneDBIngestQueryBuilder
        )

    def __init__(
        self,
        client,
        ingest_query_builder: NeptuneDBIngestQueryBuilder
    ) -> None:
        self.client = client
        self.ingest_query_builder = ingest_query_builder

    def make_query_executor(self) -> QueryExecutor:
        from .query_executor import NeptuneQueryExecutor

        return NeptuneQueryExecutor(
            self.client,
            self.ingest_query_builder
        )

    def make_type_retriever(self) -> TypeRetriever:
        from .type_retriever import NeptuneDBTypeRetriever

        return NeptuneDBTypeRetriever(self)
