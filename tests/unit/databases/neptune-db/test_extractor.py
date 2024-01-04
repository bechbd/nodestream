import pytest
from hamcrest import assert_that, equal_to
import json

from nodestream.databases.neptunedb.extractor import NeptuneDBExtractor
from nodestream.databases.neptunedb.neptune_database_connector import NeptuneDBDatabaseConnector


@pytest.mark.asyncio
async def test_extract_records(mocker):
    mock_connector = mocker.patch(
        "nodestream.databases.neptunedb.extractor.NeptuneDBExtractor"
    )
    mock_connector.from_file_data.return_value = mock_connector
    mock_connector = NeptuneDBDatabaseConnector.from_file_data(
        host="https://air-routes-oc.cluster-cei5pmtr7fqq.us-west-2.neptune.amazonaws.com:8182",
        region = 'us-west-2')
    items = "[{name: 'test1'}, {name: 'test2'},{name: 'test3'}]"
    mock_connector.client.execute_open_cypher_query(
        openCypherQuery = f"UNWIND {items} as d CREATE (n) SET n=d"
    )

    extractor = NeptuneDBExtractor.from_file_data(
        query="MATCH (n) RETURN n.name as name",
        params={"test": "test"},
        limit=5,
        host="https://air-routes-oc.cluster-cei5pmtr7fqq.us-west-2.neptune.amazonaws.com:8182",
        region= 'us-west-2', 
        database= 'neptunedb'
    )

    result = [item async for item in extractor.extract_records()]
    print(result)
    assert_that(
        result, equal_to([{"name": "test1"}, {"name": "test2"}, {"name": "test3"}])
    )
