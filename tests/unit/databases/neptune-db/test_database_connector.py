from hamcrest import assert_that, equal_to, instance_of

from nodestream.databases.neptunedb import NeptuneDBDatabaseConnector

def test_make_query_executor(mocker):
    connector = NeptuneDBDatabaseConnector(
        driver=mocker.Mock(),
        ingest_query_builder=mocker.Mock()
    )
    executor = connector.make_query_executor()
    assert_that(executor.driver, equal_to(connector.driver))
    assert_that(executor.ingest_query_builder, equal_to(connector.ingest_query_builder))


def test_make_type_retriever(mocker):
    connector = NeptuneDBDatabaseConnector(
        driver=mocker.Mock(),
        ingest_query_builder=mocker.Mock()
    )
    retriever = connector.make_type_retriever()
    assert_that(retriever.connector, equal_to(connector))


def test_from_file_data():
    from nodestream.databases.neptunedb.ingest_query_builder import NeptuneDBIngestQueryBuilder
    connector = NeptuneDBDatabaseConnector.from_file_data(
        host="http://localhost",
        port = 443,
        region_name = 'us-west-2'
    )
# TODO Put a more complete suite of test cases in here
    assert_that(connector, equal_to(connector))