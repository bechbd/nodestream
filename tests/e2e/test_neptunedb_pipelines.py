from contextlib import contextmanager
from os import environ
from pathlib import Path

import pytest

from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import Project, RunRequest


@pytest.fixture
def project():
    return Project.read_from_file(Path("tests/e2e/pipelines/neptunedb/nodestream.yaml"))


"""import boto3
neptune_db = boto3.client("neptunedata", endpoint_url=f"https://{neptune_config.host}:{neptune_config.port}")
neptune_db_response = neptune_db.execute_gremlin_query(
    gremlinQuery='''
g
// Collect all tech features of TX 350 FWD
.V().has("Vehicle Model","model_name","TX 350 FWD")
    .out("has_feature")
    .filter(values("category").is('tech'))
    .aggregate("TX350features")

// Collect all tech features of TX 500h F SPORT Performance Luxury AWD
// with the exception of TX 350 FWD features.
.V().has("Vehicle Model","model_name","TX 500h F SPORT Performance Luxury AWD")
    .out("has_feature")
    .filter(values("category").is('tech'))
    .where(without("TX350features"))
    .dedup()
    .valueMap(true)
    ''',
)
print(neptune_db_response)
        """
        
def validate_airports(session):
    result = session.run(
        """
        MATCH (a:Airport)
        RETURN count(a) AS count
        """
    )

    assert result.single()["count"] == 1000


def validate_airport_country(session):
    result = session.run(
        """
        MATCH (a:Airport{identifier: "05pn"})-[:WITHIN]->(c:Country)
        RETURN c.code as country
        """
    )

    assert result.single()["country"] == "us"


def validate_fifa_player_count(session):
    result = session.run(
        """
        MATCH (p:Player:Person)
        RETURN count(p) AS count
        """
    )

    assert result.single()["count"] == 100


def validate_fifa_mo_club(session):
    result = session.run(
        """
        MATCH (p:Player{name: "Mohamed Salah"})-[:PLAYS_FOR]->(t:Team)
        RETURN t.name as club
        """
    )

    assert result.single()["club"] == "Liverpool"


@pytest.mark.asyncio
@pytest.mark.e2e
@pytest.mark.parametrize("neptune_version", ["1.3.0.0"])
@pytest.mark.parametrize(
    "pipeline_name,validations",
    [
        ("airports", [validate_airports, validate_airport_country])
    ],
)
async def test_neptune_pipeline(
    project, pipeline_name, validations, neptune_version
):
    await project.run(
        RunRequest(
            pipeline_name,
            PipelineInitializationArguments(),
            PipelineProgressReporter(),
        )
    )

    for validator in validations:
        validator()
