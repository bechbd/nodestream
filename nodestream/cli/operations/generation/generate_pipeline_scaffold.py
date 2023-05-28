from typing import Iterable
from pathlib import Path

from cleo.commands.command import Command

from ....utilities import pretty_print_yaml_to_file
from ..operation import Operation

# TODO: Fill in basic pipeline. How do we dump the Jmespath?
SIMPLE_PIPELINE = []

WRITER_CONFIG_BY_DATABASE = {
    "neo4j": {
        "implementaton": "nodestream.databases:GraphDatabaseWriter",
        "arguments": {
            "batch_size": 1000,
            "database": "neo4j",
            "uri": "bolt://localhost:7687",
            "username": "neo4j",
            "password": "neo4j123",
        },
    }
}


class GeneratePipelineScaffold(Operation):
    def __init__(self, project_root: Path, database_name: str) -> None:
        self.project_root = project_root
        self.database_name = database_name

    async def perform(self, _: Command) -> Iterable[Path]:
        path = self.prepare_file_path()
        self.make_pipeline_at_path(path)
        return [path]

    def prepare_file_path(self) -> Path:
        pipeline_dir = self.project_root / "pipelines"
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        return pipeline_dir / "sample.yaml"

    def make_pipeline_at_path(self, path: Path):
        steps = SIMPLE_PIPELINE.copy()
        steps.append(WRITER_CONFIG_BY_DATABASE[self.database_name])
        pretty_print_yaml_to_file(path, steps)
