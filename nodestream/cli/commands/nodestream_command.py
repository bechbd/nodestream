import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from cleo.commands.command import Command
from cleo.io.outputs.output import Verbosity

from ...project import Project

if TYPE_CHECKING:
    from ..operations import Operation

DEFAULT_PROJECT_FILE = Path("nodestream.yaml")


class NodestreamCommand(Command):
    def handle(self):
        asyncio.run(self.handle_async())

    async def handle_async(self):
        raise NotImplementedError

    async def run_operation(self, opertaion: "Operation"):
        self.line(
            f"<info>Running: {opertaion.name}</info>", verbosity=Verbosity.VERBOSE
        )
        return await opertaion.perform(self)

    def get_project_path(self) -> Path:
        path = self.option("project")
        return DEFAULT_PROJECT_FILE if path is None else Path(path)

    def get_project(self) -> Project:
        return Project.from_file(self.get_project_path())

    @property
    def has_json_logging_set(self) -> bool:
        return self.option("json-logging")

    @property
    def scope(self) -> str:
        return self.option("scope")