# This file is part of gen3_shared_repo_admin.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "DECamRawIngest",
)

import logging
from pathlib import Path
from typing import Any, Optional, Iterable, List, Set, Type, TYPE_CHECKING

from lsst.utils import doImport
from .._operation import AdminOperation, SimpleStatus

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef, FileDataset
    from lsst.obs.base import RawIngestTask
    from .._tool import RepoAdminTool


class _RememberDatasetRefs:
    """Callable to pass to `lsst.obs.base.RawIngestTask` to remember references
    to all ingested raws.
    """

    def __init__(self):
        self.refs = set()

    def __call__(self, fds: List[FileDataset]) -> None:
        for fd in fds:
            self.refs.update(fd.refs)

    refs: Set[DatasetRef]


class DECamRawIngest(AdminOperation):
    """A concrete `AdminOperation` that ingests DECam raw images via
    `lsst.obs.base.RawIngestTask`.

    DECam raws aren't compatible with the `RawIngest` operation used elsewhere
    because there's no way to extract exposure IDs from their filenames.  That
    limits how useful the status can be, but without that it's also much
    simpler, as we can delegate more to `lsst.obs.base.RawIngestTask`.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    paths : `Iterable` [ `Path` ]
        Directories that directly contain files to ingest.
    instrument_name : `str`
        Short/dimension name for the instrument whose raws are being ingested.
    task_class_name : `str`, optional
        Fully-qualified name of a `RawIngestTask` subclass to use; defaults
        to `RawIngestTask` itself.
    collection : `str`, optional
        Name of the `~lsst.daf.butler.CollectionType.RUN` collection to ingest
        raws into.
    transfer : `str`, option
        Datastore transfer mode for files.  Defaults to "direct", for
        absolute-URI ingest.
    tag : `str`, optional
        Name of a ``TAGGED`` collection to create to make this set of raws
        easier to find and use.  If `None` (default), no ``TAGGED`` collection
        will be created.
    """

    def __init__(
        self,
        name: str,
        paths: Iterable[Path],
        instrument_name: str,
        task_class_name: str = "lsst.obs.base.RawIngestTask",
        collection: Optional[str] = None,
        transfer: Optional[str] = "direct",
        tag: Optional[str] = None,
    ):
        super().__init__(name)
        self._paths = tuple(paths)
        self._instrument_name = instrument_name
        self.task_class_name = task_class_name
        self.collection = collection
        self.transfer = transfer
        self.tag = tag

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        SimpleStatus.check(self, tool).print_status(self, tool, indent)

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        logging.getLogger("daf.butler.Registry.insertDatasets").setLevel(logging.WARNING)
        logging.getLogger("daf.butler.datastores.FileDatastore.ingest").setLevel(logging.WARNING)
        checker = _RememberDatasetRefs()
        task = self.make_task(tool, on_success=checker)
        with SimpleStatus.run_context(self, tool):
            if not tool.dry_run:
                try:
                    task.run(self._paths, processes=tool.jobs, run=self.collection)
                finally:
                    if self.tag is not None:
                        tool.butler.registry.registerCollection(self.tag)
                        tool.butler.registry.associate(self.tag, checker.refs)

    @property
    def TaskClass(self) -> Type[RawIngestTask]:
        """Task class (`RawIngestTask` subclass) to run.
        """
        return doImport(self.task_class_name)

    def make_task(self, tool: RepoAdminTool, **kwargs: Any) -> RawIngestTask:
        """Construct the `RawIngestTask` instance to use in `run`.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        **kwargs
            Additional keyword arguments to forward to the task constructor.
        """
        config = self.TaskClass.ConfigClass()
        config.transfer = self.transfer
        config.failFast = True  # we do our own, per-exposure continue
        return self.TaskClass(config=config, butler=tool.butler, **kwargs)
