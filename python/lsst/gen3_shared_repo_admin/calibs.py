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

__all__ = ("CalibrationOperation", "ConvertCalibrations", "WriteCuratedCalibrations",)

import logging
import re
from typing import Any, Optional, Tuple, TYPE_CHECKING

from ._operation import AdminOperation, SimpleStatus

if TYPE_CHECKING:
    from lsst.obs.base.gen2to3 import CalibRepo, ConvertRepoTask
    from lsst.obs.base import Instrument
    from ._tool import RepoAdminTool


class CalibrationOperation(AdminOperation):
    """An intermediate base class for `AdminOperation` classes that create
    calibration collections.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    instrument_name : `str`
        Short (dimension) name of the instrument.
    labels : `tuple` [ `str` ]
        Tuple of strings to include in the collection name(s).
    """
    def __init__(self, name: str, instrument_name: str, labels: Tuple[str, ...]):
        super().__init__(name)
        self.instrument_name = instrument_name
        self.labels = labels

    def instrument(self, tool: RepoAdminTool) -> Instrument:
        from lsst.obs.base import Instrument
        return Instrument.fromName(self.instrument_name, tool.butler.registry)

    def collection(self, tool: RepoAdminTool, *, instrument: Optional[Instrument] = None) -> str:
        if instrument is None:
            instrument = self.instrument(tool)
        return instrument.makeCalibrationCollectionName(*self.labels)


class WriteCuratedCalibrations(CalibrationOperation):
    """A concrete `AdminOperation` that writes curated calibrations.

    This operation assumes it is the only operation working with its output
    `~lsst.daf.butler.CollectionType.CALIBRATION` collection.
    """

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        from lsst.daf.butler.registry import MissingCollectionError
        try:
            tool.butler.registry.getCollectionType(self.collection(tool))
        except MissingCollectionError:
            print(f"{' '*indent}{self.name}: not started")
        else:
            print(f"{' '*indent}{self.name}: done")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            self.instrument(tool).writeCuratedCalibrations(tool.butler, labels=self.labels)


class ConvertCalibrations(CalibrationOperation):
    """A concrete `AdminOperation` that converts a Gen2 calibration repo.

    This operation never writes curated calibrations or sets the default
    calibration collection pointer for its instrument.  It assumes it is the
    only operation working with its collections.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    instrument_name : `str`
        Short (dimension) name of the instrument.
    labels : `tuple` [ `str` ]
        Tuple of strings to include in the collection name(s).
    root : `str`
        Root of the Gen2 repository suite (a non-calibration repo).
    repo_path : `str`
        Path to the calibration repo, either absolute or relative to ``root``.
    """
    def __init__(self, name: str, instrument_name: str, labels: Tuple[str, ...], root: str, repo_path: str):
        super().__init__(name, instrument_name=instrument_name, labels=labels)
        self._repo_path = repo_path
        self._root = root

    DATASET_TYPE_NAMES = ("flat", "bias", "dark", "fringe", "sky")

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        SimpleStatus.check(self, tool).print_status(self, tool, indent)

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        logging.getLogger("daf.butler.Registry.insertDatasets").setLevel(logging.WARNING)
        logging.getLogger("daf.butler.datastores.FileDatastore.ingest").setLevel(logging.WARNING)
        task = self.make_task(tool)
        if not tool.dry_run:
            with SimpleStatus.run_context(self, tool):
                task.run(self._root, calibs=[self.make_repo_struct(tool)], reruns=[], processes=tool.jobs)

    def cleanup(self, tool: RepoAdminTool) -> None:
        # Docstring inherited
        from lsst.daf.butler.registry import CollectionType, MissingCollectionError
        collection = self.collection(tool)
        tool.log.info("Attempting to remove CALIBRATION collection %s.", collection)
        if not tool.dry_run:
            try:
                tool.butler.registry.removeCollection(collection)
            except MissingCollectionError:
                pass
        run_collections = set(
            tool.butler.registry.queryCollections(
                re.compile(re.escape(collection) + "/.+"),
                collectionTypes={CollectionType.RUN},
            )
        )
        tool.log.info("Attempting to remove %d RUN collections.", len(run_collections))
        if not tool.dry_run:
            tool.butler.removeRuns(run_collections, unstore=False)
        tool.log.info("Attempting to remove status files.")
        if not tool.dry_run:
            SimpleStatus.cleanup(self, tool)

    def make_repo_struct(self, tool: RepoAdminTool) -> CalibRepo:
        """Construct the `CalibRepo` struct used to describe the conversion
        operation.
        """
        from lsst.obs.base.gen2to3 import CalibRepo
        return CalibRepo(
            path=self._repo_path,
            curated=False,
            labels=self.labels,
            default=False,
        )

    def make_task(self, tool: RepoAdminTool, **kwargs: Any) -> ConvertRepoTask:
        """Construct the `ConvertRepoTask` instance to use in `run`.
        """
        from lsst.obs.base.gen2to3 import ConvertRepoTask
        instrument = self.instrument(tool)
        config = ConvertRepoTask.ConfigClass()
        instrument.applyConfigOverrides(ConvertRepoTask._DefaultName, config)
        config.transfer = "direct"
        config.doMakeUmbrellaCollection = False
        config.datasetIncludePatterns = self.DATASET_TYPE_NAMES
        config.datasetIgnorePatterns.append("*_camera")
        config.datasetIgnorePatterns.append("yBackground")
        config.datasetIgnorePatterns.append("fgcmLookUpTable")
        return ConvertRepoTask(config=config, butler3=tool.butler, instrument=instrument, **kwargs)
