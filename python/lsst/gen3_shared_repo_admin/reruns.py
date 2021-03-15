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

__all__ = ("ConvertRerun",)

import logging
from typing import Any, Tuple, TYPE_CHECKING

from ._operation import AdminOperation, SimpleStatus

if TYPE_CHECKING:
    from lsst.obs.base.gen2to3 import Rerun, ConvertRepoTask
    from lsst.obs.base import Instrument
    from ._tool import RepoAdminTool


class ConvertRerun(AdminOperation):
    """A concrete `AdminOperation` that converts some or all of a Gen2 rerun
    repo.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    instrument_name : `str`
        Short (dimension) name of the instrument.
    root : `str`
        Root of the Gen2 repository suite (a non-calibration repo).
    repo_path : `str`
        Path to the rerun repo, either absolute or relative to ``root``.
    run_name : `str`
        Name of the ``RUN`` collection all datasets will be ingested into.
    include : `tuple` [ `str` ]
        Dataset type name patterns to include; replaces any list in the
        default (including ``obs`` package override) configuration.
    exclude : `tuple` [ `str` ]
        Dataset type name patterns to exclude; extends any list in the default
        (include ``obs`` package override) configuration.

    Notes
    -----
    This operation never sets up a ``CHAINED`` collection to connect the
    converted run to the conversions of its Gen2 "parent" repositories; that
    should almost always be done as a separate `DefineChain` operation.
    """
    def __init__(self, name: str, instrument_name: str, root: str, repo_path: str, run_name: str,
                 include: Tuple[str, ...], exclude: Tuple[str, ...]):
        super().__init__(name)
        self.instrument_name = instrument_name
        self._repo_path = repo_path
        self._root = root
        self._run_name = run_name
        self._include = include
        self._exclude = exclude

    def instrument(self, tool: RepoAdminTool) -> Instrument:
        from lsst.obs.base import Instrument
        return Instrument.fromName(self.instrument_name, tool.butler.registry)

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        SimpleStatus.check(self, tool).print_status(self, tool, indent)

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        logging.getLogger("daf.butler.Registry.insertDatasets").setLevel(logging.WARNING)
        # logging.getLogger("daf.butler.datastores.FileDatastore.ingest").setLevel(logging.WARNING)
        task = self.make_task(tool)
        with SimpleStatus.run_context(self, tool):
            task.run(self._root, reruns=[self.make_repo_struct(tool)], calibs=[],
                     processes=tool.jobs)

    def make_repo_struct(self, tool: RepoAdminTool) -> Rerun:
        """Construct the `Rerun` struct used to describe the conversion
        operation.
        """
        from lsst.obs.base.gen2to3 import Rerun
        return Rerun(
            path=self._repo_path,
            runName=self._run_name,
            chainName=None,
            parents=[],
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
        config.datasetIncludePatterns = list(self._include)
        config.datasetIgnorePatterns.extend(self._exclude)
        config.datasetIgnorePatterns.extend([
            "*_camera",
            "yBackground",
            "fgcmLookUpTable",
            "fgcmLookUpTable",
            "*_metadata",
            "raw",
            "brightObjectMask",
            "ref_cat",
            "*.fitsSAVE",
        ])
        config.doExpandDataIds = False
        return ConvertRepoTask(config=config, butler3=tool.butler, instrument=instrument,
                               dry_run=tool.dry_run, **kwargs)
