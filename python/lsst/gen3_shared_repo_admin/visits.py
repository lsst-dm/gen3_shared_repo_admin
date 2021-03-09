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

__all__ = ("DefineVisits",)

from typing import Optional, Set, Tuple, Type, TYPE_CHECKING

from lsst.utils import doImport

from ._operation import AdminOperation, OperationNotReadyError

if TYPE_CHECKING:
    from lsst.daf.butler import DataCoordinate
    from lsst.obs.base import DefineVisitsTask, Instrument
    from ._tool import RepoAdminTool


class DefineVisits(AdminOperation):
    """A concrete `AdminOperation` that define visits as groups of exposures
    and computes their spatial regions, via `DefineVisitsTask`.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    instrument_name : `str`
        Short (dimension) name of the instrument.
    task_class_name : `str`, optional
        Fully-qualified path to the `DefineVisitsTask` subclass to use
        (defaults to "lsst.obs.base.DefineVisitsTask" itself).  This is passed
        as a string instead of a type or instance to defer imports (which can
        be very slow) until they are actually needed, rather than include them
        in `RepoDefinition` object instantiations.
    collections : `tuple` [ `str` ], optional
        Collection search path for datasets needed to define visits (depends
        on task configuration, but this should usually include a camera, raws,
        or both).
    """
    def __init__(
        self,
        name: str,
        instrument_name: str,
        task_class_name: str = "lsst.obs.base.DefineVisitsTask",
        collections: Optional[Tuple[str, ...]] = None,
    ):
        super().__init__(name)
        self.instrument_name = instrument_name
        self.task_class_name = task_class_name
        self._collections = collections

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        todo, n_done = self.query(tool)
        if todo:
            if n_done:
                print(f"{' '*indent}{self.name}: in progress; "
                      f"{n_done} visits done, {len(todo)} exposures remaining")
            else:
                print(f"{' '*indent}{self.name}: not started; {len(todo)} exposures found")
        else:
            assert not n_done
            print(f"{' '*indent}{self.name}: {n_done} visits defined")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        instrument = self.instrument(tool)
        task = self.make_task(tool, instrument=instrument)
        todo, _ = self.query(tool, instrument=instrument, task=task)
        collections = self.collections(tool, instrument=instrument)
        if not tool.dry_run:
            task.run(todo, collections=collections, processes=tool.jobs)

    def instrument(self, tool: RepoAdminTool) -> Instrument:
        """Return the `Instrument` instance associated with this operation.
        """
        from lsst.obs.base import Instrument
        return Instrument.fromName(self.instrument_name, tool.butler.registry)

    @property
    def TaskClass(self) -> Type[DefineVisitsTask]:
        """Task class (subclass of `DefineVisitsTask`) to run.
        """
        return doImport(self.task_class_name)

    def make_task(self, tool: RepoAdminTool, *, instrument: Optional[Instrument] = None) -> DefineVisitsTask:
        """Construct the `DefineVisitsTask` instance to use in `run`.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        instrument : `Instrument`, optional
            The `Instrument` instance associated with this operation; obtained
            from the `instrument` method if not provided.
        """
        if instrument is None:
            instrument = self.instrument(tool)
        config = self.TaskClass.ConfigClass()
        self.instrument(tool).applyConfigOverrides(self.TaskClass._DefaultName, config)
        return self.TaskClass(config=config, butler=tool.butler)

    def collections(self, tool: RepoAdminTool, *, instrument: Optional[Instrument] = None,
                    ) -> Tuple[str, ...]:
        """Return the collections to pass to `DefineVisitsTask.run` in order to
        load camera geometry and/or raw images.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        instrument : `Instrument`, optional
            The `Instrument` instance associated with this operation; obtained
            from the `instrument` method if not provided.

        Returns
        -------
        collections : `tuple` [ `str` ]
            Collections to pass to `DefineVisitsTask.run`.
        """
        if instrument is None:
            instrument = self.instrument(tool)
        if self._collections is not None:
            result = self._collections
        else:
            result = (
                instrument.makeCalibrationCollectionName(),
                instrument.makeDefaultRawIngestRunName(),
            )
        found = set(tool.butler.registry.queryCollections(result))
        if found != set(result):
            raise OperationNotReadyError(f"Collections {set(result) - found} do not yet exist.")
        return result

    def query(self, tool: RepoAdminTool, *, instrument: Optional[Instrument] = None,
              task: Optional[DefineVisitsTask] = None) -> Tuple[Set[DataCoordinate], int]:
        """Query for exposures that still need to be processed.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        instrument : `Instrument`, optional
            The `Instrument` instance associated with this operation; obtained
            from the `instrument` method if not provided.
        task : `DefineVisitsTask`, optional
            The `DefineVisitsTask` instance associated with this operation;
            obtained from the `make_task` method if not provided.

        Returns
        -------
        todo : `set` [ `DataCoordinate` ]
            Exposure data IDs that still need to be grouped into visits.
        n_done : `int`
            Number of visits already defined.
        """
        if instrument is None:
            instrument = self.instrument(tool)
        if task is None:
            task = self.make_task(tool, instrument=instrument)
        # A set of exposure IDs we've already processed.
        done = {
            record.exposure
            for record in tool.progress.wrap(
                tool.butler.registry.queryDimensionRecords(
                    "visit_definition",
                    instrument=self.instrument_name,
                    where="visit_system=VS",
                    bind=dict(VS=task.groupExposures.getVisitSystem()[0]),
                ),
                desc="Querying for existing visit definitions",
            )
        }
        # A set of all exposure data IDs we still need to process
        exposures = {
            data_id
            for data_id in tool.progress.wrap(
                tool.butler.registry.queryDataIds(
                    "exposure",
                    instrument=self.instrument_name,
                    where="exposure.observation_type='science'",
                ).expanded(),
                desc="Querying for exposures to process",
            )
            if data_id["exposure"] not in done
        }
        return exposures, len(done)
