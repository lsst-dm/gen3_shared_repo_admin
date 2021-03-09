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

__all__ = ("FindFilesFunction", "DeduplicatingRawIngestGroup", "RawIngest")

import fnmatch
import logging
import os
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Set, Tuple, Type, TYPE_CHECKING

from lsst.utils import doImport

from ._operation import AdminOperation, IncompleteOperationError, OperationNotReadyError
from .common import Group

if TYPE_CHECKING:
    from lsst.daf.butler import Progress
    from lsst.obs.base import RawIngestTask
    from ._tool import RepoAdminTool


FindFilesFunction = Callable[[str, "RepoAdminTool"], Set[Path]]


class RawIngest(AdminOperation):
    """A concrete `AdminOperation` that ingests raw images via `RawIngestTask`.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    find_files : `Callable`
        A callable that takes a single `RepoAdminTool` argument and returns
        a set of filenames to ingest.  This will be run during `prep`, saved
        to a file in `RepoAdminTool.work_dir`, and used to drive actual ingest
        in `run`.
    task_class_name : `str`, optional
        Fully-qualified path to the `RawIngestTask` subclass to use (defaults
        to "lsst.obs.base.RawIngestTask" itself).  This is passed as a string
        instead of a type or instance to defer imports (which can be very slow)
        until they are actually needed, rather than include them in
        `RepoDefinition` object instantiations.
    collection : `str`, optional
        Name of the `~lsst.daf.butler.CollectionType.RUN` collection that
        datasets should be ingested into.  Default is to defer to the
        `Instrument` class, which should be appropriate for all real-data
        repositories, but not necessary those with simulated data (for which
        more collections may be necessary to distinguish between simulation
        versions).
    """
    def __init__(self, name: str, find_files: FindFilesFunction,
                 task_class_name: str = "lsst.obs.base.RawIngestTask",
                 collection: Optional[str] = None):
        super().__init__(name)
        self.find_files = find_files
        self.task_class_name = task_class_name
        self.collection = collection

    CHUNK_SIZE = 10000

    @staticmethod
    def find_file_glob(top_template: str, pattern_template: str, follow_symlinks: bool = False
                       ) -> FindFilesFunction:
        """A function that recursively finds files according to a glob pattern
        template, for use as the ``find_files`` construction argument.

        Parameters
        ----------
        top_template : `str`
            Template string for the root directory to search.  Will be
            processed via `str.format` with named ``repo`` (`RepoDefinition`)
            and ``site`` (`SiteDefinition`) arguments to form the actual root
            directory.
        pattern_template : `str`
            Template string for the filename-only glob pattern.  Will be
            processed via `str.format` with named ``repo`` (`RepoDefinition`)
            and ``site`` (`SiteDefinition`) arguments to form the actual
            filename-only glob pattern.
        follow_symlinks : `bool`, optional
            If `True`, follow symlinks and resolve them into their true paths,
            and user guarantees there are no cycles (no checking is performed).
            If `False`, all symlinks are ignored.

        Returns
        -------
        files : `Set` [ `Path` ]
            Set of full paths to the files to ingest.
        """
        def wrapper(name: str, tool: RepoAdminTool) -> Set[Path]:
            top = top_template.format(name=name, repo=tool.repo, site=tool.site)
            pattern = pattern_template.format(name=name, repo=tool.repo, site=tool.site)

            def recurse_into(path: str, progress: Progress) -> Iterator[Path]:
                subdirs = []
                for entry in progress.wrap(os.scandir(path), desc=f"Scanning {path}"):
                    if entry.is_file(follow_symlinks=follow_symlinks):
                        if fnmatch.fnmatchcase(entry.name, pattern):
                            yield Path(entry.path if not follow_symlinks else os.path.realpath(entry.path))
                    elif entry.is_dir(follow_symlinks=follow_symlinks):
                        subdirs.append(entry.path if not follow_symlinks else os.path.realpath(entry.path))
                    # Else case is deliberately ignored; possibilities are
                    # entries that no longer exist (race conditions) and
                    # symlinks when follow_symlinks is False.
                for subdir in progress.wrap(subdirs, desc=f"Descending into subdirectories of {path}"):
                    yield from recurse_into(subdir, progress.at(logging.DEBUG))

            return set(recurse_into(top, tool.progress))

        return wrapper

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        in_progress_filename = self._in_progress_filename(tool)
        input_filename = self._input_filename(tool)
        if os.path.exists(input_filename):
            todo, done = self.read_files(tool)
            if not todo:
                print(f"{' '*indent}{self.name}: ingest done")
            elif not done:
                if os.path.exists(in_progress_filename):
                    print(f"{' '*indent}{self.name}: ingest started and in progress")
                else:
                    print(f"{' '*indent}{self.name}: {len(todo)} files found, ready to ingest")
            else:
                print(f"{' '*indent}{self.name}: ingest in progress, "
                      f"{len(todo)} to do, {len(done)} done")
        else:
            print(f"{' '*indent}{self.name}: not started; prep needed")

    def prep(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        # Delegate to find_files and write the results to a temporary file.
        input_filename = self._input_filename(tool)
        tmp_filename = self._tmp_filename(tool)
        with open(tmp_filename, "wt") as file:
            file.writelines(
                str(path) + "\n" for path in
                tool.progress.wrap(
                    sorted(self.find_files(self.name, tool)),
                    desc=f"Writing {self.name} file list"
                )
            )
            # If we didn't encounter any errors, rename the file to make it
            # available to next steps.
            os.replace(tmp_filename, input_filename)

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        logging.getLogger("daf.butler.Registry.insertDatasets").setLevel(logging.WARNING)
        logging.getLogger("daf.butler.datastores.FileDatastore.ingest").setLevel(logging.WARNING)
        in_progress_filename = self._in_progress_filename(tool)
        completed_filename = self._completed_filename(tool)
        if os.path.exists(in_progress_filename):
            raise IncompleteOperationError(
                f"{self.name} is either in progress in another process, or "
                "has failed in an way that requires manual intervention; "
                f"'{in_progress_filename}' lists files that were definitely "
                "ingested, but others may have been ingested as well. "
                "To continue, ensure all files actually ingested are listed "
                f"in '{completed_filename}' and then delete "
                f"'{in_progress_filename}'."
            )
        todo, done = self.read_files(tool)
        if not todo:
            return
        sorted_todo = sorted([str(path) for path in todo])
        chunks = [sorted_todo[n: min(n + self.CHUNK_SIZE, len(sorted_todo))]
                  for n in range(0, len(sorted_todo), self.CHUNK_SIZE)]
        ingested = []
        task = self.make_task(tool, on_success=ingested.extend)
        if tool.dry_run:
            # Need a for loop to invoke returned lazy iterator.
            for chunk in tool.progress.wrap(chunks, desc=f"Ingesting in {self.CHUNK_SIZE}-file chunks"):
                for _ in task.prep(chunk, processes=tool.jobs):
                    pass
        else:
            file = open(self._in_progress_filename(tool), "wt")
            try:
                file.flush()
                for chunk in tool.progress.wrap(chunks, desc=f"Ingesting in {self.CHUNK_SIZE}-file chunks"):
                    try:
                        task.run(chunk, processes=tool.jobs, run=self.collection)
                    except RuntimeError:
                        continue
            finally:
                done.update(dataset.path for dataset in ingested)
                file.writelines(str(line) + "\n" for line in sorted(done))
                file.close()
                os.replace(self._in_progress_filename(tool), self._completed_filename(tool))

    @property
    def TaskClass(self) -> Type[RawIngestTask]:
        """Task class (`RawIngestTask` subclass) to run.
        """
        return doImport(self.task_class_name)

    def _tmp_filename(self, tool: RepoAdminTool) -> Path:
        """Filename used to save the to-do file list in `prep`, before it
        finishes.
        """
        return tool.work_dir.joinpath(f"{self.name}_files.tmp.txt")

    def _input_filename(self, tool: RepoAdminTool) -> Path:
        """Filename used to save the to-do file list `prep`, when it has
        finished.
        """
        return tool.work_dir.joinpath(f"{self.name}_files.txt")

    def _in_progress_filename(self, tool: RepoAdminTool) -> Path:
        """Filename used to save the completed file list in `run`, before it
        finishes.
        """
        return tool.work_dir.joinpath(f"{self.name}_in_progress.txt")

    def _completed_filename(self, tool: RepoAdminTool) -> Path:
        """Filename used to save the completed file list in `run`, when it has
        finished fully ingesting those files.
        """
        return tool.work_dir.joinpath(f"{self.name}_completed.txt")

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
        config.transfer = "direct"
        return self.TaskClass(config=config, butler=tool.butler, **kwargs)

    def read_input_files(self, tool: RepoAdminTool) -> Set[Path]:
        """Read the post-`prep` list of files to process.

        This method requires input file to already exist, as it should
        only be called in contexts where this is true.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        paths : `set` [ `Path` ]
            Set of full paths to all files that should be ingested, including
            those already ingested.
        """
        input_filename = self._input_filename(tool)
        if not input_filename.exists():
            raise OperationNotReadyError(f"{self.name} needs to be prepped first.")
        with open(input_filename, "rt") as file:
            paths = {Path(line.strip()) for line in file}
        return paths

    def read_done_files(self, tool: RepoAdminTool) -> Set[Path]:
        """Read the post-`run` list of files fully ingested.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        done : `set` [ `Path` ]
            Set of full paths to all files that have already been ingested.
        """
        completed_filename = self._completed_filename(tool)
        done = set()
        if completed_filename.exists():
            with open(completed_filename, "rt") as file:
                done.update(Path(line.strip()) for line in file)
        return done

    def read_files(self, tool: RepoAdminTool) -> Tuple[Set[Path], Set[Path]]:
        """Read the post-`prep` list of files to do, and the post-`run` list
        of files fully ingested, and return sets that contain what still
        needs to be done and what has already been done.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        todo : `set` [ `Path` ]
            Set of full paths to all files that still need to be ingested.
        done : `set` [ `Path` ]
            Set of full paths to all files that have already been ingested.
        """
        todo = self.read_input_files(tool)
        done = self.read_done_files(tool)
        todo -= done
        return todo, done


class DeduplicatingRawIngestGroup(Group):
    """A special `Group` that ensures its `RawIngest` children do not try to
    ingest the same raw dataset multiple times, by deduplicating in `prep`
    on filenames.

    The `prep` stages of all child operations must be run *in order*, but after
    this is done the `run` stages may be run in any order.

    This class can remove duplicates even after some files have been
    ingested according to a non-deduplicated list.  It assumes filenames
    are (alone) enough to uniquely identify raw files.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    children : `tuple` [ `RawIngest` ]
        Child ingest instances.
    """
    def __init__(self, name: str, children: Tuple[RawIngestTask, ...]):
        super().__init__(name, children)
        before = []
        after = list(self.children)
        for child in self.children:
            del after[0]
            child.find_files = self.remove_duplicates(tuple(before), tuple(after), child, child.find_files)
            before.append(child)

    children: Tuple[RawIngest, ...]

    @staticmethod
    def remove_duplicates(before: Tuple[RawIngest, ...], after: Tuple[RawIngest, ...], me: RawIngest,
                          func: FindFilesFunction) -> FindFilesFunction:
        """A callable for the ``find_files`` argument to `RawIngest` that
        removes files found in any earlier sibling ingest `prep` stage, and any
        files already ingested by any earlier or later sibling `run` stage.

        This is automatically installed by the `DeduplicatingRawIngestGroup`
        constructor.
        """
        def adapted(name: str, tool: RepoAdminTool) -> Set[Path]:
            found_by_me_dict = {path.name: path for path in func(name, tool)}
            done_by_me = {path.name for path in me.read_done_files(tool)}
            tool.log.info("%s: found %d files, with %d done...", me.name,
                          len(found_by_me_dict), len(done_by_me))
            keep = set(found_by_me_dict.keys() | done_by_me)
            for other_op in before:
                found_by_other = {path.name for path in other_op.read_input_files(tool)}
                already_taken = (found_by_other - done_by_me) & keep
                tool.log.info("  %s: removing %d files also found by %s", me.name, len(already_taken),
                              other_op.name)
                keep -= already_taken
            for other_op in after:
                done_by_other = {path.name for path in other_op.read_done_files(tool)}
                assert not done_by_other & done_by_me
                already_taken = done_by_other & keep
                tool.log.info("  %s: removing %d files already ingested by %s", me.name, len(already_taken),
                              other_op.name)
                keep -= already_taken
            tool.log.info("  %s: kept %d files", me.name, len(keep))
            return {found_by_me_dict[name] for name in keep}
        return adapted
