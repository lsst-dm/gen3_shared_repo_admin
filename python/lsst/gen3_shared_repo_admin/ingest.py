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
from typing import Callable, Iterator, Set, Tuple, Type, TYPE_CHECKING

from lsst.utils import doImport

from ._operation import AdminOperation, IncompleteOperationError
from .common import Group

if TYPE_CHECKING:
    from lsst.obs.base import RawIngestTask
    from ._tool import RepoAdminTool


FindFilesFunction = Callable[[str, "RepoAdminTool"], Set[str]]


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
    """
    def __init__(self, name: str, find_files: FindFilesFunction,
                 task_class_name: str = "lsst.obs.base.RawIngestTask"):
        self.name = name
        self.find_files = find_files
        self.task_class_name = task_class_name
        logging.getLogger("daf.butler.Registry.insertDatasets").setLevel(logging.WARNING)
        logging.getLogger("daf.butler.datastores.FileDatastore.ingest").setLevel(logging.WARNING)

    @staticmethod
    def find_file_glob(top_template: str, pattern_template: str) -> FindFilesFunction:
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
        """
        def wrapper(name: str, tool: RepoAdminTool) -> Set[str]:
            top = top_template.format(name=name, repo=tool.repo, site=tool.site)
            pattern = pattern_template.format(name=name, repo=tool.repo, site=tool.site)

            def recurse_into(path: str) -> Iterator[str]:
                subdirs = []
                for entry in tool.progress.wrap(os.scandir(path), desc=f"Scanning {path}"):
                    if entry.is_symlink():
                        tool.log.debug("Ignoring symlink %s/%s.", path, entry.name)
                    elif entry.is_file(follow_symlinks=False):
                        if fnmatch.fnmatchcase(entry.name, pattern):
                            yield entry.path
                    else:
                        subdirs.append(entry.path)
                for subdir in tool.progress.wrap(subdirs, desc=f"Descending into subdirectories of {path}"):
                    yield from recurse_into(subdir)

            return set(recurse_into(top))

        return wrapper

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        in_progress_filename = self._in_progress_filename(tool)
        input_filename = self._input_filename(tool)
        if os.path.exists(input_filename):
            todo, done = self.read_file_lists(tool)
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
                line + "\n" for line in
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
        todo, done = self.read_file_lists(tool)
        if not todo:
            return
        task = self.make_task(tool)
        ingested = []
        if tool.dry_run:
            # Need a for loop to invoke returned lazy iterator.
            for _ in task.prep(todo, ingested=ingested, processes=tool.jobs):
                pass
        else:
            file = open(self._in_progress_filename(tool), "wt")
            try:
                file.flush()
                task.run(todo, ingested=ingested, processes=tool.jobs)
            finally:
                done.update(dataset.path for dataset in ingested)
                file.writelines(line + "\n" for line in sorted(done))
                file.close()
                os.replace(self._in_progress_filename(tool), self._completed_filename(tool))

    @property
    def TaskClass(self) -> Type[RawIngestTask]:
        """Task class instance to run.
        """
        return doImport(self.task_class_name)

    def _tmp_filename(self, tool: RepoAdminTool) -> str:
        """Filename used to save the to-do file list in `prep`, before it
        finishes.
        """
        return os.path.join(tool.work_dir, f"{self.name}_files.tmp.txt")

    def _input_filename(self, tool: RepoAdminTool) -> str:
        """Filename used to save the to-do file list `prep`, when it has
        finished.
        """
        return os.path.join(tool.work_dir, f"{self.name}_files.txt")

    def _in_progress_filename(self, tool: RepoAdminTool) -> str:
        """Filename used to save the completed file list in `run`, before it
        finishes.
        """
        return os.path.join(tool.work_dir, f"{self.name}_in_progress.txt")

    def _completed_filename(self, tool: RepoAdminTool) -> str:
        """Filename used to save the completed file list in `prep`, when it has
        finished fully ingesting those files.
        """
        return os.path.join(tool.work_dir, f"{self.name}_completed.txt")

    def make_task(self, tool: RepoAdminTool) -> RawIngestTask:
        """Construct the `RawIngestTask` instance to use in `run`.
        """
        config = self.TaskClass.ConfigClass()
        config.transfer = "direct"
        return self.TaskClass(config=config, butler=tool.butler)

    def read_input_list(self, tool: RepoAdminTool) -> Set[str]:
        """Read the post-`prep` list of files to process.
        """
        input_filename = self._input_filename(tool)
        with open(input_filename, "rt") as file:
            todo = {line.strip() for line in file}
        return todo

    def read_file_lists(self, tool: RepoAdminTool) -> Tuple[Set[str], Set[str]]:
        """Read the post-`prep` list of files to do, and the post-`run` list
        of files fully ingested, and return sets that contain what still
        needs to be done and what has already been done.
        """
        completed_filename = self._completed_filename(tool)
        todo = self.read_input_list(tool)
        done = set()
        if os.path.exists(completed_filename):
            with open(completed_filename, "rt") as file:
                done.update(line.strip() for line in file)
            todo -= done
        return todo, done


class DeduplicatingRawIngestGroup(Group):
    """A special `Group` that ensures its `RawIngest` children do not try to
    ingest the same raw dataset multiple times, by deduplicating in `prep`
    on filenames.

    The `prep` stages of all child operations must be run *in order*, but after
    this is done the `run` stages may be run in any order.

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
        previous = []
        for child in self.children:
            child.find_files = self.skip_already_found(tuple(previous), child.find_files)
            previous.append(child)

    children: Tuple[RawIngest, ...]

    @staticmethod
    def skip_already_found(previous: Tuple[RawIngest, ...], func: FindFilesFunction) -> FindFilesFunction:
        """A callable for the ``find_files`` argument to `RawIngest` that
        removes files found in a previous child ingest `prep` stage.

        This is automatically installed by the `DeduplicatingRawIngestGroup`
        constructor.
        """
        def adapted(name: str, tool: RepoAdminTool) -> Set[str]:
            already_found = set()
            for other_ingest_operation in previous:
                already_found.update(other_ingest_operation.read_input_list(tool))
            return func(name, tool) - already_found
        return adapted
