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
    "DefineRawTag",
    "ExposureFinder",
    "IngestLogicError",
    "RawIngest",
    "RawIngestGroup",
    "UnstructuredExposureFinder",
)

from abc import ABC, abstractmethod
import fnmatch
import json
import logging
import math
import os
from pathlib import Path
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Match,
    Optional,
    Set,
    Tuple,
    Type,
    TYPE_CHECKING,
)

from lsst.utils import doImport

from ._operation import AdminOperation, OperationNotReadyError
from .common import Group

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef, FileDataset, Progress
    from lsst.obs.base import RawIngestTask
    from ._tool import RepoAdminTool


class ExposureFinder(ABC):
    """A helper interface for the `RawIngest` operation that identifies the
    files to be ingested and groups them by exposure.

    `ExposureFinder` objects are designed to be composed to add additional
    behaviors (usually filtering).  Subclasses should also inherit from
    `AdminOperation` (or hold nested `AdminOperation` instances) if they make
    persistent changes to filesystems or databases.
    """

    def flatten(self) -> Iterator[AdminOperation]:
        """Recursively iterate over any nested `AdminOperation` instances,
        including ``self`` if appropriate.

        Yields
        ------
        op : `AdminOperation`
           `AdminOperation` instances.
        """
        yield from ()

    @abstractmethod
    def find(self, tool: RepoAdminTool) -> Dict[int, Path]:
        """Find exposures and the root directories that contain all files to
        be ingested for them.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        exposures : `dict` [ `int`, `Path` ]
            Dictionary mapping integer exposure ID values to a directory that
            (not necessarily directly) includes all of the raw files to ingest
            for that exposure.
        """
        raise NotImplementedError()

    @abstractmethod
    def expand(self, tool: RepoAdminTool, exposure_id: int, found: Dict[int, Path]) -> Set[Path]:
        """Expand the found path for a directory into a set of paths for its
        raw files.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        exposure_id : `int`
            Integer exposure ID being ingested.
        found : `dict` [ `int`, `Path` ]
            The dictionary returned by `find`, or a subset of that dictionary
            that is guaranteed to include `exposure_id`.

        Returns
        -------
        raws : `set` [ `Path` ]
            Paths to raw files.

        Notes
        -----
        Implementations may recursively scan ``found[exposure_id]`` for files
        that look like raws, or use predefined filename patterns to predict
        them.  This function can also check for incomplete exposures and raise
        exceptions if desired.
        """
        raise NotImplementedError()

    @staticmethod
    def recursive_regex(tool: RepoAdminTool, top: Path, file_regex: str, follow_symlinks: bool = False,
                        ) -> Iterator[Tuple[Path, Match]]:
        """Recursively scan a directory for files whose names (not full paths)
        match a regular expression.

        This function is provided as a convenience for implementations of
        `find` and `expand`.  It is not used by the base class itself.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        top : `Path`
            Root path to search.
        file_regex : `str`
            Regular expression to match against filenames (including
            extensions, but not including the directory).  For file symlinks,
            this is applied to the symlink, not its target.
        follow_symlinks : `bool`, optional
            If `True`, follow directory and file symlinks.  If `False`
            (default) all symlinks are ignored.

        Yields
        ------
        path : `Path`
            Matched paths to files.
        match : `Match`
            The regular expression match object.
        """
        compiled_regex = re.compile(file_regex)

        def recurse_into(path: Path, progress: Progress) -> Iterator[Tuple[Path, Match]]:
            subdirs = []
            for entry in progress.wrap(os.scandir(path), desc=f"Scanning {path}"):
                if entry.is_file(follow_symlinks=follow_symlinks):
                    if (m := compiled_regex.match(entry.name)) is not None:
                        yield Path(entry.path if not follow_symlinks else os.path.realpath(entry.path)), m
                elif entry.is_dir(follow_symlinks=follow_symlinks):
                    subdirs.append(entry.path if not follow_symlinks else os.path.realpath(entry.path))
                # Else case is deliberately ignored; possibilities are
                # entries that no longer exist (race conditions) and
                # symlinks when follow_symlinks is False.
            for subdir in progress.wrap(subdirs, desc=f"Descending into subdirectories of {path}"):
                yield from recurse_into(subdir, progress)

        yield from recurse_into(top, tool.progress.at(logging.DEBUG))

    @staticmethod
    def recursive_glob(tool: RepoAdminTool, top: Path, file_pattern: str, follow_symlinks: bool = False,
                       ) -> Iterator[Path]:
        """Recursively scan a directory for files whose names (not full paths)
        match a shell glob.

        This function is provided as a convenience for implementations of
        `find` and `expand`.  It is not used by the base class itself.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        top : `Path`
            Root path to search.
        file_pattern : `str`
            Glob pattern to match against filenames (including extensions, but
            not including the directory).  For file symlinks, this is applied
            to the symlink, not its target.
        follow_symlinks : `bool`, optional
            If `True`, follow directory and file symlinks.  If `False`
            (default) all symlinks are ignored.

        Yields
        ------
        path : `Path`
            Matched paths to files.
        """
        file_regex = fnmatch.translate(file_pattern)
        yield from (
            path for path, _ in ExposureFinder.recursive_regex(tool, top, file_regex, follow_symlinks)
        )

    def saved_as(self, name: str) -> ExposureFinder:
        """Return an adapted version of the `ExposureFinder` that saves result
        of calling `find` to a JSON file for fast and consistent retrieval.

        Parameters
        ----------
        name : `str`
            Name of the `AdminOperation` that will save the file list.

        Returns
        -------
        finder_operation : `ExposureFinder`, `AdminOperation`
            An object that is both an `ExposureFinder` and `AdminOperation`,
            which delegates to ``self.find`` when `AdminOperation.run` is
            called, saves the results, and loads the saved
            dictionary when its own `ExposureFinder.find` is called.
        """
        return _SaveFoundExposuresAdapter(name, self)


class _SaveFoundExposuresAdapter(AdminOperation, ExposureFinder):
    """Adapter class for `ExposureFinder` that saves found exposures.

    Should only be constructed by calling `ExposureFinder.saved_as`,
    `RawIngest.save_found`, or `RawIngestGroup.save_found`; the class
    itself is an implementation detail.

    Parameters
    ----------
    name : `str`
        Name for this `AdminOperation`.
    adapted : `ExposureFinder`
        `ExposureFinder` instance to delegate to.
    """

    def __init__(self, name: str, adapted: ExposureFinder):
        super().__init__(name)
        self._adapted = adapted

    def flatten(self) -> Iterator[AdminOperation]:
        # Docstring inherited.
        yield from self._adapted.flatten()
        yield self

    def find(self, tool: RepoAdminTool) -> Dict[int, Path]:
        # Docstring inherited.
        filename = self._filename(tool)
        if not filename.exists():
            raise OperationNotReadyError(f"{self.name} has not yet been run.")
        with open(self._filename(tool), "r") as stream:
            loaded = json.load(stream)
        return {int(k): Path(v) for k, v in loaded.items()}

    def expand(self, tool: RepoAdminTool, exposure_id: int, found: Dict[int, Path]) -> Set[Path]:
        # Docstring inherited.
        return self._adapted.expand(tool, exposure_id, found)

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        if self._filename(tool).exists():
            found = self.find(tool)
            print(f"{' '*indent}{self.name}: found {len(found)} exposures")
        else:
            print(f"{' '*indent}{self.name}: not started")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        found_as_strs = {str(k): str(v) for k, v in self._adapted.find(tool).items()}
        with open(self._filename(tool), "w") as stream:
            json.dump(found_as_strs, stream, indent=0)

    def _filename(self, tool: RepoAdminTool) -> Path:
        """Return the name of the file used to save the found exposures.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        filename : `Path`
            The name of the file used to save the found exposures.
        """
        return tool.work_dir.joinpath(f"{self.name}.json")


class _ApportionFoundExposuresAdapter(ExposureFinder):
    """Adapter class for `ExposureFinder` that subselects a fixed fraction
    of its found exposures.

    Should only be constructed by calling `RawIngest.split_into`; the class
    itself is an implementation detail.

    Parameters
    ----------
    adapted : `ExposureFinder`
        `ExposureFinder` instance to delegate to.
    index : `int`
        Which chunk this adapter will select.
    count : `int`
        Number of chunks to split found exposures into.
    """

    def __init__(self, adapted: ExposureFinder, index: int, count: int):
        self._adapted = adapted
        self._index = index
        self._count = count

    def flatten(self) -> Iterator[AdminOperation]:
        # Docstring inherited.
        yield from self._adapted.flatten()

    def find(self, tool: RepoAdminTool) -> Dict[int, Path]:
        # Docstring inherited.
        found = self._adapted.find(tool)
        total = len(found)
        size = math.ceil(total / self._count)
        start = min(self._index*size, total)
        stop = min(start + size, total)
        return {exposure_id: found[exposure_id] for exposure_id in sorted(found.keys())[start: stop]}

    def expand(self, tool: RepoAdminTool, exposure_id: int, found: Dict[int, Path]) -> Set[Path]:
        # Docstring inherited.
        return self._adapted.expand(tool, exposure_id, found)


class IngestLogicError(Exception):
    """Exception raised when the information reported by `RawIngestTask` on
    success does not match what the `ExpsoureFinder` returned.

    This exception is only raised after an ingest transaction has been
    committed to the database, and hence it requires manual intervention if a
    fix is necessary.
    """
    pass


class _CheckRawIngestSuccess:
    """A callable for use as the ``on_success`` parameter to `RawIngestTask`
    as it is run by the `RawIngest` operation.
    """

    def __call__(self, fds: List[FileDataset]) -> None:
        ingested_paths = set()
        ingested_exposure_ids = set()
        for fd in fds:
            ingested_paths.add(Path(fd.path))
            for ref in fd.refs:
                ingested_exposure_ids.update(ref.dataId["exposure"] for ref in fd.refs)
        if ingested_paths != self.paths:
            raise IngestLogicError(f"Mismatch between ingested path(s) {ingested_paths - self.paths} "
                                   f"and expected paths {self.paths - ingested_paths} for "
                                   f"{self.exposure_id}.")
        if ingested_exposure_ids != {self.exposure_id}:
            bad = ingested_exposure_ids - {self.exposure_id}
            raise IngestLogicError(f"File(s) thought to be for exposure={self.exposure_id} "
                                   f"actually ingested as {bad}: {ingested_paths}.")

    paths: Set[Path]
    exposure_id: int


class RawIngestGroup(Group):
    """A custom `Group` that only holds `RawIngest` operations.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    children : `tuple` [ `RawIngest` ]
        Child operation instances.
    """

    def __init__(self, name: str, children: Tuple[RawIngest, ...]):
        super().__init__(name, children)

    children: Tuple[RawIngest, ...]

    def save_found(self, suffix: str = "find") -> RawIngestGroup:
        """Return a new `RawIngestGroup` by calling `RawIngest.save_found` on
        all children.

        This should be considered to consume ``self`` and the original child
        operations within it, as the returned group will use the same names.

        Parameters
        ----------
        suffix : `str`, optional
            Suffix to add to child operation names (with a "-" separator) to
            form the name of the operation that just saves the found exposures.

        Returns
        -------
        adapted : `RawIngestGroup`
            New `RawIngestGroup`.
        """
        return RawIngestGroup(
            self.name,
            tuple(c.save_found(suffix) for c in self.children),
        )


class RawIngest(AdminOperation):
    """A concrete `AdminOperation` that ingests raw images via
    `lsst.obs.base.RawIngestTask`.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    finder : `ExposureFinder`
        Object responsible for finding the raw files to ingest.
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
    """

    def __init__(
        self,
        name: str,
        finder: ExposureFinder,
        instrument_name: str,
        task_class_name: str = "lsst.obs.base.RawIngestTask",
        collection: Optional[str] = None,
        transfer: Optional[str] = "direct",
    ):
        super().__init__(name)
        self.finder = finder
        self._instrument_name = instrument_name
        self.task_class_name = task_class_name
        self.collection = collection
        self.transfer = transfer

    def split_into(self, n: int) -> RawIngestGroup:
        """Split this operation into a group in which each child is responsible
        for a fraction of the exposures found for the original.

        This should be considered to consume ``self``, as the returned group
        will perform the same work.

        Parameters
        ----------
        n : `int`
            Number of chunks to split into.

        Returns
        -------
        group : `RawIngestGroup`
            Group of equivalent operations.
        """
        return RawIngestGroup(
            self.name,
            tuple(
                RawIngest(
                    f"{self.name}-{i}",
                    _ApportionFoundExposuresAdapter(self.finder, i, n),
                    self._instrument_name,
                    self.task_class_name,
                    self.collection,
                )
                for i in range(n)
            )
        )

    def save_found(self, suffix: str = "find") -> RawIngest:
        """Return a new `RawIngest` operation that saves the ``dict`` of found
        exposures (via a nested operation that must be run first).

        This should be considered to consume ``self``, as the returned object
        will use the same name.

        Parameters
        ----------
        suffix : `str`, optional
            Suffix to add to ``self.name`` (with a "-" separator) to form the
            name of the operation that just saves the found exposures.

        Returns
        -------
        adapted : `RawIngestGroup`
            New `RawIngestGroup`.
        """
        return RawIngest(
            self.name,
            _SaveFoundExposuresAdapter(f"{self.name}-{suffix}", self.finder),
            self._instrument_name,
            self.task_class_name,
            self.collection,
        )

    def flatten(self) -> Iterator[AdminOperation]:
        """Iterate over ``self`` and (then) any child operations.

        Yields
        ------
        op : `AdminOperation`
            Self or an operation nested within it.
        """
        yield from self.finder.flatten()
        yield self

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        found = self.finder.find(tool)
        for child in self.finder.flatten():
            child.print_status(tool, indent)
        if found:
            ingested = self.already_ingested(tool)
            todo = found.keys() - ingested
            print(f"{' '*indent}{self.name}: {len(todo)} exposures remaining")
        else:
            print(f"{' '*indent}{self.name}: nothing to do")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        logging.getLogger("daf.butler.Registry.insertDatasets").setLevel(logging.WARNING)
        logging.getLogger("daf.butler.datastores.FileDatastore.ingest").setLevel(logging.WARNING)
        found = self.finder.find(tool)
        if not found:
            return
        ingested = self.already_ingested(tool)
        todo = found.keys() - ingested
        checker = _CheckRawIngestSuccess()
        task = self.make_task(tool, on_success=checker)
        for exposure_id in tool.progress.wrap(todo, desc="Ingesting exposures"):
            paths = self.finder.expand(tool, exposure_id, found)
            checker.paths = paths
            checker.exposure_id = exposure_id
            str_paths = [str(p) for p in paths]
            try:
                if tool.dry_run:
                    # Need a for loop to invoke returned lazy iterator.
                    for _ in task.prep(str_paths, processes=tool.jobs):
                        pass
                else:
                    task.run(str_paths, processes=tool.jobs, run=self.collection)
            except IngestLogicError:
                raise
            except Exception:
                continue

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

    def already_ingested(self, tool: RepoAdminTool):
        """Return the set of all exposures (as integer IDs) that have already
        been ingested for this instrument.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        """
        return {
            data_id["exposure"]
            for data_id in tool.butler.registry.queryDataIds(
                "exposure",
                instrument=self._instrument_name,
            )
        }


class DefineRawTag(AdminOperation):
    """A concrete `AdminOperation` that defines a ``TAGGED`` collection
    containing all raws ingested via a particular `ExposureFinder`.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    finder : `ExposureFinder`
        Object responsible for finding the raw files that were ingested.
    instrument_name : `str`
        Short/dimension name for the instrument whose raws are being ingested.
    input_collection : `str`
        Name of the collection that all raws were ingested into.  All raws in
        this collection with exposures found by ``finder`` will be tagged.
    output_collection : `str`
        Name of the new ``TAGGED`` collection.
    doc : `str`
        Documentation string for the new collection.
    """

    def __init__(self, name: str, finder: ExposureFinder, instrument_name: str,
                 input_collection: str, output_collection: str, doc: str):
        super().__init__(name)
        self.finder = finder
        self._instrument_name = instrument_name
        self.input_collection = input_collection
        self.output_collection = output_collection
        self.doc = doc

    QUERY_N_EXPOSURES = 100
    """Number of exposures to query for at once.

    We can't query for them all at once because we have to stuff them into
    one ``WHERE exposure IN (a, b, c, d, ...)`` expression, but querying
    one exposure at a time is latency-limited.
    """

    def flatten(self) -> Iterator[AdminOperation]:
        """Iterate over ``self`` and (then) any child operations.

        Yields
        ------
        op : `AdminOperation`
            Self or an operation nested within it.
        """
        yield from self.finder.flatten()
        yield self

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        from lsst.daf.butler.registry import MissingCollectionError
        try:
            tool.butler.registry.getCollectionType(self.input_collection)
        except MissingCollectionError:
            print(f"{' '*indent}{self.name}: blocked; input collection does not exist")
            return
        try:
            tool.butler.registry.getCollectionType(self.output_collection)
        except MissingCollectionError:
            print(f"{' '*indent}{self.name}: not started")
            return
        print(f"{' '*indent}{self.name}: done (or possibly interrupted; output collection exists)")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        from lsst.daf.butler import CollectionType
        refs = self.query(tool)
        if not tool.dry_run:
            tool.butler.registry.registerCollection(self.output_collection, CollectionType.TAGGED)
            tool.butler.registry.associate(self.output_collection, refs)
            tool.butler.registry.setCollectionDocumentation(self.output_collection, self.doc)

    def query(self, tool: RepoAdminTool) -> Set[DatasetRef]:
        """Query for datasets to tag.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        refs : `set` [ `DatasetRef` ]
            References to the datasets to tag.
        """
        found = list(self.finder.find(tool))
        refs = set()
        with tool.progress.bar(desc="Querying for ingested raws", total=len(found)) as progress_bar:
            for n in range(0, len(found), self.QUERY_N_EXPOSURES):
                start = n
                stop = min(n + self.QUERY_N_EXPOSURES, len(found))
                where = "exposure IN (" + ", ".join(str(id) for id in found[start: stop]) + ")"
                refs.update(
                    tool.butler.registry.queryDatasets(
                        "raw", collections=[self.input_collection],
                        instrument=self._instrument_name,
                        where=where,
                    )
                )
                progress_bar.update(stop - start)
        return refs


class UnstructuredExposureFinder(ExposureFinder):
    """An intermediate base class for `ExposureFinder` implementations that
    search directories recursively for raws matching a regex, making no
    expectations about directory structure but assuming exposure IDs can be
    extracted from the filename.

    Parameters
    ----------
    root : `str`
        Root path to search; subdirectories are searched recursively for
        matching files.
    file_regex : `str`
        Regular expression that raw files must match.  This is compared to the
        filename only, and to file symlink names, not their targets, when
        ``follow_symlinks`` is `True`.
    resolve_duplicates : `Callable`
        A callable that takes two `Path` arguments and returns a new `Path`
        (or `None`), to be invoked when the finder detects two directories
        that each contain a raw from the same exposure (but not necessarily
        the same one), indicating which is preferred.  The default always
        returns `None`, which causes `RuntimeError` to be raised.
    follow_symlinks: `bool`, optional
        If `True`, follow both file and directory symlinks and ingest their
        targets.  If `False` (default), all symlinks are ignored.

    Notes
    -----
    Subclasses must still implement `ExposureFinder.expand`, and will
    also need to reimplement the new `extract_exposure_id` method.
    """

    def __init__(self, root: Path, file_regex: str, *,
                 resolve_duplicates: Callable[[Path, Path], Optional[Path]] = lambda a, b: None,
                 follow_symlinks: bool = False):
        self._root = root
        self._file_regex = file_regex
        self._resolve_duplicates = resolve_duplicates
        self._follow_symlinks = follow_symlinks

    @abstractmethod
    def extract_exposure_id(self, tool: RepoAdminTool, match: re.Match) -> int:
        """Extract an exposure ID from a filename regular expression match
        object.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        match : `re.Match`
            Match object for a found filename.

        Returns
        -------
        exposure_id : `int`
            Integer exposure ID.
        """
        raise NotImplementedError()

    def find(self, tool: RepoAdminTool) -> Dict[int, Path]:
        # Docstring inherited.
        result = {}
        for path, match in self.recursive_regex(tool, self._root, self._file_regex,
                                                follow_symlinks=self._follow_symlinks):
            exposure_id = self.extract_exposure_id(tool, match)
            previous_path = result.setdefault(exposure_id, path.parent)
            if previous_path != path.parent:
                if (best_path := self._resolve_duplicates(previous_path, path.parent)) is not None:
                    result[exposure_id] = best_path
                else:
                    raise RuntimeError(f"Found multiple directory paths ({previous_path}, {path.parent}) "
                                       f"for exposure {exposure_id}.")
        return result
