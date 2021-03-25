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
    "CheckURIs",
)

from collections import defaultdict
import enum
import itertools
import os
from pathlib import Path
from typing import Dict, Sequence, Set, Tuple, TYPE_CHECKING

from ._operation import AdminOperation, SimpleStatus

if TYPE_CHECKING:
    from ._tool import RepoAdminTool
    from lsst.daf.butler import ButlerURI, DataId, DatasetRef


class StatusFlag(enum.Flag):
    """Flag enumeration for problems that can be found with a dataset URI.
    """
    OK = 0
    DOES_NOT_EXIST = enum.auto()
    IS_DIRECTORY = enum.auto()
    HAS_SYMLINKS = enum.auto()


class CheckURIs(AdminOperation):
    """A concrete operation that spot-checks the URIs of datasets for
    existence, symlinks, and common path prefixes.

    The output of this operation consists entirely of its log messages; which
    including ``INFO`` messages with summary information and ``WARN`` messages
    for any `StatusFlag` conditions (one per collection), or if the spot-check
    data ID values given do not yield any instances of a dataset type.

    All ``RUN`` collections and dataset types are searched.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    data_ids : `Sequence` [ `DataId` ]
        A sequence of data ID mappings that identify the datasets to spot-
        check.  Each data ID is subset the relevant dimensions for a dataset
        type when the search for that dataset type is performed, and all
        datasets matching any of the given data IDs will be included.
        Dimensions that are not constrained in a data ID will be allowed to
        take any value.
    """

    def __init__(self, name: str, data_ids: Sequence[DataId]):
        super().__init__(name)
        self._data_ids = data_ids

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        SimpleStatus.check(self, tool).print_status(self, tool, indent)

    def process_uri(self, tool: RepoAdminTool, uri: ButlerURI, ref: DatasetRef) -> Tuple[str, StatusFlag]:
        status = StatusFlag.OK
        if not uri.exists():
            status |= StatusFlag.DOES_NOT_EXIST
        if uri.isdir():
            status |= StatusFlag.IS_DIRECTORY
        try:
            ospath = Path(uri.ospath)
        except AttributeError:
            pass
        else:
            if (ospath.is_symlink()
                    or os.path.normpath(os.path.realpath(ospath)) != os.path.normpath(str(ospath))):
                status |= StatusFlag.HAS_SYMLINKS
            return str(ospath.parent), status
        return str(uri.parent()), status

    def run(self, tool: RepoAdminTool):
        # Docstring inherited.
        from lsst.daf.butler import CollectionType
        with SimpleStatus.run_context(self, tool):
            full_data_ids = [tool.butler.registry.expandDataId(data_id) for data_id in self._data_ids]
            dataset_types = set(tool.butler.registry.queryDatasetTypes(...))
            collections = set(
                tool.butler.registry.queryCollections(..., collectionTypes={CollectionType.RUN})
            )
            tool.log.info("Found %d dataset types and %d RUN collections.",
                          len(dataset_types), len(collections))
            dirs_by_collection: Dict[str, Set[str]] = defaultdict(set)
            status_by_collection: Dict[str, StatusFlag] = defaultdict(lambda: StatusFlag.OK)
            for dataset_type in tool.progress.wrap(dataset_types, desc="Checking URIs by dataset type"):
                constraint_data_ids = [full_data_id.subset(dataset_type.dimensions & full_data_id.graph)
                                       for full_data_id in full_data_ids]
                datasets = set(
                    itertools.chain.from_iterable(
                        tool.butler.registry.queryDatasets(
                            dataset_type,
                            collections=collections,
                            findFirst=False,
                            dataId=constraint_data_id
                        ) for constraint_data_id in constraint_data_ids
                    )
                )
                collections_for_dataset_type = set()
                for ref in datasets:
                    primary_uri, component_uris = tool.butler.datastore.getURIs(ref, predict=False)
                    if primary_uri is not None:
                        dir, status = self.process_uri(tool, primary_uri, ref)
                        dirs_by_collection[ref.run].add(dir)
                        status_by_collection[ref.run] |= status
                    for component_uri in component_uris.values():
                        dir, status = self.process_uri(tool, component_uri, ref)
                        dirs_by_collection[ref.run].add(dir)
                        status_by_collection[ref.run] |= status
                    collections_for_dataset_type.add(ref.run)
                if not datasets:
                    tool.log.warning(
                        "No instances of registered dataset type %s found with spot-check data IDs.",
                        dataset_type.name
                    )
                elif len(collections_for_dataset_type) == 1:
                    (collection_for_dataset_type,) = collections_for_dataset_type
                    tool.log.info(
                        "Spot-check found %d instance(s) of dataset type %s in %s.",
                        len(datasets), dataset_type.name, collection_for_dataset_type,
                    )
                else:
                    tool.log.info(
                        "Spot-check found %d instance(s) of dataset type %s in %d different RUN collections.",
                        len(datasets), dataset_type.name, len(collections_for_dataset_type),
                    )
            for collection, dirs in dirs_by_collection.items():
                tool.log.info(
                    "%s: common prefix is %s",
                    collection,
                    os.path.commonpath(list(dirs))
                )
                status = status_by_collection[collection]
                if status & StatusFlag.HAS_SYMLINKS:
                    tool.log.warn("%s: one or more URIs contain symlinks.", collection)
                if status & StatusFlag.DOES_NOT_EXIST:
                    tool.log.warn("%s: one or more URIs do not exist.", collection)
                if status & StatusFlag.IS_DIRECTORY:
                    tool.log.warn("%s: one or more URIs are directories.", collection)
