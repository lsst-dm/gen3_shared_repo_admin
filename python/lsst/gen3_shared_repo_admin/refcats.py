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

__all__ = ("RefCatIngest",)

import os
from pathlib import Path
import re
from typing import Iterable, Iterator, TYPE_CHECKING

from ._operation import AdminOperation, SimpleStatus
from .common import DefineChain, Group

if TYPE_CHECKING:
    from ._tool import RepoAdminTool


class RefCatIngest(AdminOperation):
    """A concrete `AdminOperation` that ingests an already-shared reference
    catalog.

    Parameters
    ----------
    refcat_name : `str`
        Name of the reference catalog; used as the dataset type name.
    path : `Path`
        Full path to the directory containing all reference catalog files.
    collection : `str`
        Name of the collection that reference catalog datasets should be
        ingested into.

    Notes
    -----
    This class's `run` method is largely copied (with small modifications from)
    `lsst.obs.base.ConvertRepoTask` and its helper objects, and probably should
    find a long-term home in ``meas_algorithms``.
    """
    def __init__(self, refcat_name: str, path: Path, collection: str) -> None:
        super().__init__(f"refcats-{refcat_name}")
        self.refcat_name = refcat_name
        self.path = path
        self.collection = collection

    REGEX = re.compile(r"(\d+)\.fits")

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        SimpleStatus.check(self, tool).print_status(self, tool, indent)

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        from lsst.daf.butler import CollectionType, DatasetRef, DatasetType, FileDataset
        from lsst.meas.algorithms import DatasetConfig as RefCatDatasetConfig
        config_file = self.path.joinpath("config.py")
        if not config_file.exists():
            raise FileNotFoundError(f"No configuration file for refcat {self.refcat_name} at {config_file}.")
        config = RefCatDatasetConfig()
        config.load(str(config_file))
        if config.indexer.name != "HTM":
            raise ValueError(f"Reference catalog '{self.refcat_name}' uses unsupported "
                             f"pixelization '{config.indexer.name}'.")
        level = config.indexer["HTM"].depth
        try:
            dimension = tool.butler.registry.dimensions[f"htm{level}"]
        except KeyError as err:
            raise ValueError(f"Reference catalog {self.refcat_name} uses HTM level {level}, but no "
                             f"htm{level} skypix dimension is configured for this registry.") from err
        dataset_type = DatasetType(self.refcat_name.replace("-", "_"), dimensions=[dimension],
                                   universe=tool.butler.registry.dimensions,
                                   storageClass="SimpleCatalog")
        datasets = []
        for entry in tool.progress.wrap(os.scandir(self.path), desc=f"Scanning files for {self.refcat_name}"):
            if (m := self.REGEX.match(entry.name)) is not None:
                htmId = int(m.group(1))
                dataId = tool.butler.registry.expandDataId({dimension: htmId})
                datasets.append(FileDataset(entry.path, refs=DatasetRef(dataset_type, dataId)))
        if not tool.dry_run:
            with SimpleStatus.run_context(self, tool):
                tool.butler.registry.registerCollection(self.collection, CollectionType.RUN)
                tool.butler.registry.registerDatasetType(dataset_type)
                tool.butler.ingest(*datasets, transfer="direct", run=self.collection)


def generate(ticket: str, root: Path, names: Iterable[str]) -> Iterator[AdminOperation]:
    ingests = tuple(
        RefCatIngest(
            name,
            path=Path(f"/datasets/refcats/htm/v1/{name}"),
            collection=f"refcats/{ticket}",
        )
        for name in names
    )
    chain = DefineChain(
        "refcats-chain",
        "refcats",
        (f"refcats/{ticket}",),
        doc="Umbrella collection for all active reference catalogs.",
    )
    return Group(
        "refcats",
        ingests + (chain,)
    ).flatten()
