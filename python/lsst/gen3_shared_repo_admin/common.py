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
    "CreateRepo",
    "DefineChain",
    "DefineTag",
    "Group",
    "IngestFiles",
    "RegisterInstrument",
    "RegisterSkyMap",
)

import os
from pathlib import Path
from typing import AbstractSet, Any, Iterable, Iterator, Mapping, Optional, Tuple, TYPE_CHECKING

from lsst.utils import doImport
from lsst.daf.butler import (
    ButlerURI,
    Butler,
    CollectionType,
    Config,
    DatasetRef,
    DatasetType,
    DimensionConfig,
    FileDataset,
)
from lsst.daf.butler.registry import MissingCollectionError

from ._operation import AdminOperation, OperationNotReadyError

if TYPE_CHECKING:
    from ._tool import RepoAdminTool
    from lsst.obs.base import Instrument


class Group(AdminOperation):
    """An `AdminOperation` that just delegates to a sequence of other
    `AdminOperation`instances, providing structure.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    children : `tuple` [ `AdminOperation` ]
        Child operation instances.
    """
    def __init__(self, name: str, children: Tuple[AdminOperation, ...]):
        super().__init__(name)
        self.children = tuple(children)

    def flatten(self) -> Iterator[AdminOperation]:
        # Docstring inherited.
        yield self
        for child in self.children:
            yield from child.flatten()

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        print(f"{' '*indent}{self.name}:")
        for child in self.children:
            try:
                child.print_status(tool, indent + 2)
            except OperationNotReadyError as err:
                print(f"{' '*(indent + 2)}{child.name}: blocked; {err}")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        for child in self.children:
            child.run(tool)


class CreateRepo(AdminOperation):
    """A concrete `AdminOperation` that creates an empty data repository.

    This operation is always given the name 'create'.
    """
    def __init__(self):
        super().__init__("create")

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        if os.path.exists(os.path.join(tool.root, "butler.yaml")):
            print(f"{' '*indent}{self.name}: done")
        else:
            print(f"{' '*indent}{self.name}: not started")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            Butler.makeRepo(tool.root, config=self.make_butler_config(tool),
                            dimensionConfig=self.make_dimension_config(tool))

    def make_butler_config(self, tool: RepoAdminTool) -> Config:
        """Assemble the butler configuration to use when creating the data
        repository.

        Returns
        -------
        config : `lsst.daf.butler.Config`
            Butler configuration object.
        """
        config = Config()
        config[".registry.db"] = tool.site.db_uri_template.format(repo=tool.repo)
        config[".registry.namespace"] = tool.site.db_namespace_template.format(repo=tool.repo)
        for template in tool.repo.butler_config_templates:
            uri = ButlerURI(template.format(repo=tool.repo, site=tool.site))
            if uri.exists():
                config.update(Config(uri))
        return config

    def make_dimension_config(self, tool: RepoAdminTool) -> DimensionConfig:
        """Assemble the dimension configuration to use when creating the data
        repository.

        Returns
        -------
        config : `lsst.daf.butler.DimensionConfig`
            Butler dimension configuration object.
        """
        config = DimensionConfig()
        for template in tool.repo.dimension_config_templates:
            uri = ButlerURI(template.format(repo=tool.repo, site=tool.site))
            if uri.exists():
                config.update(Config(uri))
        return config


class RegisterInstrument(AdminOperation):
    """A concrete `AdminOperation` that calls `Instrument.register` on a
    data repository.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    instrument_class_name : `str`
        Fully-qualified path to the instrument class.  This is passed as a
        string instead of a type or instance to defer imports (which can be
        very slow) until they are actually needed, rather than include them
        in `RepoDefinition` object instantiations.
    """
    def __init__(self, name: str, instrument_class_name: str):
        super().__init__(name)
        self.instrument_class_name = instrument_class_name

    @property
    def instrument(self) -> Instrument:
        """An instance of the `Instrument` class to be registered.
        """
        cls = doImport(self.instrument_class_name)
        return cls()

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        try:
            tool.butler.registry.expandDataId(instrument=self.instrument.getName())
        except LookupError:
            print(f"{' '*indent}{self.name}: not started")
        else:
            print(f"{' '*indent}{self.name}: done")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            self.instrument.register(tool.butler.registry)


class RegisterSkyMap(AdminOperation):
    """A concrete `AdminOperation` that calls `BaseSkyMap.register`, using
    configuration packaged within `gen3_shared_repo_admin` itself.

    Parameters
    ----------
    skymap_name : `str`
        Name for the skymap dimension record; also used as the filename
        (without extension) for the config file, and the operation name
        (with a ``skymaps-`` prefix).
    """
    def __init__(self, skymap_name: str) -> None:
        super().__init__(f"skymaps-{skymap_name}")
        self.skymap_name = skymap_name
        self.config_uri = f"resource://lsst.gen3_shared_repo_admin/config/skymaps/{skymap_name}.py"

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        try:
            tool.butler.registry.expandDataId(skymap=self.name)
        except LookupError:
            print(f"{' '*indent}{self.name}: not started")
        else:
            print(f"{' '*indent}{self.name}: done")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        from lsst.pipe.tasks.script.registerSkymap import MakeSkyMapConfig
        config = MakeSkyMapConfig()
        config.loadFromStream(ButlerURI(self.config_uri).read().decode())
        assert config.name == self.skymap_name
        tool.log.info("Constructing SkyMap '%s' from configuration.", config.name)
        skymap = config.skyMap.apply()
        skymap.logSkyMapInfo(tool.log)
        tool.log.info("Registering SkyMap '%s' in database.", config.name)
        if not tool.dry_run:
            skymap.register(config.name, tool.butler)


class DefineTag(AdminOperation):
    """A concrete `AdminOperation` that defines a
    `~lsst.daf.butler.CollectionType.TAGGED` collection.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    tagged : `str`
        Named of the `~lsst.daf.butler.CollectionType.TAGGED` collection to
        create.
    query_args : `Iterable`
        Iterable of ``(*args, **kwargs)`` pairs for
        `lsst.daf.butler.Registry.queryDatasets`, to use to obtain the datasets
        to associate into ``tagged``.
    doc : `str`
        Documentation string for this collection.
    """

    def __init__(self, name: str, tagged: str, query_args: Iterable[Tuple[tuple, dict]],
                 doc: str):
        super().__init__(name)
        self.tagged = tagged
        self._query_args = tuple(query_args)
        self.doc = doc

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        from lsst.daf.butler.registry import MissingCollectionError
        try:
            refs = set(tool.butler.registry.queryDatasets(..., collections=self.tagged))
        except MissingCollectionError:
            print(f"{' '*indent}{self.name}: not started")
        else:
            if refs == set(self._query(tool)):
                print(f"{' '*indent}{self.name}: done")
            else:
                print(f"{' '*indent}{self.name}: definition changed; run again")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        refs = set(self._query(tool))
        tool.log.info("Found %d datasets to associate into %s.", len(refs), self.tagged)
        if not tool.dry_run:
            tool.butler.registry.registerCollection(self.tagged, CollectionType.TAGGED)
            tool.butler.registry.associate(self.tagged, refs)
            tool.butler.registry.setCollectionDocumentation(self.tagged, self.doc)

    def cleanup(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            tool.butler.registry.removeCollection(self.tagged)

    def _query(self, tool: RepoAdminTool) -> Iterator[DatasetRef]:
        """Iterate over all datasets to tag,
        """
        for args, kwargs in tool.progress.wrap(self._query_args, desc=f"Querying for {self.tagged} datasets"):
            yield from tool.butler.registry.queryDatasets(*args, **kwargs)


class DefineChain(AdminOperation):
    """A concrete `AdminOperation` that defines a
    `~lsst.daf.butler.CollectionType.CHAINED` collection.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    chain : `str`
        Named of the `~lsst.daf.butler.CollectionType.CHAINED` collection to
        create.
    children : `tuple` [ `str` ]
        Names of the child collections.
    doc : `str`
        Documentation string for this collection.
    flatten : `bool`, optional
        If `True`, recursively flatten any chains in ``children`` before
        defining the chain.
    """

    def __init__(self, name: str, chain: str, children: Tuple[str, ...], doc: str, flatten: bool = False):
        super().__init__(name)
        self.chain = chain
        self.children = children
        self.doc = doc
        self._flatten = flatten

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        try:
            children = tool.butler.registry.getCollectionChain(self.chain)
        except MissingCollectionError:
            try:
                c = set(tool.butler.registry.queryCollections(self.children, flattenChains=self._flatten))
            except MissingCollectionError:
                print(f"{' '*indent}{self.name}: blocked; some child collections do not exist")
                return
            print(f"{' '*indent}{self.name}: ready to run")
        else:
            try:
                c = tuple(tool.butler.registry.queryCollections(self.children, flattenChains=self._flatten))
            except MissingCollectionError:
                print(f"{' '*indent}{self.name}: blocked; some child collections do not exist")
                return
            if tuple(children) == c:
                print(f"{' '*indent}{self.name}: done")
            else:
                print(f"{' '*indent}{self.name}: definition changed; run again")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            tool.butler.registry.registerCollection(self.chain, CollectionType.CHAINED)
            tool.butler.registry.setCollectionChain(self.chain, self.children, flatten=self._flatten)
            tool.butler.registry.setCollectionDocumentation(self.chain, self.doc)

    def cleanup(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            tool.butler.registry.removeCollection(self.chain)


class IngestFiles(AdminOperation):

    def __init__(self, name: str, collection: str,
                 dataset_type_name: str,
                 dimensions: AbstractSet[str],
                 storage_class: str,
                 datasets: Mapping[Path, Mapping[str, Any]],
                 transfer: Optional[str]):
        super().__init__(name)
        self.collection = collection
        self.dataset_type_name = dataset_type_name
        self.dimensions = dimensions
        self.storage_class = storage_class
        self.datasets = datasets
        self.transfer = transfer

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        try:
            dataset_type = tool.butler.registry.getDatasetType(self.dataset_type_name)
        except KeyError:
            print(f"{' '*indent}{self.name}: not started.")
            return
        n_found = 0
        n_total = 0
        for file_dataset in self._file_datasets(tool, dataset_type):
            for ref in file_dataset.refs:
                resolved_ref = tool.butler.registry.findDataset(ref.datasetType, ref.dataId,
                                                                collections=[self.collection])
                if resolved_ref is not None:
                    n_found += 1
                n_total += 1
        if n_found == n_total:
            print(f"{' '*indent}{self.name}: done; {n_total} dataset(s) ingested.")
        elif n_found == 0:
            print(f"{' '*indent}{self.name}: dataset type and collection registered; "
                  f"{n_total} dataset(s) to ingest.")
        else:
            print(f"{' '*indent}{self.name}: in progress; {n_found} of {n_total} dataset(s) ingested.")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        dataset_type = self._dataset_type(tool)
        if not tool.dry_run:
            tool.butler.registry.registerCollection(self.collection, CollectionType.RUN)
            tool.butler.registry.registerDatasetType(dataset_type)
            tool.butler.ingest(*self._file_datasets(tool, dataset_type),
                               transfer=self.transfer, run=self.collection)

    def _dataset_type(self, tool: RepoAdminTool) -> DatasetType:
        return DatasetType(self.dataset_type_name, dimensions=self.dimensions,
                           storageClass=self.storage_class,
                           universe=tool.butler.registry.dimensions)

    def _file_datasets(self, tool: RepoAdminTool, dataset_type: Optional[DatasetType] = None,
                       ) -> Iterator[FileDataset]:
        if dataset_type is None:
            dataset_type = self._dataset_type(tool)
        for path, data_id in self.datasets.items():
            yield FileDataset(
                refs=[DatasetRef(dataset_type, data_id, conform=True)],
                path=str(path),
            )
