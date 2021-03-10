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
    "RegisterInstrument",
    "RegisterSkyMap",
)

import os
from typing import Iterable, Iterator, Tuple, TYPE_CHECKING

from lsst.utils import doImport
from lsst.daf.butler import ButlerURI, Butler, Config, DatasetRef, DimensionConfig

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

    def prep(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        for child in self.children:
            child.prep(tool)

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
    """

    def __init__(self, name: str, tagged: str, query_args: Iterable[Tuple[tuple, dict]]):
        super().__init__(name)
        self.tagged = tagged
        self._query_args = tuple(query_args)

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
        from lsst.daf.butler import CollectionType
        refs = set(self._query(tool))
        tool.log.info("Found %d datasets to associate into %s.", len(refs), self.tagged)
        if not tool.dry_run:
            tool.butler.registry.registerCollection(self.tagged, CollectionType.TAGGED)
            tool.butler.registry.associate(self.tagged, refs)

    def cleanup(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            tool.butler.registry.removeCollection(self.tagged)

    def _query(self, tool: RepoAdminTool) -> Iterator[DatasetRef]:
        """Iterate over all datasets to tag,
        """
        for args, kwargs in self._query_args:
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
    """

    def __init__(self, name: str, chain: str, children: Tuple[str, ...]):
        super().__init__(name)
        self.chain = chain
        self.children = children

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        from lsst.daf.butler.registry import MissingCollectionError
        try:
            children = tool.butler.registry.getCollectionChain(self.chain)
        except MissingCollectionError:
            try:
                c = set(tool.butler.registry.queryCollections(self.children))
            except MissingCollectionError:
                print(f"{' '*indent}{self.name}: blocked; some child collections do not exist")
                return
            if c == set(self.children):
                print(f"{' '*indent}{self.name}: ready to run")
            else:
                print(f"{' '*indent}{self.name}: blocked; child collection(s): {set(self.children) - c}")
        else:
            if tuple(children) == self.children:
                print(f"{' '*indent}{self.name}: done")
            else:
                print(f"{' '*indent}{self.name}: definition changed; run again")

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        from lsst.daf.butler import CollectionType
        if not tool.dry_run:
            tool.butler.registry.registerCollection(self.chain, CollectionType.CHAINED)
            tool.butler.registry.setCollectionChain(self.chain, self.children)

    def cleanup(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            tool.butler.registry.removeCollection(self.chain)
