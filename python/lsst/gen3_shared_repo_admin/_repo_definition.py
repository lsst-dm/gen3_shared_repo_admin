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

from abc import abstractmethod

__all__ = ("RepoDefinition", "HomogeneousRepoDefinition", "IndependentRepoDefinition")

import dataclasses
from abc import ABC
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional

if TYPE_CHECKING:
    from ._operation import AdminOperation
    from ._site_definition import HomogeneousSiteDefinition, SiteDefinition


@dataclasses.dataclass
class RepoDefinition(ABC):
    """Struct that defines a particular instance of data repository.

    Notes
    -----
    A single `RepoDefinition` can nominally represent different instantiations
    of a data repository at different sites, via its interaction with a `Site`
    instance, but this assumes the sites really are quite similar.
    """

    name: str
    """Name, not including any date component.
    """

    date: str
    """Date string indicating the repository version, YYYYMMDD.
    """

    site: SiteDefinition
    """Definitions for the site that hosts this data repository.
    """

    operations: Callable[[], Iterator[AdminOperation]]
    """Callable that returns an iterator over `AdminOperation` objects that
    define the work needed to set up this data repository.
    """

    butler_config_templates: list[str] = dataclasses.field(
        default_factory=lambda: [
            "resource://lsst.gen3_shared_repo_admin/config/butler/{repo.date}.yaml",
            "resource://lsst.gen3_shared_repo_admin/config/butler/{repo.name}.yaml",
            "resource://lsst.gen3_shared_repo_admin/config/butler/{repo.name}_{repo.date}.yaml",
            # Hope we never need additional per-site overrides, but room for
            # them here if we do.
        ]
    )
    """URI templates for butler config overrides.

    This will be processed with `str.format`, passing named ``repo``
    (`RepoDefinition`) and ``site`` (`SiteDefinition`) arguments.

    Templates that evaluate to URIs that do not exist are ignored.
    """

    dimension_config_templates: list[str] = dataclasses.field(
        default_factory=lambda: [
            "resource://lsst.gen3_shared_repo_admin/config/dimension/{repo.date}.yaml",
            "resource://lsst.gen3_shared_repo_admin/config/dimension/{repo.name}.yaml",
            "resource://lsst.gen3_shared_repo_admin/g/dimension/{repo.name}_{repo.date}.yaml",
            # Hope we never need additional per-site overrides, but room for
            # them here if we do.
        ]
    )
    """URI templates for dimension config overrides.

    This will be processed with `str.format`, passing named ``repo``
    (`RepoDefinition`) and ``site`` (`SiteDefinition`) arguments.

    Templates that evaluate to URIs that do not exist are ignored.
    """

    @property
    @abstractmethod
    def root(self) -> str:
        """The repository root URI."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def db_uri(self) -> str:
        """The database URI used for the Registry."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def db_namespace(self) -> Optional[str]:
        """The database namespace/schema used for the registry."""
        raise NotImplementedError()


@dataclasses.dataclass
class HomogeneousRepoDefinition(RepoDefinition):
    """A repo subclass where all data repository roots, database URIs, and
    database namespaces at a site share a common naming pattern.

    This repo class should be used with the `HomogeneousSiteDefinition` class.
    """

    site: HomogeneousSiteDefinition

    @property
    def root(self) -> str:
        return self.site.repo_uri_template.format(repo=self)

    @property
    def db_uri(self) -> str:
        return self.site.db_uri_template.format(repo=self)

    @property
    def db_namespace(self) -> Optional[str]:
        if self.site.db_namepace_template is not None:
            return self.site.db_namespace_template.format(repo=self)
        else:
            return None


class IndependentRepoDefinition(RepoDefinition):
    """A repo subclass that stores its root, database URI, and database
    namespace explicitly.
    """
    def __init__(
        self,
        root: str,
        db_uri: Optional[str] = None,
        db_namespace: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._root = root
        self._db_uri = (
            db_uri if db_uri is not None else f"sqlite://{self._root}/gen3.sqlite3"
        )
        self._db_namespace = db_namespace

    @property
    def root(self) -> str:
        return self._root

    @property
    def db_uri(self) -> str:
        return self._db_uri

    @property
    def db_namespace(self) -> Optional[str]:
        return self._db_namespace
