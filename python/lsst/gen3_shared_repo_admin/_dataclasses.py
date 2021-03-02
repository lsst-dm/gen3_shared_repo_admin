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

__all__ = ("SiteDefinition", "RepoDefinition")

import dataclasses
from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ._operation import AdminOperation


@dataclasses.dataclass
class SiteDefinition:
    """Struct that defines URI templates for a particular compute center or
    data facility.

    Notes
    -----
    The separation between `SiteDefinition` and `RepoDefinition` regarding what
    is actually site-specific is not great; `SiteDefinition` just contains
    things that are pretty clearly necessary for *all* sites to define, with
    exactly the same structure, while `RepoDefinition` instances often include
    `AdminOperation` lists that really are quite site-specific in practice.
    At present, it just isn't worth the effort to try to fix this, especially
    with only one concrete site to use as an example.
    """

    name: str
    """Unique name for the site.
    """

    repo_uri_template: str
    """Template for the root repo URI.

    This will be processed with `str.format`, passing a single named ``repo``
    argument (a `RepoDefinition` instance).
    """

    db_namespace_template: str
    """Template for the Registry database's ``namespace`` configuration option
    (i.e. schema name).

    This will be processed with `str.format`, passing a single named ``repo``
    argument (a `RepoDefinition` instance).
    """

    db_uri_template: str
    """Template for the Registry database's connection URI.

    This will be processed with `str.format`, passing a single named ``repo``
    argument (a `RepoDefinition` instance).
    """


@dataclasses.dataclass
class RepoDefinition:
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

    date: Optional[str] = None
    """Date string indicating the repository version, YYYYMMDD.

    May be `None` only as way to define a common repository definition that
    will be copied into many date-versioned repository definitions.
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

    operations: Tuple[AdminOperation, ...] = ()
    """Sequence of `AdminOperation` objects that describes the work needed
    to set up this data repository.

    `Group` objects should usually be used to provide some structure.
    The list should be in an order consistent with any dependencies between
    operations that aren't captured by special `Group` subclasses.

    This is a tuple rather than a list to ensure concrete, date-versioned
    `RepoDefinition` objects aren't created with a mutable reference to any
    abstract defintions they are based upon.
    """
