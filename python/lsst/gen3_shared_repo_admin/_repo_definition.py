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

__all__ = ("RepoDefinition",)

import dataclasses
from typing import Callable, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from ._site_definition import SiteDefinition
    from ._operation import AdminOperation


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

    date: str
    """Date string indicating the repository version, YYYYMMDD.
    """

    site: SiteDefinition
    # TODO

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
