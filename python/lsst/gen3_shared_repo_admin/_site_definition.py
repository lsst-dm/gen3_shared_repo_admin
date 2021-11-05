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

__all__ = ("SiteDefinition", "HomogeneousSiteDefinition")

import dataclasses


@dataclasses.dataclass
class SiteDefinition:
    """Struct that defines URI templates for a particular compute center or
    data facility.
    """

    name: str
    """Unique name for the site.
    """


@dataclasses.dataclass
class HomogeneousSiteDefinition(SiteDefinition):
    """A site subclass where all data repository roots, database URIs, and
    database namespaces share a common naming pattern.

    This site class should be used with the `HomogeneousRepoDefinition` class.
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
