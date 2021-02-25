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
from typing import Optional


@dataclasses.dataclass
class SiteDefinition:
    name: str
    repo_uri_template: str
    db_namespace_template: str
    db_uri_template: str


@dataclasses.dataclass
class RepoDefinition:
    name: str
    date: Optional[str] = None

    butler_config_templates: list[str] = dataclasses.field(
        default_factory=lambda: [
            "resource://lsst.gen3_shared_repo_admin/config/butler/{repo.date}.yaml",
            "resource://lsst.gen3_shared_repo_admin/config/butler/{repo.name}.yaml",
            "resource://lsst.gen3_shared_repo_admin/config/butler/{repo.name}_{repo.date}.yaml",
            # Hope we never need additional per-site overrides, but room for
            # them here if we do.
        ]
    )
    dimension_config_templates: list[str] = dataclasses.field(
        default_factory=lambda: [
            "resource://lsst.gen3_shared_repo_admin/config/dimension/{repo.date}.yaml",
            "resource://lsst.gen3_shared_repo_admin/config/dimension/{repo.name}.yaml",
            "resource://lsst.gen3_shared_repo_admin/g/dimension/{repo.name}_{repo.date}.yaml",
            # Hope we never need additional per-site overrides, but room for
            # them here if we do.
        ]
    )

    skymaps: list[str] = dataclasses.field(default_factory=list)