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

__all__ = ("REPOS", "SITES")

import dataclasses

from ._dataclasses import RepoDefinition, SiteDefinition


# Template repo definitions that don't include dates, for things that don't
# change with every date.

MAIN = RepoDefinition(
    name="main",
    skymaps=[
        "resource://lsst.gen3_shared_repo_admin/config/skymaps/hsc_rings_v1.py",
    ]
)

DC2 = RepoDefinition(
    name="dc2",
    skymaps=[
        "resource://lsst.gen3_shared_repo_admin/config/skymaps/DC2.py",
    ]
)

CCSO = RepoDefinition(
    name="ccso",
)

TESTSTAND = RepoDefinition(
    name="teststand",
)

# Concrete repo definitions that do include dates, and whatever specializations
# those involve, grouped by name then date.

REPOS = {
    "main": {
        "20210215": dataclasses.replace(MAIN, date="20210215"),
    },
    "dc2": {
        "20210215": dataclasses.replace(DC2, date="20210215"),
    },
    "ccso": {
        "20210215": dataclasses.replace(CCSO, date="20210215"),
    },
    "teststand": {
        "20210215": dataclasses.replace(TESTSTAND, date="20210215"),
    },
}

# Concrete site definitions.

SITES = {
    "NCSA": SiteDefinition(
        name="ncsa",
        repo_uri_template="/repo/{repo.name}_{repo.date}",
        db_namespace_template="{repo.name}_{repo.date}",
        db_uri_template="postgresql://lsst-pg-prod1.ncsa.illinois.edu:5432/lsstdb1",
    ),
}
