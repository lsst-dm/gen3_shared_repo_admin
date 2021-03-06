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

"""Definitions for ``/repo/ccso`` at NCSA.

This repository includes only LSST raws written out by the CCS with
``controller='O'``; these cannot be included in the same data repository as
other real data because their data IDs conflict with the primary versions
of those raws.
"""

from __future__ import annotations

__all__ = ()

from typing import Iterator, TYPE_CHECKING

from ..._repo_definition import RepoDefinition
from ... import common
from ._site import NCSA

if TYPE_CHECKING:
    from ._operation import AdminOperation


def repos() -> Iterator[RepoDefinition]:
    """Generate the definitions for the `/repo/ccso` data repository at NCSA.
    """
    yield RepoDefinition(name="ccso", date="20210215", site=NCSA, operations=operations)


def operations() -> Iterator[AdminOperation]:
    """Generate all operations used to set up the `/repo/ccso` data repository
    at NCSA.
    """
    yield common.CreateRepo()
    yield common.RegisterInstrument("LSSTCam-registration", "lsst.obs.lsst.LsstCam")
    yield common.RegisterInstrument("LSSTComCam-registration", "lsst.obs.lsst.LsstComCam")
    yield common.RegisterInstrument("LATISS-registration", "lsst.obs.lsst.Latiss")
