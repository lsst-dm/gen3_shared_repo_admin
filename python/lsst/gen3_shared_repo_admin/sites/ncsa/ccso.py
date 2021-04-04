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

__all__ = ()

from typing import Iterator, TYPE_CHECKING

from ..._repo_definition import RepoDefinition
from ... import common
from ._site import NCSA

if TYPE_CHECKING:
    from ._operation import AdminOperation


def operations() -> Iterator[AdminOperation]:
    yield common.CreateRepo()
    yield common.RegisterInstrument("LSSTCam-registration", "lsst.obs.lsst.LsstCam")
    yield common.RegisterInstrument("LSSTComCam-registration", "lsst.obs.lsst.LsstComCam")
    yield common.RegisterInstrument("LATISS-registration", "lsst.obs.lsst.Latiss")


def repos() -> Iterator[RepoDefinition]:
    yield RepoDefinition(name="ccso", date="20210215", site=NCSA, operations=operations)