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

"""Definitions for the ``/repo/main`` data repository at NCSA.

This data repository includes all data from real instruments, except data from
non-primary controllers whose data IDs conflict with the primary versions of
those same raws (e.g. ``/repo/ccso``).
"""

from __future__ import annotations

__all__ = ()

from pathlib import Path
from typing import Iterator, TYPE_CHECKING

from ..._repo_definition import RepoDefinition
from ... import common
from ... import refcats
from ._site import NCSA
from . import hsc

if TYPE_CHECKING:
    from ._operation import AdminOperation


def repos() -> Iterator[RepoDefinition]:
    """Generate the definitions for the `/repo/main` data repository at NCSA.
    """
    yield RepoDefinition(name="main", date="20210215", site=NCSA, operations=operations)


def operations() -> Iterator[AdminOperation]:
    """Generate all operations used to set up the `/repo/main` data repository
    at NCSA.
    """
    yield common.CreateRepo()
    yield common.RegisterSkyMap("hsc_rings_v1")
    yield from refcats.ingest_refcats(
        "DM-28636",
        Path("/datasets/refcats/htm/v1"),
        (
            "gaia_dr2_20200414",
            "ps1_pv3_3pi_20170110",
            "sdss-dr9-fink-v5b",
        )
    )
    yield from hsc.operations()
    yield common.RegisterInstrument("LATISS-registration", "lsst.obs.lsst.Latiss")
    yield common.RegisterInstrument("LSSTCam-registration", "lsst.obs.lsst.LsstCam")
    yield common.RegisterInstrument("LSSTComCam-registration", "lsst.obs.lsst.LsstComCam")
    yield common.RegisterInstrument("LSST-TS8-registration", "lsst.obs.lsst.LsstTS8")
    yield common.RegisterInstrument("LSST-TS3-registration", "lsst.obs.lsst.LsstTS3")
