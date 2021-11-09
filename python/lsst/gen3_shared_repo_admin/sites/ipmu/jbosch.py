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

"""Definitions for the ``/gpfs02/work/jbosch/repo`` data repository at IPMU.

This repository contains a subset of the HSC SSP intended for testing and
learning to use the Gen3 middleware.
"""

from __future__ import annotations

__all__ = ()

from pathlib import Path
from typing import Iterator, TYPE_CHECKING

from ..._repo_definition import IndependentRepoDefinition, RepoDefinition
from ... import common
from ... import calibs
from ... import refcats
from ...instruments.hsc import (
    ingest_raws,
    WriteStrayLightData,
)
from ._site import IPMU

if TYPE_CHECKING:
    from ..._operation import AdminOperation


def repos() -> Iterator[RepoDefinition]:
    """Generate the definitions for the ``/gpfs02/work/jbosch/repo`` data
    repository at IPMU.
    """
    yield IndependentRepoDefinition(name="jbosch", date="20211105", site=IPMU, operations=operations,
                                    root="/gpfs02/work/jbosch/repo")


def operations() -> Iterator[AdminOperation]:
    """Generate all operations used to set up the gpfs02/work/jbosch/repo``
    data repository at IPMU.
    """
    yield common.CreateRepo()
    yield common.RegisterSkyMap("hsc_rings_v1")
    yield common.RegisterInstrument("HSC-registration", "lsst.obs.subaru.HyperSuprimeCam")
    yield from refcats.ingest_refcats(
        "gen2",
        Path("/work/hsc/astrometry_net_data"),
        (
            "ps1_pv3_3pi_20170110",
        )
    )
    yield from raw_operations()


@common.Group.wrap("HSC-raw")
def raw_operations() -> Iterator[AdminOperation]:
    top = Path("/gpfs01/hsc/SSP/")
    for subdir in ("SSP_UDEEP_COSMOS",):
        yield from ingest_raws(
            f"HSC-raw-{subdir}",
            top.joinpath(subdir),
            save_found=True,
            follow_symlinks=True,
        )
