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
from ... import calibs
from ... import common
from ... import ingest
from ... import doc_templates
from ... import refcats
from ... import visits
from ._site import NCSA
from . import hsc
from . import decam

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
    yield from rubin_operations("LATISS", "lsst.obs.lsst.Latiss")
    yield from rubin_operations("LSSTComCam", "lsst.obs.lsst.LsstComCam")
    yield from rubin_operations("LSSTCam", "lsst.obs.lsst.LsstCam")
    yield from rubin_operations("LSST-TS8", "lsst.obs.lsst.LsstTS8")
    yield from rubin_operations("LSST-TS3", "lsst.obs.lsst.LsstTS3")
    yield from decam.operations()
    yield ingest.PatchExistingExposures(
        "LATISS-ingest-patch",
        "LATISS",
        where="exposure.observation_reason='science' AND exposure.sky_angle = NULL",
    )
    yield visits.PatchExistingVisits("LATISS-visits-patch", "LATISS", visit_system="one-to-one")
    yield visits.DefineVisits("LATISS-visits", "LATISS", visit_system="one-to-one")


def rubin_operations(name: str, class_name: str) -> Iterator[AdminOperation]:
    """Generate operations used to set up a Rubin instrument in the
    `/repo/main` repository.

    This registers the instrument, writes curated calibrations, and sets up the
    default calibration collection pointers to point to just that.
    """
    yield common.RegisterInstrument(f"{name}-registration", class_name)
    yield common.Group(
        f"{name}-calibs", (
            calibs.WriteCuratedCalibrations(f"{name}-calibs-curated", name, labels=("DM-28636",)),
            common.DefineChain(
                f"{name}-calibs-default",
                f"{name}/calib", (
                    f"{name}/calib/DM-28636",
                    f"{name}/calib/DM-28636/unbounded",
                ),
                doc=doc_templates.DEFAULT_CALIBS.format(instrument=name),
            ),
            common.DefineChain(
                f"{name}-calibs-default-unbounded",
                f"{name}/calib/unbounded", (
                    f"{name}/calib/DM-28636/unbounded",
                ),
                doc=doc_templates.DEFAULT_CALIBS_UNBOUNDED.format(instrument=name),
            ),
        )
    )
