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

"""Definitions for DECam data in the ``/repo/main`` data repository at NCSA.
"""

from __future__ import annotations

__all__ = ()

from pathlib import Path
from typing import Iterator

from ..._operation import AdminOperation
from ... import common
from ... import calibs
from ... import visits
from ...instruments.decam import DECamRawIngest
from ... import doc_templates


@common.Group.wrap("DECam")
def operations() -> Iterator[AdminOperation]:
    """Generate all operations used to set up DECam data in the `/repo/main`
    data repository at NCSA.

    This does not include skymap registration or reference catalog ingest, as
    these are considered shared by all instruments in the data repository.
    """
    yield common.RegisterInstrument("DECam-registration", "lsst.obs.decam.DarkEnergyCamera")
    yield calibs.WriteCuratedCalibrations("DECam-calibs-curated", "DECam", labels=("DM-28638",))
    yield common.DefineChain(
        "DECam-calibs-chain",
        "DECam/calib",
        ["DECam/calib/DM-28638", "DECam/calib/DM-28638/unbounded"],
        doc=doc_templates.DEFAULT_CALIBS.format(instrument="DECam"),
    )
    yield common.DefineChain(
        "DECam-calibs-chain-unbounded",
        "DECam/calib/unbounded",
        ["DECam/calib/DM-28638/unbounded"],
        doc=doc_templates.DEFAULT_CALIBS_UNBOUNDED.format(instrument="DECam"),
    )
    yield from raw_operations()
    yield visits.DefineVisits("DECam-visits", "DECam")
    yield from umbrella_operations()


@common.Group.wrap("DECam-raw")
def raw_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to ingest raw DECam data in the
    `/repo/main` data repository at NCSA.
    """
    root = Path("/datasets/decam/_internal/raw")
    yield DECamRawIngest(
        "DECam-raw-hits2014",
        [
            root.joinpath("hits", d)
            for d in ("Blind14A_04", "Blind14A_09", "Blind14A_10")
        ],
        "DECam",
        tag="DECam/raw/hits2014",
    )
    yield DECamRawIngest(
        "DECam-raw-hits2015",
        [
            root.joinpath("hits", d)
            for d in ("Blind15A_26", "Blind15A_40", "Blind15A_42")
        ],
        "DECam",
        tag="DECam/raw/hits2015",
    )
    yield DECamRawIngest(
        "DECam-raw-hits-calibs",
        [
            "/project/mrawls/hits-raw-calibs/biases",
            "/project/mrawls/hits-raw-calibs/flats",
        ],
        "DECam",
        transfer="copy",
    )


@common.Group.wrap("DECam-umbrella")
def umbrella_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to define convenience umbrella collections
    for DECam in the `/repo/main` data repository at NCSA.
    """
    yield common.DefineChain(
        "DECam-defaults-all",
        "DECam/defaults", (
            "DECam/raw/all", "DECam/calib", "refcats", "skymaps",
        ),
        doc=doc_templates.UMBRELLA.format(tail="all available DECam data.")
    )
    yield common.DefineChain(
        "DECam-defaults-hits2014",
        "DECam/defaults/hits2014", (
            "DECam/raw/hits2014", "DECam/calib", "refcats", "skymaps",
        ),
        doc=doc_templates.UMBRELLA.format(tail="fields 4, 9, and 10 of the HiTS 2014 dataset.")
    )
    yield common.DefineChain(
        "DECam-defaults-hits2015",
        "DECam/defaults/hits2015", (
            "DECam/raw/hits2015", "DECam/calib", "refcats", "skymaps",
        ),
        doc=doc_templates.UMBRELLA.format(tail="fields 26, 40, and 42 of the HiTS 2015 dataset.")
    )
