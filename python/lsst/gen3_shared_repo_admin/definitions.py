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
from pathlib import Path

# We try hard to avoid importing much of the stack here, as that's slow
# (especially compared to expected command-line responsivity for things like
# checking status).

from ._dataclasses import RepoDefinition, SiteDefinition
from . import common
from . import hsc
from . import rubin
from . import refcats
from . import dc2
from . import visits


# Template repo definitions that don't include dates, for things that don't
# change with every date.

MAIN = RepoDefinition(
    name="main",
    operations=(
        common.CreateRepo(),
        common.Group(
            "skymaps", (
                common.RegisterSkyMap("hsc_rings_v1"),
            ),
        ),
        common.Group(
            "refcats", tuple(
                refcats.RefCatIngest(
                    name,
                    path=Path(f"/datasets/refcats/htm/v1/{name}"),
                    collection="refcats/DM-28636",
                )
                for name in (
                    "gaia_dr2_20200414",
                    "ps1_pv3_3pi_20170110",
                    "sdss-dr9-fink-v5b",
                )
            ) + (
                common.DefineChain(
                    "refcats-chain",
                    "refcats",
                    ("refcats/DM-28636",),
                    doc="Umbrella collection for all active reference catalogs.",
                ),
            )
        ),
        hsc.operations(),
        rubin.main_operations(),
    ),
)

DC2 = RepoDefinition(
    name="dc2",
    operations=(
        common.CreateRepo(),
        common.Group(
            "skymaps", (
                common.RegisterSkyMap("DC2"),
            ),
        ),
        common.RegisterInstrument("imSim-registration", "lsst.obs.lsst.LsstCamImSim"),
        common.RegisterInstrument("phoSim-registration", "lsst.obs.lsst.LsstCamPhoSim"),
        dc2.raw_operations(),
        dc2.refcat_operations(),
        dc2.calib_operations(),
        visits.DefineVisits("2.2i-visits", "LSSTCam-imSim", collections=("2.2i/raw/all",)),
    ),
)

CCSO = RepoDefinition(
    name="ccso",
    operations=(
        common.CreateRepo(),
        rubin.ccso_operations(),
    ),
)

TESTSTAND = RepoDefinition(
    name="teststand",
    operations=(
        common.CreateRepo(),
        rubin.teststand_operations(),
    ),
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
