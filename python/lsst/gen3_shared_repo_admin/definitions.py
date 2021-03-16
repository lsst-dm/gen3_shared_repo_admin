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

__all__ = ("REPOS",)

from pathlib import Path
from typing import Iterator, TYPE_CHECKING

# We try hard to avoid importing much of the stack here, as that's slow
# (especially compared to expected command-line responsivity for things like
# checking status).

from ._repo_definition import RepoDefinition
from ._site_definition import SiteDefinition
from . import common
from . import hsc
from . import rubin
from . import refcats
from . import dc2
from . import visits

if TYPE_CHECKING:
    from ._operation import AdminOperation


# Concrete site definitions.

NCSA = SiteDefinition(
    name="NCSA",
    repo_uri_template="/repo/{repo.name}_{repo.date}",
    db_namespace_template="{repo.name}_{repo.date}",
    db_uri_template="postgresql://lsst-pg-prod1.ncsa.illinois.edu:5432/lsstdb1",
)


# Generators for operations.

def main_ncsa_operations() -> Iterator[AdminOperation]:
    yield common.CreateRepo()
    yield common.RegisterSkyMap("hsc_rings_v1")
    yield from refcats.generate(
        "DM-28636",
        Path("/datasets/refcats/htm/v1"),
        (
            "gaia_dr2_20200414",
            "ps1_pv3_3pi_20170110",
            "sdss-dr9-fink-v5b",
        )
    ).flatten()
    yield from hsc.operations().flatten()
    yield from rubin.main_operations().flatten()


def dc2_ncsa_operations() -> Iterator[AdminOperation]:
    yield common.CreateRepo()
    yield common.RegisterSkyMap("DC2")
    yield common.RegisterInstrument("imSim-registration", "lsst.obs.lsst.LsstCamImSim")
    yield common.RegisterInstrument("phoSim-registration", "lsst.obs.lsst.LsstCamPhoSim")
    yield from dc2.raw_operations().flatten()
    yield from dc2.refcat_operations().flatten()
    yield from dc2.calib_operations().flatten()
    yield visits.DefineVisits("2.2i-visits", "LSSTCam-imSim", collections=("2.2i/raw/all",))
    yield from dc2.umbrella_operations().flatten()
    yield from dc2.rerun_operations_DP0().flatten()


def ccso_ncsa_operations() -> Iterator[AdminOperation]:
    yield common.CreateRepo()
    yield rubin.ccso_operations()


def teststand_ncsa_operations() -> Iterator[AdminOperation]:
    yield common.CreateRepo()
    yield rubin.teststand_operations()


REPOS = {
    (repo.name, repo.date, repo.site.name): repo for repo in [
        RepoDefinition(name="main", date="20210215", site=NCSA, operations=main_ncsa_operations),
        RepoDefinition(name="dc2", date="20210215", site=NCSA, operations=dc2_ncsa_operations),
        RepoDefinition(name="ccso", date="20210215", site=NCSA, operations=ccso_ncsa_operations),
        RepoDefinition(name="teststand", date="20210215", site=NCSA, operations=teststand_ncsa_operations),
    ]
}
