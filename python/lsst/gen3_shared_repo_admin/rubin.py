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

__all__ = ("main_operations",)


from .common import Group, RegisterInstrument


def main_operations() -> Group:
    """Helper function that returns all Rubin instrument admin operations for
    the main data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.

    Notes
    -----
    Raw ingest for Rubin instruments is handled by the Data Backbone Buffer
    Manager, not the repo adminstration tools in this package.
    """
    return Group(
        "Rubin", (
            Group(
                "Rubin-LATISS", (
                    RegisterInstrument("Rubin-LATISS-registration", "lsst.obs.lsst.Latiss"),
                ),
            ),
            Group(
                "Rubin-LSSTCam", (
                    RegisterInstrument("Rubin-LSSTCam-registration", "lsst.obs.lsst.LsstCam"),
                ),
            ),
            Group(
                "Rubin-LSSTComCam", (
                    RegisterInstrument("Rubin-LSSTComCam-registration", "lsst.obs.lsst.LsstComCam"),
                ),
            ),
            Group(
                "Rubin-LSST-TS8", (
                    RegisterInstrument("Rubin-LSST-TS8-registration", "lsst.obs.lsst.LsstTS8"),
                ),
            ),
            Group(
                "Rubin-LSST-TS3", (
                    RegisterInstrument("Rubin-LSST-TS3-registration", "lsst.obs.lsst.LsstTS3"),
                ),
            ),
        )
    )


def ccso_operations() -> Group:
    """Helper function that returns all Rubin instrument admin operations for
    the ccso data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.

    Notes
    -----
    Raw ingest for Rubin instruments is handled by the Data Backbone Buffer
    Manager, not the repo adminstration tools in this package.
    """
    return Group(
        "Rubin", (
            Group(
                "Rubin-LSSTCam", (
                    RegisterInstrument("Rubin-LSSTCam-registration", "lsst.obs.lsst.LsstCam"),
                ),
            ),
            Group(
                "Rubin-LSSTComCam", (
                    RegisterInstrument("Rubin-LSSTComCam-registration", "lsst.obs.lsst.LsstComCam"),
                ),
            ),
            Group(
                "Rubin-LATISS", (
                    RegisterInstrument("Rubin-LATISS-registration", "lsst.obs.lsst.Latiss"),
                ),
            ),
        )
    )


def teststand_operations() -> Group:
    """Helper function that returns all Rubin instrument admin operations for
    the teststand data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.

    Notes
    -----
    Raw ingest for Rubin instruments is handled by the Data Backbone Buffer
    Manager, not the repo adminstration tools in this package.
    """
    return Group(
        "Rubin", (
            Group(
                "Rubin-LATISS", (
                    RegisterInstrument("Rubin-LATISS-registration", "lsst.obs.lsst.Latiss"),
                ),
            ),
            Group(
                "Rubin-LSSTComCam", (
                    RegisterInstrument("Rubin-LSSTComCam-registration", "lsst.obs.lsst.LsstComCam"),
                ),
            ),
        )
    )
