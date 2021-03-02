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

__all__ = ("operations",)

import os

from .ingest import DeduplicatingRawIngestGroup, RawIngest
from .common import Group, RegisterInstrument


def raw_ingest(subdir: str, top="/datasets/hsc/raw") -> RawIngest:
    """Helper function to generate a `RawIngest` appropriate for finding
    HSC raws (recursively) in a directory.

    Parameters
    ----------
    subdir : `str`
        Subdirectory of ``top`` to search.  Also used as the operation name.
    top : `str`, optional
        Root directory for all raws for this instrument.

    Returns
    -------
    ingest : `RawIngest`
        A raw ingest operation.
    """
    return RawIngest(
        name=f"HSC-raw-{subdir}",
        find_files=RawIngest.find_file_glob(
            top_template=os.path.join(top, subdir),
            pattern_template="HSCA*.fits"
        ),
    )


def operations() -> Group:
    """Helper function that returns all HSC-specific operations.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    return Group(
        "HSC", (
            RegisterInstrument("HSC-registration", "lsst.obs.subaru.HyperSuprimeCam"),
            DeduplicatingRawIngestGroup(
                "HSC-raw", tuple(
                    raw_ingest(s) for s in (
                        "commissioning",
                        "cosmos",
                        "newhorizons",
                        "ssp_extra",
                        "ssp_pdr1",
                        "ssp_pdr2",
                        "sxds-i2",
                    )
                )
            )
        )
    )
