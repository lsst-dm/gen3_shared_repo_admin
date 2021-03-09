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

__all__ = ("raw_operations", "refcat_operations")

from pathlib import Path

from .common import Group
from .ingest import DeduplicatingRawIngestGroup, RawIngest
from .refcats import RefCatIngest


def raw_operations() -> Group:
    """Helper function that returns all raw ingest admin operations for
    the DC2 data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    return Group(
        "2.2i", (
            DeduplicatingRawIngestGroup(
                "2.2i-raw", (
                    RawIngest(
                        "2.2i-raw-DR6",
                        find_files=RawIngest.find_file_glob(
                            "/datasets/DC2/DR6/Run2.2i/patched/2021-02-10/raw",
                            "*-R??-S??-det???-???.fits",
                            follow_symlinks=True,
                        ),
                        collection="2.2i/raw/all",
                    ),
                    RawIngest(
                        "2.2i-raw-monthly",
                        find_files=RawIngest.find_file_glob(
                            "/datasets/DC2/repoRun2.2i/raw",
                            "*-R??-S??-det???.fits",
                            follow_symlinks=True,
                        ),
                        collection="2.2i/raw/all",
                    ),
                ),
            ),
        )
    )


def refcat_operations() -> Group:
    """Helper function that returns all refcat ingest admin operations for
    the DC2 data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    return Group(
        "refcats", (
            RefCatIngest(
                "cal_ref_cat_2_2",
                path=Path("/datasets/DC2/DR6/Run2.2i/patched/2021-02-10/ref_cats/cal_ref_cat"),
                collection="refcats/DM-28636",
            ),
        )
    )
