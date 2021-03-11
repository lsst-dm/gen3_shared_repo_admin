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

import os
from pathlib import Path
from typing import Dict, Set, TYPE_CHECKING

from .calibs import WriteCuratedCalibrations
from .common import DefineChain, Group
from .ingest import DefineRawTag, ExposureFinder, RawIngest
from .refcats import RefCatIngest
from . import doc_templates

if TYPE_CHECKING:
    from ._tool import RepoAdminTool


class _ExposureFinder(ExposureFinder):
    """An `ExposureFinder` implementation for DC2 data (and possibly,
    accidentally, the way some of it is organized at NCSA).

    This finder does not expect to ingest only full exposures, as apparently
    much of the dataset at NCSA involved transfers of partial exposures,
    presumably corresponding to tract boundaries.  Symbolic links are always
    followed.

    Parameters
    ----------
    root : `str`
        Root path to search; expected to directly containing subdirectories
        that each map to exactly one exposure (with the exposure ID as the
        subdirectory name).
    file_pattern : `str`
        Glob pattern that raw files must match.
    """

    def __init__(self, root: str, file_pattern: str):
        self._root = Path(root)
        self._file_pattern = file_pattern

    def find(self, tool: RepoAdminTool) -> Dict[int, Path]:
        # Docstring inherited.
        result = {}
        for entry in tool.progress.wrap(os.scandir(self._root), desc=f"Scanning {self._root}"):
            if entry.is_dir(follow_symlinks=True):
                result[int(entry.name)] = entry.path
        return result

    def expand(self, tool: RepoAdminTool, exposure_id: int, found: Dict[int, Path]) -> Set[Path]:
        # Docstring inherited.
        path = found[exposure_id]
        return set(self.recursive_glob(tool, path, self._file_pattern, follow_symlinks=True))


def raw_operations() -> Group:
    """Helper function that returns all raw ingest admin operations for
    the DC2 data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    dr6_finder = _ExposureFinder(
        "/datasets/DC2/DR6/Run2.2i/patched/2021-02-10/raw",
        "*-R??-S??-det???-???.fits",
    ).saved_as("2.2i-raw-DR6-find")
    monthly_finder = _ExposureFinder(
        "/datasets/DC2/repoRun2.2i/raw",
        "*-R??-S??-det???.fits",
    )
    return Group(
        "2.2i", (
            Group(
                "2.2i-raw", (
                    RawIngest(
                        "2.2i-raw-DR6",
                        dr6_finder,
                        instrument_name="LSSTCam-imSim",
                        collection="2.2i/raw/all",
                    ).split_into(4).save_found(),
                    RawIngest(
                        "2.2i-raw-monthly",
                        monthly_finder,
                        instrument_name="LSSTCam-imSim",
                        collection="2.2i/raw/all",
                    ).save_found(),
                    DefineRawTag(
                        "2.2i-raw-tag-DP0",
                        dr6_finder,
                        instrument_name="LSSTCam-imSim",
                        input_collection="2.2i/raw/all",
                        output_collection="2.2i/raw/DP0",
                        doc="ImSim raw images designated for use in Data Preview 0.",
                    ),
                    DefineRawTag(
                        "2.2i-raw-tag-med",
                        monthly_finder,
                        instrument_name="LSSTCam-imSim",
                        input_collection="2.2i/raw/all",
                        output_collection="2.2i/raw/test-med-1",
                        doc="ImSim raw images used as inputs for DM's medium-scale regular test processing.",
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


def calib_operations() -> Group:
    """Helper function that returns all calibration ingest admin operations for
    the DC2 data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    return Group(
        "2.2i-calibs", (
            WriteCuratedCalibrations("2.2i-calibs-curated", "LSSTCam-imSim", labels=("PREOPS-301",),
                                     collection_prefix="2.2i"),
            DefineChain(
                "2.2i-calibs-default",
                "2.2i/calib", (
                    "2.2i/calib/PREOPS-301",
                    "2.2i/calib/PREOPS-301/unbounded",
                ),
                doc=doc_templates.DEFAULT_CALIBS.format(instrument="Run2.2i"),
            )
        )
    )
