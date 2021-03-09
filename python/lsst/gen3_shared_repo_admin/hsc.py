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
from typing import Tuple, TYPE_CHECKING

from ._operation import SimpleStatus
from .ingest import DeduplicatingRawIngestGroup, RawIngest
from .calibs import CalibrationOperation, ConvertCalibrations, WriteCuratedCalibrations
from .common import Group, RegisterInstrument, DefineChain
from .visits import DefineVisits

if TYPE_CHECKING:
    from ._tool import RepoAdminTool


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


class WriteStrayLightData(CalibrationOperation):
    """A concrete `AdminOperation` that copies HSC's special y-band stray light
    data file from a Gen2 repo.

    This operation assumes it is the only operation working with its output
    collections.
    """

    def __init__(self, name: str, labels: Tuple[str, ...], directory: str):
        super().__init__(name, "HSC", labels)
        self.directory = directory

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        SimpleStatus.check(self, tool).print_status(self, tool, indent)

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        if not tool.dry_run:
            with SimpleStatus.run_context(self, tool):
                self.instrument(tool).ingestStrayLightData(
                    tool.butler,
                    directory=self.directory,
                    transfer="direct",
                    labels=self.labels,
                )


def convert_calibs(subdir: str, top="/datasets/hsc/calib", root="/datasets/hsc/repo") -> Group:
    """Helper function to generate a `ConvertCalibrations` appropriate for a
    Gen2 HSC calibration repo.

    Parameters
    ----------
    subdir : `str`
        Subdirectory of ``top`` that contains the calibration repo.  Also used
        as the operation name.
    top : `str`, optional
        Root directory for all raws for this instrument.
    root : `str`, optional
        Path to the HSC Gen2 repository root.

    Returns
    -------
    group : `Group`
        A group of calibration conversion operations.
    """
    return Group(
        f"HSC-calibs-{subdir}", (
            ConvertCalibrations(
                name=f"HSC-calibs-{subdir}-convert",
                instrument_name="HSC",
                labels=("gen2", subdir),
                root=root,
                repo_path=os.path.join(root, top, subdir),
            ),
            WriteStrayLightData(
                name=f"HSC-calibs-{subdir}-straylight",
                labels=("gen2", subdir),
                directory=os.path.join(top, subdir, "STRAY_LIGHT"),
            ),
        )
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
            ),
            Group(
                "HSC-calibs", (
                    WriteCuratedCalibrations("HSC-calibs-curated", "HSC", labels=("DM-28636",)),
                    convert_calibs("20180117"),
                    convert_calibs("20200115"),
                    DefineChain("HSC-calibs-default", "HSC/calib", (
                        "HSC/calib/gen2/20180117",
                        "HSC/calib/DM-28636",
                        "HSC/calib/gen2/20180117/unbounded",
                        "HSC/calib/DM-28636/unbounded",
                    )),
                ),
            ),
            DefineVisits("HSC-visits", "HSC"),
        )
    )
