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
from pathlib import Path
from typing import Callable, Dict, Optional, Set, Tuple, TYPE_CHECKING

from ._operation import SimpleStatus
from .ingest import RawIngest, ExposureFinder
from .calibs import CalibrationOperation, ConvertCalibrations, WriteCuratedCalibrations
from .common import Group, RegisterInstrument, DefineChain
from .visits import DefineVisits

if TYPE_CHECKING:
    from ._tool import RepoAdminTool


class _ExposureFinder(ExposureFinder):
    """An `ExposureFinder` implementation for HSC data (and possibly,
    accidentally, the way some of it is organized at NCSA).

    This finder  expects many exposures to be present in directories, and makes
    no assumptions about how those directories are organized.  It does assume
    that the filenames themselves are the original ``HSCA*.fits`` names,
    allowing exposure IDs to be derived from those names.  Symbolic links are
    never followed.

    Parameters
    ----------
    root : `str`
        Root path to search; subdirectories are searched recursively for
        matching files.
    allow_incomplete : `bool`, optional
        If `True`, allow incomplete exposures that do not have a full
        complement of detectors (including wavefront sensors, but not focus
        sensors).  Default is `False`.
    resolve_duplicates : `Callable`
        A callable that takes two `Path` arguments and returns a new `Path`
        (or `None`), to be invoked when the finder detects two directories
        that each contain a raw from the same exposure (but not necessarily
        the same one), indicating which is preferred.  The default always
        returns `None`, which causes `RuntimeError` to be raised.
    """

    def __init__(self, root: Path, allow_incomplete: bool = False,
                 resolve_duplicates: Callable[[Path, Path], Optional[Path]] = lambda a, b: None):
        self._root = root
        self._allow_incomplete = allow_incomplete
        self._resolve_duplicates = resolve_duplicates

    FILE_REGEX = r"HSCA(\d{8}).fits"

    DETECTOR_NUMS_FOR_FILENAMES = (
        list(range(0, 49)) + list(range(51, 58)) + list(range(100, 149)) + list(range(151, 158))
    )
    """HSC internal detector IDs used in filenames.
    """

    def find(self, tool: RepoAdminTool) -> Dict[int, Path]:
        # Docstring inherited.
        result = {}
        for path, match in self.recursive_regex(tool, self._root, self.FILE_REGEX):
            # HSC visit/exposure IDs are always even-numbered, to allow for
            # more than 100 CCDs while otherwise using the same pattern as the
            # old Supreme-Cam.  The CCD identifiers here aren't the
            # pure-integer ones we prefer to use in the pipelines, so we ignore
            # them entirely; we'll get those from metadata extraction during
            # actualy ingest anyway.
            exposure_id = int(match.group(1)) // 100
            exposure_id -= exposure_id % 2
            previous_path = result.setdefault(exposure_id, path.parent)
            if previous_path != path.parent:
                if (best_path := self._resolve_duplicates(previous_path, path.parent)) is not None:
                    result[exposure_id] = best_path
                else:
                    raise RuntimeError(f"Found multiple directory paths ({previous_path}, {path.parent}) "
                                       f"for exposure {exposure_id}.")
        return result

    def expand(self, tool: RepoAdminTool, exposure_id: int, found: Dict[int, Path]) -> Set[Path]:
        # Docstring inherited.
        base = found[exposure_id]
        result = set()
        for detector_num in self.DETECTOR_NUMS_FOR_FILENAMES:
            path = base.joinpath(f"HSCA{exposure_id*100 + detector_num:08d}.fits")
            if path.exists():
                result.add(path)
            elif not self._allow_incomplete:
                raise FileNotFoundError(f"Missing raw {path} for {exposure_id}.")
        return result


def reject_domeflat_duplicates(a: Path, b: Path) -> Optional[Path]:
    """Some HSC raw paths (at least at NCSA) have duplicates of some raws,
    with some appearing in a 'domeflat' subdirectory.  This function selects
    those that aren't in those subdirectories.
    """
    if "domeflat" in str(a):
        return b
    if "domeflat" in str(b):
        return a
    return None


def raw_ingest(subdir: str, top: Path = Path("/datasets/hsc/raw"), **kwargs) -> RawIngest:
    """Helper function to generate a `RawIngest` appropriate for finding
    HSC raws (recursively) in a directory.

    Parameters
    ----------
    subdir : `str`
        Subdirectory of ``top`` to search.  Also used as the operation name.
    top : `str`, optional
        Root directory for all raws for this instrument.
    **kwargs
        Additional keyword arguments forwarded to the exposure finder
        constructor.

    Returns
    -------
    ingest : `RawIngest`
        A raw ingest operation.
    """
    return RawIngest(
        f"HSC-raw-{subdir}",
        _ExposureFinder(top.joinpath(subdir), **kwargs),
        instrument_name="HSC",
    ).save_found()


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
            Group(
                "HSC-raw", (
                    raw_ingest("commissioning"),
                    raw_ingest("cosmos"),
                    raw_ingest("newhorizons", resolve_duplicates=reject_domeflat_duplicates),
                    raw_ingest("ssp_extra"),
                    raw_ingest("ssp_pdr1"),
                    raw_ingest("ssp_pdr2"),
                    raw_ingest("sxds-i2"),
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
