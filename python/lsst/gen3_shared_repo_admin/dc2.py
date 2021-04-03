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

__all__ = ("raw_operations", "refcat_operations", "umbrella_operations", "calib_operations",
           "rerun_operations_DP0")

import itertools
import os
from pathlib import Path
import textwrap
from typing import Dict, Iterator, Set, TYPE_CHECKING

from .calibs import ConvertCalibrations, WriteCuratedCalibrations
from .common import DefineChain, Group
from .ingest import DefineRawTag, ExposureFinder, RawIngest, UnstructuredExposureFinder
from .refcats import RefCatIngest
from .reruns import ConvertRerun
from .visits import DefineVisits
from . import doc_templates

if TYPE_CHECKING:
    import re
    from ._operation import AdminOperation
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
        Root path to search; expected to directly contain subdirectories
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


class _NaiveExposureFinder(UnstructuredExposureFinder):
    """An `ExposureFinder` implementation for DC2 data that isn't organized
    into per-exposure directories, but also isn't very big so we don't have
    to care about scanning those directories multiple times.

    Parameters
    ----------
    root : `str`
        Root path to search for matching files.
    """

    def __init__(self, root: str, *, has_band_suffix: bool, allow_incomplete: bool = False):
        super().__init__(
            Path(root),
            self.FILE_REGEX_BAND_SUFFIX if has_band_suffix else self.FILE_REGEX_NO_BAND_SUFFIX,
        )
        self._allow_incomplete = allow_incomplete
        self._has_band_suffix = has_band_suffix

    FILE_REGEX_BAND_SUFFIX = r"lsst_a_(\d{7})_R\d{2}_S\d{2}_[ugrizy].fits"

    FILE_REGEX_NO_BAND_SUFFIX = r"lsst_a_(\d{7})_R\d{2}_S\d{2}.fits"

    DETECTOR_NAMES = set(
        f"{r}_{s}" for r, s in itertools.product(
            (
                f"R{i}{j}" for i, j in itertools.product(range(5), range(5))
                if (i, j) not in set(itertools.product([0, 4], [0, 4]))
            ),
            (
                f"S{i}{j}" for i, j in itertools.product(range(3), range(3))
            ),
        )
    )

    def extract_exposure_id(self, tool: RepoAdminTool, match: re.Match) -> int:
        # Docstring inherited.
        return int(match.group(1))

    def expand(self, tool: RepoAdminTool, exposure_id: int, found: Dict[int, Path]) -> Set[Path]:
        # Docstring inherited.
        base = found[exposure_id]
        result = set()
        band = None
        for detector_name in self.DETECTOR_NAMES:
            if not self._has_band_suffix:
                path = base.joinpath(f"lsst_a_{exposure_id}_{detector_name}.fits")
                if path.exists():
                    result.add(path)
                elif not self._allow_incomplete:
                    raise FileNotFoundError(f"Missing raw with detector={detector_name} for {exposure_id}.")
            elif band is None:
                for trial_band in "ugrizy":
                    path = base.joinpath(f"lsst_a_{exposure_id}_{detector_name}_{trial_band}.fits")
                    if path.exists():
                        band = trial_band
                        result.add(path)
                        break
                else:
                    raise FileNotFoundError(f"Missing raw with detector={detector_name} for {exposure_id}.")
            else:
                path = base.joinpath(f"lsst_a_{exposure_id}_{detector_name}_{band}.fits")
                if path.exists():
                    result.add(path)
                elif not self._allow_incomplete:
                    raise FileNotFoundError(f"Missing raw with detector={detector_name} (assuming band "
                                            f"{band}) for {exposure_id}.")
        return result


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
    raw_calibs_flats_finder = _NaiveExposureFinder(
        "/project/czw/dataDirs/DC2_raw_calibs/calibration_data",
        has_band_suffix=True,
    )
    raw_calibs_others_finder = _NaiveExposureFinder(
        "/project/czw/dataDirs/DC2_raw_calibs/calibration_data",
        has_band_suffix=False,
    )
    raw_calibs_bf_finder = _NaiveExposureFinder(
        "/project/czw/dataDirs/DC2_raw_calibs/bf_flats_20190408",
        has_band_suffix=True,
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
                        doc="Raw images from DR6 WFD designated for use in Data Preview 0.",
                    ),
                    DefineRawTag(
                        "2.2i-raw-tag-med",
                        monthly_finder,
                        instrument_name="LSSTCam-imSim",
                        input_collection="2.2i/raw/all",
                        output_collection="2.2i/raw/test-med-1",
                        doc=textwrap.fill(
                            "Raw images used as inputs for DM's medium-scale regular test processing. "
                            "This includes two tracts of y1-wfd data, tracts 3828 and 3829 and partial y2. "
                            "See DM-22954 for more information."
                        ),
                    ),
                    Group(
                        "2.2i-raw-calibs", (
                            RawIngest(
                                "2.2i.raw-calibs-flats",
                                raw_calibs_flats_finder,
                                instrument_name="LSSTCam-imSim",
                                collection="2.2i/raw/all",
                                transfer="copy",
                            ),
                            RawIngest(
                                "2.2i.raw-calibs-others",
                                raw_calibs_others_finder,
                                instrument_name="LSSTCam-imSim",
                                collection="2.2i/raw/all",
                                transfer="copy",
                            ),
                            RawIngest(
                                "2.2i.raw-calibs-bf",
                                raw_calibs_bf_finder,
                                instrument_name="LSSTCam-imSim",
                                collection="2.2i/raw/all",
                                transfer="copy",
                            ),
                        ),
                    )
                ),
            ),
        )
    )


def umbrella_operations() -> Group:
    """Helper function that returns all umbrella-collection defintion admin
    operations for the DC2 data repository.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    return Group(
        "2.2i-defaults",
        (
            DefineChain(
                "2.2i-defaults-alll",
                "2.2i/defaults", (
                    "2.2i/raw/all",
                    "2.2i/calib",
                    "skymaps",
                    "refcats",
                ),
                doc=doc_templates.UMBRELLA.format(tail="all available DC2 run2.2i data."),
            ),
            DefineChain(
                "2.2i-defaults-DR6",
                "2.2i/defaults/DP0", (
                    "2.2i/raw/DP0",
                    "2.2i/calib",
                    "skymaps",
                    "refcats",
                ),
                doc=doc_templates.UMBRELLA.format(
                    tail="the DC2 DR6 WFD subset designated for Data Preview 0."
                ),
            ),
            DefineChain(
                "2.2i-defaults-monthly",
                "2.2i/defaults/test-med-1", (
                    "2.2i/raw/test-med-1",
                    "2.2i/calib",
                    "skymaps",
                    "refcats",
                ),
                doc=doc_templates.UMBRELLA.format(
                    tail="the DC2 subset used for DM's medium-scale regular test processing."
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
                collection="refcats/PREOPS-301",
            ),
            DefineChain(
                "refcats-chain",
                "refcats",
                ("refcats/PREOPS-301",),
                doc="Umbrella collection for all active reference catalogs.",
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
    root = "/datasets/DC2/DR6/Run2.2i/patched/2021-02-10"
    return Group(
        "2.2i-calibs", (
            WriteCuratedCalibrations("2.2i-calibs-curated", "LSSTCam-imSim", labels=("PREOPS-301",),
                                     collection_prefix="2.2i"),
            ConvertCalibrations(
                name="2.2i-calibs-convert",
                instrument_name="LSSTCam-imSim",
                labels=("gen2",),
                root=root,
                repo_path=os.path.join(root, "CALIB"),
                collection_prefix="2.2i",
            ),
            DefineChain(
                "2.2i-calibs-default",
                "2.2i/calib", (
                    "2.2i/calib/PREOPS-301",
                    "2.2i/calib/gen2",
                    "2.2i/calib/PREOPS-301/unbounded",
                ),
                doc=doc_templates.DEFAULT_CALIBS.format(instrument="Run2.2i"),
            )
        )
    )


def rerun_operations_DP0() -> Group:
    """Helper function that returns the admin operations that convert the
    DESC DC2 processing reruns that will be used for DP0.1.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    # Coaddition and coadd-processing repos are small enough to be converted
    # in one go.
    full_repo_steps = {
        "-coadd-wfd-dr6-v1-grizy": "coadd/wfd/dr6/v1/grizy",
        "-coadd-wfd-dr6-v1-u": "coadd/wfd/dr6/v1/u",
        "-coadd-wfd-dr6-v1": "coadd/wfd/dr6/v1",
    }
    reruns = [
        ConvertRerun(
            f"2.2i-rerun-DP0-{v.replace('/', '-')}",
            instrument_name="LSSTCam-imSim",
            root="/datasets/DC2/DR6/Run2.2i/patched/2021-02-10",
            repo_path=f"rerun/run2.2i{k}",
            run_name=f"2.2i/runs/DP0.1/{v}",
            include=("*",),
            exclude=(),
        )
        for k, v in full_repo_steps.items()
    ]
    # The calexp repo is so big we convert it in a few steps, splitting up the
    # dataset types.
    # This first bunch got ingested via an earlier operation definition that
    # didn't split them up.
    done_first = ("calexp", "calexpBackground", "icSrc", "icSrc_schema", "src_schema")
    reruns.append(
        ConvertRerun(
            "2.2i-rerun-DP0-calexp-0",
            instrument_name="LSSTCam-imSim",
            root="/datasets/DC2/DR6/Run2.2i/patched/2021-02-10",
            repo_path="rerun/run2.2i-calexp-v1",
            run_name="2.2i/runs/DP0.1/calexp/v1",
            include=done_first,
            exclude=(),
        )
    )
    # Make each other regular dataset its own step
    full_dataset_types = ("src", "skyCorr", "srcMatch")
    reruns.extend(
        ConvertRerun(
            f"2.2i-rerun-DP0-calexp-{dataset_type}",
            instrument_name="LSSTCam-imSim",
            root="/datasets/DC2/DR6/Run2.2i/patched/2021-02-10",
            repo_path="rerun/run2.2i-calexp-v1",
            run_name="2.2i/runs/DP0.1/calexp/v1",
            include=(dataset_type,),
            exclude=(),
        )
        for dataset_type in full_dataset_types
    )
    # Add one more step for miscellaneous things, like config datasets.
    reruns.append(
        ConvertRerun(
            "2.2i-rerun-DP0-calexp-misc",
            instrument_name="LSSTCam-imSim",
            root="/datasets/DC2/DR6/Run2.2i/patched/2021-02-10",
            repo_path="rerun/run2.2i-calexp-v1",
            run_name="2.2i/runs/DP0.1/calexp/v1",
            include=("*",),
            exclude=done_first + full_dataset_types,
        )
    )
    inputs = ("2.2i/raw/DP0", "2.2i/calib", "refcats", "skymaps")
    return Group(
        "2.2i-rerun-DP0",
        (
            tuple(reruns)
            + (
                DefineChain(
                    "2.2i-rerun-DP0-chain",
                    "2.2i/runs/DP0.1",
                    tuple(
                        f"2.2i/runs/DP0.1/{v}"
                        for v in (list(reversed(full_repo_steps.values())) + ["calexp/v1"])
                    ) + inputs,
                    doc=textwrap.fill(
                        "Parent collection for all DESC DC2 DR6 WFD "
                        "processing converted from Gen2 for Data Preview 0.1."
                    ),
                    flatten=True,
                ),
            )
        )
    )


def generate() -> Iterator[AdminOperation]:
    yield from raw_operations().flatten()
    yield from refcat_operations().flatten()
    yield from calib_operations().flatten()
    yield DefineVisits("2.2i-visits", "LSSTCam-imSim", collections=("2.2i/raw/all",))
    yield from umbrella_operations().flatten()
    yield from rerun_operations_DP0().flatten()
