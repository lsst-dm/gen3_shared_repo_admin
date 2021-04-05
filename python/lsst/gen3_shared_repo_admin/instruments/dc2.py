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

"""Operation definitions and utility code that is expected to be useful for
any data repository containing DESC DC2 data.
"""

from __future__ import annotations

__all__ = (
    "ImSimExposureFinder",
    "ingest_raws",
    "ingest_refcat",
    "UnstructuredImSimExposureFinder",
)

import functools
import itertools
import os
from pathlib import Path
from typing import Dict, Iterator, Set, TYPE_CHECKING

from ..common import DefineChain, Group
from .. import ingest
from ..refcats import RefCatIngest

if TYPE_CHECKING:
    import re
    from .._operation import AdminOperation
    from .._tool import RepoAdminTool


class ImSimExposureFinder(ingest.ExposureFinder):
    """An `ExposureFinder` implementation for DC2 data that assumes raws are
    organized into per-exposure directories.

    This finder does not check that all found exposures are complete.  Symbolic
    links are always followed.

    Parameters
    ----------
    root : `Path`
        Root path to search; expected to directly contain subdirectories
        that each map to exactly one exposure (with the exposure ID as the
        subdirectory name).
    file_pattern : `str`
        Glob pattern that raw files must match.
    """

    def __init__(self, root: Path, file_pattern: str):
        self._root = root
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


class UnstructuredImSimExposureFinder(ingest.UnstructuredExposureFinder):
    """An `ExposureFinder` implementation for DC2 data that isn't organized
    into per-exposure directories.

    Parameters
    ----------
    root : `Path`
        Root path to search for matching files.
    has_band_suffix : `bool`, optional
        If `True`, expect file to end with ``_{band}.fits`` instead of just
        ``.fits``, where ``{{band}}`` is e.g. ``r``.  DC2 raw flats seem to
        include the band while raw biases and darks do not.
    allow_incomplete : `bool`, optional
        If `True`, allow exposures to be ingested even if only some detectors
        are present.
    **kwargs
        Additional keyword arguments are forwarded to
        `ingest.UnstructuredExposureFinder`.
    """

    def __init__(self, root: Path, *, has_band_suffix: bool, allow_incomplete: bool = False):
        super().__init__(
            root,
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
    """Rxy_Sxy detector names for the 189 LSSTCam science sensors."""

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


ingest_raws = functools.partial(ingest.ingest_raws, instrument_name="LSSTCam-imSim",
                                collection="2.2i/raw/all")


@Group.wrap("refcats")
def ingest_refcat(ticket: str, path: Path) -> Iterator[AdminOperation]:
    """Generate operations that ingest the DC2 reference catalog.

    These operations assume they are generating the only reference catalog in
    the repository, and hence should define the global ``refcats`` ``CHAINED``
    collection pointer.

    Parameters
    ----------
    ticket : `str`
        Ticket name on which ingest was done; used as part of the collection
        name.
    path : `Path`
        Full path to the directory containing sharded reference catalog files.

    Returns
    -------
    operations : `Iterator` [ `AdminOperation` ]
        Operations that ingest reference catalogs.
    """
    yield RefCatIngest(
        "cal_ref_cat_2_2",
        path=path,
        collection=f"refcats/{ticket}",
    )
    yield DefineChain(
        "refcats-chain",
        "refcats",
        (f"refcats/{ticket}",),
        doc="Umbrella collection for all active reference catalogs.",
    )
