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

__all__ = (
    "HyperSuprimeCamExposureFinder",
    "ingest_raws",
    "IngestBrightObjectMasks",
    "define_rc2_tags",
    "WriteStrayLightData",
)

from collections import defaultdict
import logging
import os
from pathlib import Path
import textwrap
from typing import Any, Dict, Iterator, Optional, Set, Tuple, TYPE_CHECKING

from .._operation import AdminOperation, OperationNotReadyError, SimpleStatus
from .. import ingest
from .. import calibs
from .. import common

if TYPE_CHECKING:
    import re
    from lsst.daf.butler import DataCoordinate
    from lsst.skymap import BaseSkyMap
    from .._tool import RepoAdminTool


def ingest_raws(
    name: str,
    path: Path,
    *,
    save_found: bool = False,
    split_into: Optional[int] = None,
    transfer: Optional[str] = "direct",
    **kwargs: Any,
) -> Iterator[AdminOperation]:
    """Generate one or more operations that ingest HSC raws.

    This is an HSC-specific variant of `ingest.ingest_raws` provided for
    convenience.  It always uses the `HyperSuprimeCamExposureFinder` (taking a
    `Path` argument instead of the ``finder`` argument of
    `ingest.ingest_raws`), never creates
    `~lsst.daf.butler.CollectionType.TAGGED` collections (for HSC, these are
    created from explicit lists of exposures and patterns for good detectors;
    see e.g. `define_rc2_tags`).

    Parameters
    ----------
    name : `str`
        Name of the primary ingest operation; related operations will have
        names derived from this by adding suffixes.
    path : `Path`
        Path to search (recursively) for raws.
    instrument_name: `str`
        Name of the instrument.
    save_found : `bool`, optional
        If `True`, save the exposures found by ``finder`` to a file in a
        helper operation, allowing any filesystem scanning to only happen once.
    split_into : `int`, optional
        If not `None`, split the ingest operation into this many (roughly)
        evenly-sized steps, each of which can be run in parallel.
    transfer : `str` or `None`, optional
        Transfer mode for ingest.
    **kwargs
        Additional keyword arguments are forwarded to
        `HyperSuprimeCamExposureFinder`.

    Returns
    -------
    operations : `Iterator` [ `AdminOperation` ]
        Operations that ingest HSC raws.
    """
    finder = HyperSuprimeCamExposureFinder(path, **kwargs)
    yield from ingest.ingest_raws(name, finder, instrument_name="HSC", save_found=save_found,
                                  split_into=split_into, transfer=transfer)


class HyperSuprimeCamExposureFinder(ingest.UnstructuredExposureFinder):
    """An `ExposureFinder` implementation for HSC data.

    This finder expects many exposures to be present in directories, and makes
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
    **kwargs
        Forwarded to `UnstructuredExposureFinder`.
    """

    def __init__(self, root: Path, *, allow_incomplete: bool = False, **kwargs: Any):
        super().__init__(root, self.FILE_REGEX, **kwargs)
        self._root = root
        self._allow_incomplete = allow_incomplete

    FILE_REGEX = r"HSCA(\d{8}).fits"

    DETECTOR_NUMS_FOR_FILENAMES = (
        list(range(0, 49)) + list(range(51, 58)) + list(range(100, 149)) + list(range(151, 158))
    )
    """HSC internal detector IDs used in filenames.
    """

    def extract_exposure_id(self, tool: RepoAdminTool, match: re.Match) -> int:
        # Docstring inherited.
        # HSC visit/exposure IDs are always even-numbered, to allow for
        # more than 100 CCDs while otherwise using the same pattern as the
        # old Supreme-Cam.  The CCD identifiers here aren't the
        # pure-integer ones we prefer to use in the pipelines, so we ignore
        # them entirely; we'll get those from metadata extraction during
        # actualy ingest anyway.
        exposure_id = int(match.group(1)) // 100
        exposure_id -= exposure_id % 2
        return exposure_id

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


class WriteStrayLightData(calibs.CalibrationOperation):
    """A concrete `AdminOperation` that copies HSC's special y-band stray light
    data file from a Gen2 repo.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    labels : `tuple` [ `str` ]
        Tuple of strings to include in the collection name(s).
    directory : `Path`
        Directory that directly contains stray-light data files.
    """

    def __init__(self, name: str, labels: Tuple[str, ...], directory: Path):
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
                    directory=str(self.directory),
                    transfer="direct",
                    labels=self.labels,
                )


class IngestBrightObjectMasks(AdminOperation):
    """A concrete `AdminOperation` that ingests HSC's BrightObjectMasks.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    root : `Path`
        Directory that directly contains subdirectories that correspond to
        tracts.
    collection : `str`
        ``RUN`` collection to ingest into.
    skymap_name : `str`
        Dimension-system name of the skymap that defines the tracts and patches
        the masks are defined on.
    """

    def __init__(self, name: str, root: Path, collection: str, skymap_name: str = "hsc_rings_v1"):
        super().__init__(name)
        self._root = root
        self._collection = collection
        self._skymap_name = skymap_name

    FILE_REGEX_TEMPLATE = r"BrightObjectMask-{tract_id}-(?P<x>\d+),(?P<y>\d+)-(?P<filter>[\w\-]+)\.reg"

    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        # Docstring inherited.
        SimpleStatus.check(self, tool).print_status(self, tool, indent)

    def run(self, tool: RepoAdminTool) -> None:
        # Docstring inherited.
        from lsst.daf.butler import CollectionType, DatasetRef, DatasetType, FileDataset
        logging.getLogger("daf.butler.Registry.insertDatasets").setLevel(logging.WARNING)
        dataset_type = DatasetType("brightObjectMask", dimensions=("tract", "patch", "band"),
                                   storageClass="ObjectMaskCatalog",
                                   universe=tool.butler.registry.dimensions)
        n_tracts, found_iter = self.find(tool)
        if not tool.dry_run:
            with SimpleStatus.run_context(self, tool):
                tool.butler.registry.registerCollection(self._collection, CollectionType.RUN)
                tool.butler.registry.registerDatasetType(dataset_type)
                for tract_data in tool.progress.wrap(found_iter, total=n_tracts, desc="Ingesting by tract"):
                    fds = tuple(
                        FileDataset(
                            path=str(path),
                            refs=[DatasetRef(dataset_type, data_id) for data_id in data_ids],
                        )
                        for path, data_ids in tract_data.items()
                    )
                    tool.butler.ingest(*fds, transfer="direct", run=self._collection)
        else:
            n_files = 0
            n_data_ids = 0
            for tract_data in found_iter:
                n_files += len(tract_data)
                for data_ids in tract_data.values():
                    n_data_ids += len(data_ids)
            print(f"Found {n_files} distinct files for {n_data_ids} data IDs, over {n_tracts} tracts.")

    def find(self, tool: RepoAdminTool) -> Tuple[int, Iterator[Dict[Path, Set[DataCoordinate]]]]:
        """Scan the filesystem for BrightObjectMask datasets to ingest.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        n_tracts : `int`
            Number of tracts found.
        data_ids_by_path : `Iterator` [ `dict` [ `Path`, `DataCoordinate` ] ]
            Iterator over dictionaries that map file paths to their data IDs,
            with one tract per dictionary.
        """
        from lsst.daf.butler import DataCoordinate
        tracts = {}
        for entry in tool.progress.wrap(os.scandir(self._root), "Scanning for tracts"):
            if entry.is_dir(follow_symlinks=False):
                tracts[int(entry.name)] = entry.path
        if not tracts:
            raise RuntimeError("No tract directories found in {self._root)")
        skyMap = self.loadSkyMap(tool)
        filters = {r.name: r.band for r in tool.butler.registry.queryDimensionRecords("physical_filter",
                                                                                      instrument="HSC")}

        def iter() -> Iterator[Dict[Path, DataCoordinate]]:
            for tract_id, tract_root in tracts.items():
                n_patches_x, _ = skyMap[tract_id].getNumPatches()
                file_regex = self.FILE_REGEX_TEMPLATE.format(tract_id=tract_id)
                found = defaultdict(set)
                for path, match in ingest.ExposureFinder.recursive_regex(tool, tract_root, file_regex,
                                                                         follow_symlinks=True):
                    band = filters[match.group("filter")]
                    patch_id = int(match.group("y"))*n_patches_x + int(match.group("x"))
                    data_id = DataCoordinate.standardize(skymap=self._skymap_name, tract=tract_id,
                                                         patch=patch_id, band=band,
                                                         universe=tool.butler.registry.dimensions)
                    found[path].add(data_id)
                yield found
        return len(tracts), iter()

    def loadSkyMap(self, tool: RepoAdminTool) -> BaseSkyMap:
        """Load the skymap instance that defines the mask's tracts and patches.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        skyMap : `BaseSkyMap`
            SkyMap object.
        """
        from lsst.daf.butler.registry import MissingCollectionError
        from lsst.skymap import BaseSkyMap
        try:
            skyMap = tool.butler.get(BaseSkyMap.SKYMAP_DATASET_TYPE_NAME, skymap=self._skymap_name,
                                     collections=[BaseSkyMap.SKYMAP_RUN_COLLECTION_NAME])
        except MissingCollectionError as err:
            raise OperationNotReadyError("No skymap collection found") from err
        if skyMap is None:
            raise OperationNotReadyError(f"No skymap {self._skymap_name}")
        return skyMap

    def cleanup(self, tool: RepoAdminTool) -> None:
        # Docstring inherited
        if not tool.dry_run:
            tool.butler.removeRuns([self._collection], unstore=False)
            SimpleStatus.cleanup(self, tool)


@common.Group.wrap("HSC-tags-RC2")
def define_rc2_tags() -> Iterator[AdminOperation]:
    """Generate operations that define ``TAGGED`` collections for the HSC RC2
    subset.

    Returns
    -------
    operations : `Iterator` [ `AdminOperation` ]
        Operations that tag the HSC RC2 subset.
    """
    for tract_id, tract_name in [
        (9615, "GAMA 15H (Wide)"),
        (9697, "VVDS (Wide)"),
        (9813, "COSMOS (UltraDeep)")
    ]:
        yield common.DefineTag(
            f"HSC-tags-RC2-raw-{tract_id}",
            f"HSC/raw/RC2/{tract_id}",
            [
                (("raw",), dict(instrument="HSC", collections=["HSC/raw/all"], exposure=v,
                                where="detector != 9 AND detector.purpose='SCIENCE'"))
                for v in RC2_VISITS[tract_id]
            ],
            doc=textwrap.fill(
                "Raws included in the Release Candidate 2 medium-scale test dataset "
                f"(see DM-11345), overlapping tract {tract_id} in {tract_name}."
            ),
        )
    yield common.DefineChain(
        "HSC-tags-RC2-raw",
        "HSC/raw/RC2",
        tuple(f"HSC/raw/RC2/{tract_id}" for tract_id in RC2_VISITS.keys()),
        doc=textwrap.fill(
            "Raws included in the Release Candidate 2 medium-scale test dataset (see DM-11345)."
        )
    )


RC2_VISITS = {
    9615: [
        26024, 26028, 26032, 26036, 26044, 26046, 26048, 26050, 26058,
        26060, 26062, 26070, 26072, 26074, 26080, 26084, 26094,
        23864, 23868, 23872, 23876, 23884, 23886, 23888, 23890, 23898,
        23900, 23902, 23910, 23912, 23914, 23920, 23924, 28976,
        1258, 1262, 1270, 1274, 1278, 1280, 1282, 1286, 1288, 1290, 1294,
        1300, 1302, 1306, 1308, 1310, 1314, 1316, 1324, 1326, 1330, 24494,
        24504, 24522, 24536, 24538,
        23212, 23216, 23224, 23226, 23228, 23232, 23234, 23242, 23250,
        23256, 23258, 27090, 27094, 27106, 27108, 27116, 27118, 27120,
        27126, 27128, 27130, 27134, 27136, 27146, 27148, 27156,
        380, 384, 388, 404, 408, 424, 426, 436, 440, 442, 446, 452, 456,
        458, 462, 464, 468, 470, 472, 474, 478, 27032, 27034, 27042,
        27066, 27068,
    ],
    9697: [
        6320, 34338, 34342, 34362, 34366, 34382, 34384, 34400, 34402,
        34412, 34414, 34422, 34424, 34448, 34450, 34464, 34468, 34478,
        34480, 34482, 34484, 34486,
        7138, 34640, 34644, 34648, 34652, 34664, 34670, 34672, 34674,
        34676, 34686, 34688, 34690, 34698, 34706, 34708, 34712, 34714,
        34734, 34758, 34760, 34772,
        35870, 35890, 35892, 35906, 35936, 35950, 35974, 36114, 36118,
        36140, 36144, 36148, 36158, 36160, 36170, 36172, 36180, 36182,
        36190, 36192, 36202, 36204, 36212, 36214, 36216, 36218, 36234,
        36236, 36238, 36240, 36258, 36260, 36262,
        36404, 36408, 36412, 36416, 36424, 36426, 36428, 36430, 36432,
        36434, 36438, 36442, 36444, 36446, 36448, 36456, 36458, 36460,
        36466, 36474, 36476, 36480, 36488, 36490, 36492, 36494, 36498,
        36504, 36506, 36508, 38938, 38944, 38950,
        34874, 34942, 34944, 34946, 36726, 36730, 36738, 36750, 36754,
        36756, 36758, 36762, 36768, 36772, 36774, 36776, 36778, 36788,
        36790, 36792, 36794, 36800, 36802, 36808, 36810, 36812, 36818,
        36820, 36828, 36830, 36834, 36836, 36838,
    ],
    9813: [
        11690, 11692, 11694, 11696, 11698, 11700, 11702, 11704, 11706,
        11708, 11710, 11712, 29324, 29326, 29336, 29340, 29350,
        1202, 1204, 1206, 1208, 1210, 1212, 1214, 1216, 1218, 1220, 23692,
        23694, 23704, 23706, 23716, 23718,
        1228, 1230, 1232, 1238, 1240, 1242, 1244, 1246, 1248, 19658,
        19660, 19662, 19680, 19682, 19684, 19694, 19696, 19698, 19708,
        19710, 19712, 30482, 30484, 30486, 30488, 30490, 30492, 30494,
        30496, 30498, 30500, 30502, 30504,
        1166, 1168, 1170, 1172, 1174, 1176, 1178, 1180, 1182, 1184, 1186,
        1188, 1190, 1192, 1194, 17900, 17902, 17904, 17906, 17908, 17926,
        17928, 17930, 17932, 17934, 17944, 17946, 17948, 17950, 17952,
        17962,
        318, 322, 324, 326, 328, 330, 332, 344, 346, 348, 350, 352, 354,
        356, 358, 360, 362, 1868, 1870, 1872, 1874, 1876, 1880, 1882,
        11718, 11720, 11722, 11724, 11726, 11728, 11730, 11732, 11734,
        11736, 11738, 11740, 22602, 22604, 22606, 22608, 22626, 22628,
        22630, 22632, 22642, 22644, 22646, 22648, 22658, 22660, 22662,
        22664,
        23038, 23040, 23042, 23044, 23046, 23048, 23050, 23052, 23054, 23056,
        23594, 23596, 23598, 23600, 23602, 23604, 23606, 24298, 24300, 24302,
        24304, 24306, 24308, 24310, 25810, 25812, 25814, 25816,
    ],
}
