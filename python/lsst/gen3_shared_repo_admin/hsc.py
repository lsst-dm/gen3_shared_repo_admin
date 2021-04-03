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

__all__ = ("generate",)

from collections import defaultdict
import logging
import os
from pathlib import Path
import textwrap
from typing import Any, Dict, Iterator, Optional, Set, Tuple, TYPE_CHECKING

from ._operation import AdminOperation, OperationNotReadyError, SimpleStatus
from .ingest import RawIngest, ExposureFinder, UnstructuredExposureFinder
from .calibs import CalibrationOperation, ConvertCalibrations, WriteCuratedCalibrations
from .common import Group, RegisterInstrument, DefineChain, DefineTag, IngestFiles
from .visits import DefineVisits
from .reruns import ConvertRerun
from . import doc_templates

if TYPE_CHECKING:
    import re
    from lsst.daf.butler import DataCoordinate
    from lsst.skymap import BaseSkyMap
    from ._tool import RepoAdminTool


class _ExposureFinder(UnstructuredExposureFinder):
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


class IngestBrightObjectMasks(AdminOperation):
    """A concrete `AdminOperation` that ingests HSC's BrightObjectMasks.

    Parameters
    ----------
    name : `str`
        Name of the operation.  Should include any parent-operation prefixes
        (see `AdminOperation` documentation).
    root : `str`
        Directory that directly contains subdirectories that correspond to
        tracts.
    collection : `str`
        ``RUN`` collection to ingest into.
    skymap_name : `str`
        Dimension-system name of the skymap that defines the tracts and patches
        the masks are defined on.
    """

    def __init__(self, name: str, root: str, collection: str, skymap_name: str = "hsc_rings_v1"):
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
                for path, match in ExposureFinder.recursive_regex(tool, tract_root, file_regex,
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
    ],
}


def rc2_tags() -> Group:
    """Helper function that returns operations that tag the raws for the RC2
    datasets.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    per_tract = tuple(
        DefineTag(
            f"HSC-tags-RC2-raw-{tract_id}",
            f"HSC/raw/RC2/{tract_id}",
            [
                (("raw",), dict(instrument="HSC", collections=["HSC/raw/all"], exposure=v))
                for v in RC2_VISITS[tract_id]
            ],
            doc=(
                "Raws included in the Release Candidate 2 medium-scale test dataset "
                f"(see DM-11345), overlapping tract {tract_id} in {tract_name}."
            ),
        )
        for tract_id, tract_name in [
            (9615, "GAMA 15H (Wide)"), (9697, "VVDS (Wide)"), (9813, "COSMOS (UltraDeep)")
        ]
    )
    all_tracts = DefineChain(
        "HSC-tags-RC2-raw",
        "HSC/raw/RC2",
        tuple(f"HSC/raw/RC2/{tract_id}" for tract_id in RC2_VISITS.keys()),
        doc="Raws included in the Release Candidate 2 medium-scale test dataset (see DM-11345)."
    )
    return Group(
        "HSC-tags-RC2", per_tract + (all_tracts,)
    )


def rc2_rerun(weekly: str, ticket: str, steps: Dict[str, str]) -> Group:
    """Helper function that returns operations that convert a single logical
    Gen2 RC2 reprocessing rerun, assuming its child reruns form a consistent
    pattern.

    Parameters
    ----------
    weekly : `str`
        Weekly tag string (e.g. ``w_2021_06``).
    ticket : `str`
        Ticket number for the processing run.
    steps : `dict`
        Dictionary mapping child rerun suffixes (e.g. ``"-sfm"`` or ``""``)
        to the names used for Gen3 ``RUN`` collections and operation names.

    Returns
    -------
    group : `Group`
        A group of admin operations.
    """
    reruns = tuple(
        ConvertRerun(
            f"HSC-rerun-RC2-{weekly}-{v}",
            instrument_name="HSC",
            root="/datasets/hsc/repo",
            repo_path=f"rerun/RC/{weekly}/{ticket}{k}",
            run_name=f"HSC/runs/RC2/{weekly}/{ticket}/{v}",
            include=("*",),
            exclude=("*_metadata", "raw", "brightObjectMask", "ref_cat"),
        )
        for k, v in steps.items()
    )
    chain = DefineChain(
        f"HSC-rerun-RC2-{weekly}-chain",
        f"HSC/runs/RC2/{weekly}/{ticket}",
        (
            tuple(f"HSC/runs/RC2/{weekly}/{ticket}/{v}" for v in reversed(steps.values()))
            + ("HSC/raw/RC2", "HSC/calib", "HSC/masks", "skymaps", "refcats")
        ),
        doc=textwrap.fill(
            f"HSC RC2 processing with weekly {weekly} on ticket {ticket}, "
            "(converted from Gen2 repo at /datasets/hsc/repo).",
        ),
        flatten=True,
    )
    return Group(f"HSC-rerun-RC2-{weekly}", reruns + (chain,))


def rc2_fgcmcal_lut() -> AdminOperation:
    return Group(
        "HSC-fgcmlut-RC2", (
            IngestFiles(
                "HSC-fgcmlut-RC2-ingest",
                collection="HSC/fgcmcal/lut/RC2/DM-28636",
                dataset_type_name="fgcmLookUpTable",
                dimensions={"instrument"},
                storage_class="Catalog",
                datasets={
                    Path("/project/erykoff/rc2_gen3/fgcmlut/fgcm-process/fgcmLookUpTable.fits"):
                        {"instrument": "HSC"}
                },
                transfer="copy",
            ),
            DefineChain(
                "HSC-fgcmlut-RC2-chain",
                "HSC/fgcmcal/lut/RC2",
                ("HSC/fgcmcal/lut/RC2/DM-28636",),
                doc="Default lookup table for FGCM over the RC2 dataset.",
            ),
        )
    )


def generate() -> Iterator[AdminOperation]:
    """Helper function that yields all HSC-specific operations.

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
                    DefineChain(
                        "HSC-calibs-default",
                        "HSC/calib", (
                            "HSC/calib/gen2/20180117",
                            "HSC/calib/DM-28636",
                            "HSC/calib/gen2/20180117/unbounded",
                            "HSC/calib/DM-28636/unbounded",
                        ),
                        doc=doc_templates.DEFAULT_CALIBS.format(instrument="HSC"),
                    ),
                    DefineChain(
                        "HSC-calibs-default-unbounded",
                        "HSC/calib/unbounded", (
                            "HSC/calib/gen2/20180117/unbounded",
                            "HSC/calib/DM-28636/unbounded",
                        ),
                        doc=doc_templates.DEFAULT_CALIBS_UNBOUNDED.format(instrument="HSC"),
                    )
                ),
            ),
            DefineVisits("HSC-visits", "HSC"),
            Group(
                "HSC-masks", (
                    IngestBrightObjectMasks(
                        "HSC-masks-s18a",
                        "/datasets/hsc/BrightObjectMasks/GouldingMasksS18A",
                        collection="HSC/masks/s18a",
                    ),
                    IngestBrightObjectMasks(
                        "HSC-masks-arcturus",
                        "/datasets/hsc/BrightObjectMasks/ArcturusMasks",
                        collection="HSC/masks/arcturus",
                    ),
                    DefineChain(
                        "HSC-masks-default",
                        "HSC/masks",
                        (
                            "HSC/masks/s18a",
                        ),
                        doc="Recommended version of the HSC bright object masks.",
                    ),
                )
            ),
            DefineChain(
                "HSC-defaults",
                "HSC/defaults", (
                    "HSC/raw/all", "HSC/calib", "HSC/masks", "refcats", "skymaps",
                ),
                doc=doc_templates.UMBRELLA.format(tail="all available HSC data.")
            ),
            rc2_fgcmcal_lut(),
            rc2_tags(),
            DefineChain(
                "HSC-RC2-defaults",
                "HSC/RC2/defaults", (
                    "HSC/raw/RC2", "HSC/calib", "HSC/masks", "HSC/fgcmcal/lut/RC2", "refcats", "skymaps",
                ),
                doc=doc_templates.UMBRELLA.format(tail="the HSC RC2 test dataset.")
            ),
            Group(
                "HSC-rerun",
                (
                    rc2_rerun("w_2021_06", "DM-28654", {"-sfm": "sfm", "": "rest"}),
                    rc2_rerun("w_2021_02", "DM-28282", {"-sfm": "sfm", "": "rest"}),
                    rc2_rerun("w_2020_50", "DM-28140", {"-sfm": "sfm", "": "rest"}),
                    rc2_rerun("w_2020_42", "DM-27244", {"-sfm": "sfm", "": "rest"}),
                )
            ),
        )
    ).flatten()
