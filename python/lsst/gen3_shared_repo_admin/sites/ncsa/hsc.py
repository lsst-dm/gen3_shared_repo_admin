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

"""Definitions for HSC data in the ``/repo/main`` data repository at NCSA.
"""

from __future__ import annotations

__all__ = ()

from pathlib import Path
import textwrap
from typing import Iterator, Optional

from ..._operation import AdminOperation
from ... import calibs
from ... import common
from ... import reruns
from ... import visits
from ... import doc_templates
from ...instruments.hsc import (
    IngestBrightObjectMasks,
    define_rc2_tags,
    ingest_raws,
    WriteStrayLightData,
)


@common.Group.wrap("HSC")
def operations() -> Iterator[AdminOperation]:
    """Generate all operations used to set up HSC data in the `/repo/main` data
    repository at NCSA.

    This does not include skymap registration or reference catalog ingest, as
    these are considered shared by all instruments in the data repository (even
    if some are primarily used for HSC processing).
    """
    yield common.RegisterInstrument("HSC-registration", "lsst.obs.subaru.HyperSuprimeCam")
    yield from raw_operations()
    yield from calib_operations()
    yield visits.DefineVisits("HSC-visits", "HSC")
    yield from mask_operations()
    yield common.DefineChain(
        "HSC-defaults",
        "HSC/defaults", (
            "HSC/raw/all", "HSC/calib", "HSC/masks", "refcats", "skymaps",
        ),
        doc=doc_templates.UMBRELLA.format(tail="all available HSC data.")
    )
    yield from ingest_rc2_fgcmcal_lut()
    yield from define_rc2_tags()
    yield common.DefineChain(
        "HSC-RC2-defaults",
        "HSC/RC2/defaults", (
            "HSC/raw/RC2", "HSC/calib", "HSC/masks", "HSC/fgcmcal/lut/RC2", "refcats", "skymaps",
        ),
        doc=doc_templates.UMBRELLA.format(tail="the HSC RC2 test dataset.")
    )
    yield from rerun_operations()


@common.Group.wrap("HSC-raw")
def raw_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to ingest raw HSC data in the `/repo/main`
    data repository at NCSA.
    """
    top = Path("/datasets/hsc/raw")
    for subdir in ("commissioning", "cosmos", "newhorizons", "ssp_extra", "ssp_pdr1", "ssp_pdr2", "sxds-i2"):
        yield from ingest_raws(
            f"HSC-raw-{subdir}",
            top.joinpath(subdir),
            save_found=True,
            resolve_duplicates=reject_domeflat_duplicates if subdir == "newhorizons" else None,
        )


def reject_domeflat_duplicates(a: Path, b: Path) -> Optional[Path]:
    """A duplicate-resolution function for use with
    `ingest.UnstructuredExposureFinder`.

    Some HSC raw paths (at least at NCSA) have duplicates of some raws,
    with some appearing in a 'domeflat' subdirectory.  This function selects
    those that aren't in those subdirectories.
    """
    if "domeflat" in str(a):
        return b
    if "domeflat" in str(b):
        return a
    return None


@common.Group.wrap("HSC-calibs")
def calib_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to ingest/convert HSC master calibrations
    in the `/repo/main` data repository at NCSA.
    """
    yield calibs.WriteCuratedCalibrations("HSC-calibs-curated", "HSC", labels=("DM-28636",))
    root_path = Path("/datasets/hsc/repo")
    top_path = Path("/datasets/hsc/calib")
    for subdir in ("20180117", "20200115"):
        yield common.Group(
            f"HSC-calibs-{subdir}", (
                calibs.ConvertCalibrations(
                    name=f"HSC-calibs-{subdir}-convert",
                    instrument_name="HSC",
                    labels=("gen2", subdir),
                    root=root_path,
                    repo_path=top_path.joinpath(subdir),
                ),
                WriteStrayLightData(
                    name=f"HSC-calibs-{subdir}-straylight",
                    labels=("gen2", subdir),
                    directory=top_path.joinpath(subdir, "STRAY_LIGHT"),
                ),
            )
        )
    yield common.DefineChain(
        "HSC-calibs-default",
        "HSC/calib", (
            "HSC/calib/gen2/20180117",
            "HSC/calib/DM-28636",
            "HSC/calib/gen2/20180117/unbounded",
            "HSC/calib/DM-28636/unbounded",
        ),
        doc=doc_templates.DEFAULT_CALIBS.format(instrument="HSC"),
    )
    yield common.DefineChain(
        "HSC-calibs-default-unbounded",
        "HSC/calib/unbounded", (
            "HSC/calib/gen2/20180117/unbounded",
            "HSC/calib/DM-28636/unbounded",
        ),
        doc=doc_templates.DEFAULT_CALIBS_UNBOUNDED.format(instrument="HSC"),
    )


@common.Group.wrap("HSC-masks")
def mask_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to ingest/convert HSC bright object masks
    in the `/repo/main` data repository at NCSA.
    """
    yield IngestBrightObjectMasks(
        "HSC-masks-s18a",
        Path("/datasets/hsc/BrightObjectMasks/GouldingMasksS18A"),
        collection="HSC/masks/s18a",
    )
    yield IngestBrightObjectMasks(
        "HSC-masks-arcturus",
        Path("/datasets/hsc/BrightObjectMasks/ArcturusMasks"),
        collection="HSC/masks/arcturus",
    )
    yield common.DefineChain(
        "HSC-masks-default",
        "HSC/masks",
        (
            "HSC/masks/s18a",
        ),
        doc="Recommended version of the HSC bright object masks.",
    )


@common.Group.wrap("HSC-rerun")
def rerun_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to convert HSC processing runs
    in the `/repo/main` data repository at NCSA.
    """
    for weekly, ticket in {
        "w_2021_14": "DM-29519",
        "w_2021_10": "DM-29074",
        "w_2021_06": "DM-28654",
        "w_2021_02": "DM-28282",
        "w_2020_50": "DM-28140",
        "w_2020_42": "DM-27244",
    }.items():
        def generate() -> Iterator[AdminOperation]:
            chain = ["HSC/raw/RC2", "HSC/calib", "HSC/masks", "skymaps", "refcats"]
            for old_suffix, new_suffix in {"-sfm": "sfm", "": "rest"}.items():
                yield reruns.ConvertRerun(
                    f"HSC-rerun-RC2-{weekly}-{new_suffix}",
                    instrument_name="HSC",
                    root="/datasets/hsc/repo",
                    repo_path=f"rerun/RC/{weekly}/{ticket}{old_suffix}",
                    run_name=f"HSC/runs/RC2/{weekly}/{ticket}/{new_suffix}",
                    include=("*",),
                    exclude=("*_metadata", "raw", "brightObjectMask", "ref_cat"),
                )
                chain.insert(0, f"HSC/runs/RC2/{weekly}/{ticket}/{new_suffix}")
            yield common.DefineChain(
                f"HSC-rerun-RC2-{weekly}-chain",
                f"HSC/runs/RC2/{weekly}/{ticket}",
                tuple(chain),
                doc=textwrap.fill(
                    f"HSC RC2 processing with weekly {weekly} on ticket {ticket}, "
                    "(converted from Gen2 repo at /datasets/hsc/repo).",
                ),
                flatten=True,
            )
        yield common.Group(f"HSC-rerun-RC2-{weekly}", generate())


@common.Group.wrap("HSC-fgcmlut-RC2")
def ingest_rc2_fgcmcal_lut() -> Iterator[AdminOperation]:
    """Generate all operations used to ingest the FGCM lookup table for HSC RC2
    in the `/repo/main` data repository at NCSA.
    """
    yield common.IngestFiles(
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
    )
    yield common.DefineChain(
        "HSC-fgcmlut-RC2-chain",
        "HSC/fgcmcal/lut/RC2",
        ("HSC/fgcmcal/lut/RC2/DM-28636",),
        doc="Default lookup table for FGCM over the RC2 dataset.",
    )
