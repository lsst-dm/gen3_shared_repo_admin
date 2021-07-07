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

"""Definitions for ``/repo/dc2`` at NCSA.

This repository includes all DESC DC2 data at NCSA.
"""

from __future__ import annotations

__all__ = ()

from pathlib import Path
import textwrap
from typing import Iterator, TYPE_CHECKING

from ..._repo_definition import RepoDefinition
from ... import common
from ... import calibs
from ... import check
from ... import doc_templates
from ... import reruns
from ... import visits
from ...instruments.dc2 import (
    ImSimExposureFinder,
    ingest_raws,
    ingest_refcat,
    UnstructuredImSimExposureFinder,
)
from ._site import NCSA

if TYPE_CHECKING:
    from ._operation import AdminOperation


def repos() -> Iterator[RepoDefinition]:
    """Generate the definitions for the `/repo/dc2` data repository at NCSA.
    """
    yield RepoDefinition(name="dc2", date="20210215", site=NCSA, operations=operations)


def operations() -> Iterator[AdminOperation]:
    """Generate all operations used to set up the `/repo/dc2` data repository
    at NCSA.
    """
    yield common.CreateRepo()
    yield common.RegisterSkyMap("DC2")
    yield common.RegisterInstrument("imSim-registration", "lsst.obs.lsst.LsstCamImSim")
    yield common.RegisterInstrument("phoSim-registration", "lsst.obs.lsst.LsstCamPhoSim")
    yield from raw_operations()
    yield from ingest_refcat(
        ticket="PREOPS-301",
        path=Path("/datasets/DC2/DR6/Run2.2i/patched/2021-02-10/ref_cats/cal_ref_cat"),
    )
    yield from calib_operations()
    yield visits.DefineVisits("2.2i-visits", "LSSTCam-imSim", collections=("2.2i/raw/all",))
    yield visits.PatchExistingVisits("2.2i-visits-patch", "LSSTCam-imSim")
    yield from umbrella_operations()
    yield from dp0_rerun_operations()
    yield from med1_rerun_operations()
    yield check.CheckURIs(
        "check-URIs",
        # visits are arbitrary, but cover all bands and all overlap this
        # (also-arbitrary) tract.
        [{"instrument": "LSSTCam-imSim", "visit": v, "exposure": v,
          "skymap": "DC2", "tract": 4644, "detector": 90}
         for v in (760247, 944265, 896824, 471974, 971097, 190279)]
    )


@common.Group.wrap("2.2i-raw")
def raw_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to ingest DC2 raws.
    """
    # The DR6 WFD raw dataset used for DP0.1; save this large list of exposures
    # to a file (as an operation of its own), then split it up into 4
    # (arbitrary) sets we can ingest in parallel.
    yield from ingest_raws(
        "2.2i-raw-DR6",
        ImSimExposureFinder(
            Path("/datasets/DC2/DR6/Run2.2i/patched/2021-02-10/raw"),
            "*-R??-S??-det???-???.fits",
        ).saved_as("2.2i-raw-DR6-find"),
        split_into=4,
        save_found=True,
        tag_as="2.2i/raw/DP0",
        tag_doc="Raw images from DR6 WFD designated for use in Data Preview 0.",
    )
    # The raws used in DM's ~monthly test processing.  These are a subset of
    # the DP0.1 raws, but we want to create a separate tag for them, and
    # defining the ingest operation anyway is what tells (when we check status)
    # that it's already done even before we run it.
    yield from ingest_raws(
        "2.2i-raw-monthly",
        ImSimExposureFinder(
            Path("/datasets/DC2/repoRun2.2i/raw"),
            "*-R??-S??-det???.fits",
        ),
        save_found=True,
        tag_as="2.2i/raw/test-med-1",
        tag_doc=(
            "Raw images used as inputs for DM's medium-scale regular test processing. "
            "This includes two tracts of y1-wfd data, tracts 3828 and 3829 and partial y2. "
            "See DM-22954 for more information."
        ),
    )
    # Raw calibrations from Jim Chiang, via Chris Waters.  These were not in a
    # permanent location at NCSA previously, so we ingest them with
    # transfer=copy so this data repo _becomes_ their permanent location.  They
    # also aren't nicely organized into per-exposure directories, so we have to
    # use a different ExposureFinder.
    yield from ingest_raws(
        "2.2i-raw-calibs-others",
        UnstructuredImSimExposureFinder(
            Path("/project/czw/dataDirs/DC2_raw_calibs/calibration_data"),
            has_band_suffix=False,
        ),
        transfer="copy",
    )
    yield from ingest_raws(
        "2.2i-raw-calibs-bf-flats",
        UnstructuredImSimExposureFinder(
            Path("/project/czw/dataDirs/DC2_raw_calibs/bf_flats_20190408"),
            has_band_suffix=True,
        ),
        transfer="copy",
    )
    # Raws missing from the original DP0 transfer for unknown reasons, plus all
    # of those in Run2.2i that we hadn't intended to transfer, moved to NCSA
    # from NERSC by Jim Chiang.  This also includes missing raws from the
    # test-med-1 subset, which were present at NCSA before but were not
    # ingested as part of the 2.2i-raw-monthly operation because at least one
    # detector from each exposure was part of the initial DP0 transfer, and
    # that was enough for our deduplication to block the missing ones from
    # ingesting earlier.  We ingest those from the path below rather than the
    # the other /datasets/DC2/raw/Run2.2i to limit the number of raw URI
    # roots/patterns we have in the database.
    yield from ingest_raws(
        "2.2i-raw-missing",
        UnstructuredImSimExposureFinder(
            Path("/datasets/DC2/raw/Run2.2i/dp0-missing"),
            has_band_suffix=True,
            allow_incomplete=True,
        ),
        save_found=True,
        extend_ingested_exposures=True,
    )


@common.Group.wrap("2.2i-calibs")
def calib_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to ingest/convert DC2 master calibrations.
    """
    root = Path("/datasets/DC2/DR6/Run2.2i/patched/2021-02-10")
    yield calibs.WriteCuratedCalibrations(
        "2.2i-calibs-curated",
        "LSSTCam-imSim",
        labels=("PREOPS-301",),
        collection_prefix="2.2i",
    )
    yield calibs.WriteCuratedCalibrations(
        "2.2i-calibs-curated+bf",
        "LSSTCam-imSim",
        labels=("DM-30694",),
        collection_prefix="2.2i",
    )
    yield calibs.ConvertCalibrations(
        name="2.2i-calibs-convert",
        instrument_name="LSSTCam-imSim",
        labels=("gen2",),
        root=root,
        repo_path=root.joinpath("CALIB"),
        collection_prefix="2.2i",
        dataset_type_names=("flat", "bias", "dark", "fringe"),
    )
    yield calibs.ConvertCalibrations(
        name="2.2i-calibs-sky",
        instrument_name="LSSTCam-imSim",
        labels=("gen2",),
        root=root,
        repo_path=root.joinpath("CALIB"),
        collection_prefix="2.2i",
        dataset_type_names=("sky",),
        # obs_lsst master has different templates, and these names
        # didn't get patched with the rest of the DC2 DP0 patching.
        # But overriding here is better than patching anyway - it just
        # wasn't an option when everything else was converted.
        dataset_template_overrides={
            "sky": ("SKY/%(calibDate)s/%(filter)s/SKY-%(calibDate)s-%(filter)s"
                    "-%(raftName)s-%(detectorName)s-det%(detector)03d_%(calibDate)s.fits"),
        }
    )
    yield common.DefineChain(
        "2.2i-calibs-default",
        "2.2i/calib", (
            "2.2i/calib/DM-30694",
            "2.2i/calib/gen2",
            "2.2i/calib/DM-30694/unbounded",
        ),
        doc=doc_templates.DEFAULT_CALIBS.format(instrument="Run2.2i"),
    )


@common.Group.wrap("2.2i-defaults")
def umbrella_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to set up "umbrella" default collections
    for DC2.
    """
    yield common.DefineChain(
        "2.2i-defaults-all",
        "2.2i/defaults", (
            "2.2i/raw/all",
            "2.2i/calib",
            "skymaps",
            "refcats",
        ),
        doc=doc_templates.UMBRELLA.format(tail="all available DC2 run2.2i data."),
    )
    yield common.DefineChain(
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
    )
    yield common.DefineChain(
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
    )


@common.Group.wrap("2.2i-rerun-DP0")
def dp0_rerun_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to convert (to Gen3) the DESC-processed DR6
    WFD reruns that will be re-released as DP0.1.
    """
    # Coaddition and coadd-processing repos are small enough to be converted
    # in one go.
    full_repo_steps = {
        "-coadd-wfd-dr6-v1-grizy": "coadd/wfd/dr6/v1/grizy",
        "-coadd-wfd-dr6-v1-u": "coadd/wfd/dr6/v1/u",
        "-coadd-wfd-dr6-v1": "coadd/wfd/dr6/v1",
    }
    rerun_ops = [
        reruns.ConvertRerun(
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
    rerun_ops.append(
        reruns.ConvertRerun(
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
    rerun_ops.extend(
        reruns.ConvertRerun(
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
    rerun_ops.append(
        reruns.ConvertRerun(
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
    yield from rerun_ops
    yield common.DefineChain(
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
    )


@common.Group.wrap("2.2i-rerun-med1")
def med1_rerun_operations() -> Iterator[AdminOperation]:
    """Generate all operations used to convert (to Gen3) recent DM reprocessing
    of the test-med-1 subset.
    """
    for weekly, ticket in {
        "w_2021_24": "DM-30730",
        "w_2021_20": "DM-30297",
        "w_2021_16": "DM-29770",
        "w_2021_12": "DM-29427",
        "w_2021_04": "DM-28453",
    }.items():
        def generate() -> Iterator[AdminOperation]:
            chain = ["2.2i/defaults/test-med-1"]
            for suffix in ("sfm", "coadd", "multi"):
                yield reruns.ConvertRerun(
                    f"2.2i-rerun-RC2-{weekly}-{suffix}",
                    instrument_name="LSSTCam-imSim",
                    root="/datasets/DC2/repoRun2.2i",
                    repo_path=f"rerun/{weekly}/{ticket}/{suffix}",
                    run_name=f"2.2i/runs/test-med-1/{weekly}/{ticket}/{suffix}",
                    include=("*",),
                    exclude=("*_metadata", "raw", "ref_cat"),
                )
                chain.insert(0, f"2.2i/runs/test-med-1/{weekly}/{ticket}/{suffix}")
            yield common.DefineChain(
                f"2.2i-rerun-med1-{weekly}-chain",
                f"2.2i/runs/test-med-1/{weekly}/{ticket}",
                tuple(chain),
                doc=textwrap.fill(
                    f"DM reprocessing of the test-med-1 dataset with weekly {weekly} on ticket {ticket}, "
                    "(converted from Gen2 repo at /datasets/DC2/repoRun2.2i).",
                ),
                flatten=True,
            )
        yield common.Group(f"2.2i-rerun-med1-{weekly}", generate())
