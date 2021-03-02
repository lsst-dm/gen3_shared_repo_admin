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

__all__ = ("RepoAdminTool",)

import logging
import os

from lsst.daf.butler import Butler, Progress

from ._dataclasses import RepoDefinition, SiteDefinition
from ._operation import OperationNotReadyError
from .definitions import REPOS, SITES


class RepoAdminTool:
    """A helper object that maintains shared state for all repository
    administration operations.

    A `RepoAdminTool` instance is constructed for each command-line tool
    invocation, and essentially provides the corresponding high-level Python
    interface.

    Parameters
    ----------
    repo : `RepoDefinition`
        Definition of the concrete repository.
    site : `SiteDefinition`
        Definition of the site where this instance of the repository lives.
    work_root : `str`
        Root directory for logs and status files.  Must be consistent between
        runs.
    dry_run : `bool`, optional
        If `True`, do not make any writes to the data repository (writes to
        ``work_root`` may still occur).
    jobs : `int`, optional
        Number of processes to use, when possible.  Defaults to 1.
    """
    def __init__(self, repo: RepoDefinition, site: SiteDefinition, work_root: str, dry_run: bool = False,
                 jobs: int = 1):
        self.repo = repo
        self.site = site
        self.operations = {}
        for parent_operation in self.repo.operations:
            self.operations.update((op.name, op) for op in parent_operation.flatten())
        self._butler = None
        self.dry_run = dry_run
        self.work_dir = os.path.join(work_root, f"{self.repo.name}_{self.repo.date}")
        os.makedirs(self.work_dir, exist_ok=True)
        self.progress = Progress("butler-admin")
        self.log = logging.getLogger(f"butler-admin")
        self.jobs = jobs

    @classmethod
    def from_strings(cls, repo: str, site: str, date: str, work_root: str, dry_run: bool = False,
                     jobs: int = 1) -> RepoAdminTool:
        """Construct a `RepoAdminTool` from the name, site, and date strings
        that identify the repo and the site.

        Parameters
        ----------
        repo : `str`
            Base name of the repository definition.
        site : `str`
            Name for the site definition.
        date : `str`
            Date for the repository definition, as an 8-char YYYYMMDD string.
        work_root : `str`
            Root directory for logs and status files.  Must be consistent
            between runs.
        dry_run : `bool`, optional
            If `True`, do not make any writes to the data repository (writes to
            ``work_root`` may still occur).
        jobs : `int`, optional
            Number of processes to use, when possible.  Defaults to 1.

        Returns
        -------
        tool : `RepoAdminTool`
            A new tool instance.
        """
        return cls(REPOS[repo][date], SITES[site], work_root=work_root, dry_run=dry_run, jobs=jobs)

    @property
    def root(self) -> str:
        """Absolute path or URI to the data repository root (`str`).
        """
        return self.site.repo_uri_template.format(repo=self.repo)

    @property
    def butler(self) -> Butler:
        """A butler client for the data repository (`lsst.daf.butler.Butler`).

        This is read-only if and only if ``self.dry-run`` is `True`.
        """
        if self._butler is None:
            try:
                self._butler = Butler(self.root, writeable=not self.dry_run)
            except FileNotFoundError:
                raise OperationNotReadyError("Repo has not yet been created.")
        return self._butler

    def status(self, name: str) -> None:
        """Print status for the named operation to stdout.

        Parameters
        ----------
        name : `str`
            Name of the operation.
        """
        if name is None:
            name = self.repo.name
        self.operations[name].print_status(self, indent=0)

    def prep(self, name: str) -> None:
        """Prepare the named operation, performing only steps that do not
        require modifying the data repository.

        Parameters
        ----------
        name : `str`
            Name of the operation.
        """
        if name is None:
            name = self.repo.name
        self.log.info("Preparing %s in %s.", name, self.root)
        self.operations[name].prep(self)

    def run(self, name: str) -> None:
        """Run the named operation.

        Parameters
        ----------
        name : `str`
            Name of the operation.

        Notes
        -----
        Whether an operation requires `prep` to be invoked before `run` depends
        on the operation, but should is always reported by `status`.
        """
        if name is None:
            name = self.repo.name
        self.log.info("Running %s in %s.", name, self.root)
        self.operations[name].run(self)
