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

__all__ = ("admin_create",)

import logging

import click
from lsst.daf.butler.cli.utils import ButlerCommand

from .._tool import RepoAdminTool


@click.command(cls=ButlerCommand, short_help="Admin interface for creating shared data repositories.")
@click.argument("repo", type=str)
@click.option("--date", type=str, envvar="LSST_REPO_ADMIN_DATE")
@click.option("--site", type=str, envvar="LSST_REPO_ADMIN_SITE")
@click.option("--verbose", type=bool, default=False)
@click.option("--log-file", type=str, default="./gen3-repo-admin.log")
def admin_create(repo, date, site, verbose, log_file):
    log = logging.getLogger("gen3-repo-admin")
    tool = RepoAdminTool.from_strings(repo, date=date, site=site, log=log)
    # tool.create()
    log.info("Repository root is %s.", tool.root)
