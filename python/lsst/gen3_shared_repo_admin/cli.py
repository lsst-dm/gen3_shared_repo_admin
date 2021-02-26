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

__all__ = ("main",)

import sys
import logging

import click

from lsst.daf.butler.cli.cliLog import CliLog
from ._tool import RepoAdminTool


@click.group("butler-admin", short_help="Administrative interface for major shared data repositories.")
@click.option("--repo", type=str)
@click.option("--date", type=str, envvar="LSST_REPO_ADMIN_DATE")
@click.option("--site", type=str, envvar="LSST_REPO_ADMIN_SITE")
@click.option("--verbose", type=bool, default=False)
@click.option("--log-file", type=str, default="./gen3-repo-admin.log")
@click.pass_context
def cli(ctx: click.Context, repo: str, date: str, site: str, verbose: bool, log_file: str):
    CliLog.initLog(longlog=True)
    log = logging.getLogger("gen3-repo-admin")
    ctx.obj = RepoAdminTool.from_strings(repo, date=date, site=site, log=log)


@cli.command(short_help="Create a new data repository.")
@click.pass_obj
def create(tool: RepoAdminTool):
    # tool.create()
    tool.log.info("Repository root is %s.", tool.root)


def main():
    return sys.exit(cli())
