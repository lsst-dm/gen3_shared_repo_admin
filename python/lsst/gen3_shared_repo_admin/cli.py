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

import logging
import sys

import click

import lsst.log

from ._tool import RepoAdminTool

# Custom logging setup here is copied liberally from daf_butler's cliLog; it
# doesn't look like delegating to that would give us the kind of control we
# want her.
#
# Instead of setting up logs to write to stderr, we write them to per-repo
# files in the user's current directory, and reserving direct console output
# for exceptions and progress meters.  (If it didn't look _super-XML-painful_
# to write just warning logs to the console, I'd have done that, too).
#
# One thing this does not carry over from cliLog is unsetting the log state for
# use in unit tests; these are admin scripts, and we're not going to bother
# trying to unit test the CLI for them.  But don't copy this elsewhere if you
# want to write tests against the CLI - go directly to daf_butler instead.

_LOG_PROP = """\
log4j.rootLogger=DEBUG, FA
log4j.appender.FA=FileAppender
log4j.appender.FA.file={log_file}
log4j.appender.FA.layout=PatternLayout
log4j.appender.FA.layout.ConversionPattern=%-5p %d{{yyyy-MM-ddTHH:mm:ss.SSSZ}} %c %m%n
"""


@click.group("butler-admin", short_help="Administrative interface for major shared data repositories.")
@click.option("--repo", type=str)
@click.option("--date", type=str, envvar="LSST_REPO_ADMIN_DATE")
@click.option("--site", type=str, envvar="LSST_REPO_ADMIN_SITE")
@click.option("--verbose", type=bool, default=False)
@click.option("--log-path", type=click.Path(dir_okay=True, file_okay=False, exists=True, writable=True),
              default=".")
@click.option("-n", "--dry-run", is_flag=True)
@click.pass_context
def cli(ctx: click.Context, repo: str, date: str, site: str, verbose: bool, log_path: str, dry_run: bool):
    lsst.log.configure_prop(_LOG_PROP.format(log_file=f"{log_path}/{repo}_{date}.log"))
    python_logger = logging.getLogger()
    python_logger.setLevel(logging.INFO)
    python_logger.addHandler(lsst.log.LogHandler())
    ctx.obj = RepoAdminTool.from_strings(repo, date=date, site=site, dry_run=dry_run)


@cli.command(short_help="Create a new data repository.")
@click.pass_obj
def create(tool: RepoAdminTool):
    tool.create()


@cli.command(short_help="Register skymaps.")
@click.option("--resume", is_flag=True)
@click.pass_obj
def register_skymaps(tool: RepoAdminTool, resume: bool):
    tool.register_skymaps(resume=resume)


def main():
    return sys.exit(cli())
