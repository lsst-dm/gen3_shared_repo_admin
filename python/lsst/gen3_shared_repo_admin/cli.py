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
import tqdm

import lsst.log
from lsst.daf.butler.core.progress import Progress, ProgressHandler

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


class ConsoleProgressHandler(ProgressHandler):
    """A `ProgressHandler` implementation that delegates to the `tqdm` standard
    console progress bar.
    """

    def get_progress_bar(self, iterable, desc, total, level):
        # Docstring inherited.
        return tqdm.tqdm(iterable, total=total, desc=desc, file=sys.stdout)


@click.command("butler-admin", short_help="Administrative interface for major shared data repositories.")
@click.argument("repo", type=str)
@click.argument("name", type=str)
@click.option("--date", type=str, envvar="LSST_BUTLER_ADMIN_DATE")
@click.option("--site", type=str, envvar="LSST_BUTLER_ADMIN_SITE")
@click.option("--verbose", type=bool, default=False)
@click.option("--work-root", type=click.Path(dir_okay=True, file_okay=False, exists=True, writable=True),
              envvar="LSST_BUTLER_ADMIN_WORK_ROOT")
@click.option("-n", "--dry-run", is_flag=True)
@click.option("-j", "--jobs", type=int, default=1)
@click.option("--prep", is_flag=True)
@click.option("--status", is_flag=True)
@click.option("--cleanup", is_flag=True)
def cli(repo: str, name: str, date: str, site: str, verbose: bool, work_root: str, dry_run: bool,
        jobs: int, prep: bool, status: bool, cleanup: bool):
    lsst.log.configure_prop(_LOG_PROP.format(log_file=f"{work_root}/{repo}_{date}.log"))
    python_logger = logging.getLogger()
    python_logger.setLevel(logging.INFO)
    python_logger.addHandler(lsst.log.LogHandler())
    Progress.set_handler(ConsoleProgressHandler())
    dry_run = dry_run or prep or status
    tool = RepoAdminTool.from_strings(repo, date=date, site=site, work_root=work_root, dry_run=dry_run,
                                      jobs=jobs)
    if status:
        assert not prep
        assert not cleanup
        tool.status(name)
    elif prep:
        assert not cleanup
        tool.prep(name)
    elif cleanup:
        tool.cleanup(name)
    else:
        tool.run(name)


def main():
    return sys.exit(cli())
