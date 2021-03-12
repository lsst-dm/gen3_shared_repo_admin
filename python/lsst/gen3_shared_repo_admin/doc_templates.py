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

___all__ = (
    "DEFAULT_CALIBS",
    "UMBRELLA",
)

import textwrap


class WrappedStringTemplate:

    def __init__(self, template: str):
        self._template = template

    def format(self, **kwargs: str) -> str:
        paragraphs = self._template.format(**kwargs).split("\n\n")
        return "\n\n".join(textwrap.fill(p) for p in paragraphs)


DEFAULT_CALIBS = WrappedStringTemplate("""\
Default calibration datasets for {instrument} processing.

This collection should be updated as needed to point to the current best
calibrations.  The search path should start with one or more `CALIBRATION`
collections that associate each dataset with a validity range (even if it is
infinite), followed by zero or more non-`CALIBRATION` collections holding only
unbounded calibrations - those whose validity ranges can be safely assumed by
pipeline code to be infinite (possibly because the actual validity lookup is
internal to the dataset.
""")

UMBRELLA = WrappedStringTemplate("""\
Convenience collection that points to the recommended versions of most standard
processing input collections, for {tail}.
""")
