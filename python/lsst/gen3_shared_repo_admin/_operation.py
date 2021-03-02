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

__all__ = ("AdminOperation", "IncompleteOperationError", "OperationNotReadyError")

from abc import ABC, abstractmethod
from typing import Iterator, TYPE_CHECKING


if TYPE_CHECKING:
    from ._tool import RepoAdminTool


class IncompleteOperationError(RuntimeError):
    """Exception raised when an operation is in an incomplete and/or
    inconsistent state that requires manual intervention.
    """
    pass


class OperationNotReadyError(RuntimeError):
    """Exception raise when an operation cannot be invoked (even for status)
    because some previous step is incomplete.
    """
    pass


class AdminOperation(ABC):
    """Base class for operations involving in setting up or otherwise
    administering a data repository.

    Parameters
    ----------
    name : `str`
        Unique name that identifies this operation.  Operations nested in
        groups should by convention include their parent names, separated by
        dashes.
    """
    def __init__(self, name: str):
        self.name = name

    def flatten(self) -> Iterator[AdminOperation]:
        """Iterate over ``self`` and (then) any child operations.

        Yields
        ------
        op : `AdminOperation`
            Self or an operation nested within it.
        """
        yield self

    @abstractmethod
    def print_status(self, tool: RepoAdminTool, indent: int) -> None:
        """Report the qualitative status for this operation to STDOUT.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        indent : `str`
            Number of spaces to indent reporting.
        """
        raise NotImplementedError()

    def prep(self, tool: RepoAdminTool) -> None:
        """Scan existing files, databases, etc. to determine what this step
        needs to do next, writing a description to ``tool.work_dir``.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Notes
        -----
        This should never modify the output data repository, and may be invoked
        even in dry-run conditions (with ``tool.butler`` read-only).  It should
        generally do as much work as possible under these conditions.

        The default implementation of this method does nothing.
        """
        pass

    @abstractmethod
    def run(self, tool: RepoAdminTool) -> None:
        """Run this operation, modifying the data repository.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Notes
        -----
        Implementations are responsible for checking ``tool.dry_run`` and
        behaving accordingly, but will generally be prevented by writing to
        ``tool.butler`` if ``tool.dry_run`` is `True` as an extra precaution.
        """
        raise NotImplementedError()
