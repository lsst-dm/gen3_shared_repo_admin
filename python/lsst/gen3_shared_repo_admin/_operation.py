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

__all__ = ("AdminOperation", "IncompleteOperationError", "OperationNotReadyError", "SimpleStatus")

from abc import ABC, abstractmethod
from contextlib import contextmanager
import enum
from pathlib import Path
from typing import Generator, Iterator, TYPE_CHECKING


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

    def cleanup(self, tool: RepoAdminTool) -> None:
        """Attempt to revert an aborted or incomplete `run` invocation.

        Parameters
        ----------
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        """
        raise NotImplementedError(f"{self.name} does not support cleanup.")


class SimpleStatus(enum.Enum):
    """An enumeration-based helper class for operations that do not have
    a natural way to track their status.

    `SimpleStatus` assumes that the status of an operation can be fully
    described by its enumeration values, and provides methods to both check
    that status and report it by manipulating zero-size files.

    Operation classes must use `print_status` (or at least `check`) and
    `run_context` together.
    """
    NOT_STARTED = "not started"
    IN_PROGRESS = "in progress"
    INTERRUPTED = "interrupted"
    DONE = "done"

    @classmethod
    def check(cls, op: AdminOperation, tool: RepoAdminTool) -> SimpleStatus:
        """Check the current status of the operation, returning it as an
        enumeration value.

        Parameters
        ----------
        op : `AdminOperation`
            Operation whose status should be checked.
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        status : `SimpleStatus`
            Enumeration value indicating current status.
        """
        for status in (cls.DONE, cls.IN_PROGRESS, cls.INTERRUPTED):
            if status.filename(op, tool).exists():
                return status
        return cls.NOT_STARTED

    def print_status(self, op: AdminOperation, tool: RepoAdminTool, indent: int) -> None:
        """Print this status value to STDOUT in a way that can be used to
        implement `AdminOperation.print_status`.

        Parameters
        ----------
        op : `AdminOperation`
            Operation whose status should be checked.
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        indent : `str`
            Number of spaces to indent reporting.
        """
        print(f"{' '*indent}{op.name}: {self.value}")

    def filename(self, op: AdminOperation, tool: RepoAdminTool) -> Path:
        """Return the file name associated with this status value.

        Parameters
        ----------
        op : `AdminOperation`
            Operation whose status should be checked.
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        filename : `Path`
            Path object representing the status file.
        """
        return Path(f"{tool.work_dir}/{op.name}.{self.value.replace(' ', '_')}")

    @classmethod
    @contextmanager
    def run_context(cls, op: AdminOperation, tool: RepoAdminTool) -> Generator[None, None, None]:
        """Return a context manager that manipulates status files in a way
        consistent with `check`.

        All writes that could fail in an `AdminOperation.run` operation should
        be wrapped in this context.

        Parameters
        ----------
        op : `AdminOperation`
            Operation whose status should be checked.
        tool : `RepoAdminTool`
            Object managing shared state for all operations.

        Returns
        -------
        context : `ContextManager`
            Context manager with no interface of its own.
        """
        in_progress = cls.IN_PROGRESS.filename(op, tool)
        in_progress.touch(exist_ok=True)
        try:
            yield
        except BaseException:
            in_progress.replace(cls.INTERRUPTED.filename(op, tool))
            raise
        else:
            in_progress.replace(cls.DONE.filename(op, tool))

    @classmethod
    def cleanup(cls, op: AdminOperation, tool: RepoAdminTool) -> None:
        """Delete all status files.

        Parameters
        ----------
        op : `AdminOperation`
            Operation whose status should be checked.
        tool : `RepoAdminTool`
            Object managing shared state for all operations.
        """
        for status in cls:
            status.filename(op, tool).unlink(missing_ok=True)
