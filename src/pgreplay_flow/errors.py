"""Public exceptions and internal control-flow signals."""

from __future__ import annotations

import uuid
from typing import Any


class FlowError(Exception):
    """Base class for pgreplay-flow errors."""


class WorkflowNotFound(FlowError):
    """No workflow with this name is registered on the Flow."""


class RunNotFound(FlowError):
    """No run with this id exists."""


class NonDeterminismError(FlowError):
    """Workflow code diverged from the run's recorded journal."""


class StepFailed(FlowError):
    """A step exhausted its retries. Raised into workflow code, which may catch it.

    The original exception type is not preserved across replays, so workflow
    code always sees StepFailed (live and on replay) to stay deterministic.
    """

    def __init__(self, step: str, error: dict[str, Any]):
        self.step = step
        self.error = error
        self.error_type: str = error.get("type", "Exception")
        self.error_message: str = error.get("message", "")
        super().__init__(f"step {step!r} failed: {self.error_type}: {self.error_message}")


class WorkflowFailed(FlowError):
    def __init__(self, run_id: uuid.UUID, error: dict[str, Any] | None):
        self.run_id = run_id
        self.error = error or {}
        super().__init__(
            f"run {run_id} failed: {self.error.get('type')}: {self.error.get('message')}"
        )


class WorkflowCancelled(FlowError):
    def __init__(self, run_id: uuid.UUID):
        self.run_id = run_id
        super().__init__(f"run {run_id} was cancelled")


class ChildFailed(StepFailed):
    """A child workflow awaited with ctx.child() failed or was cancelled."""

    def __init__(self, workflow: str, child_id: uuid.UUID, error: dict[str, Any]):
        self.child_id = child_id
        super().__init__(workflow, error)
        self.args = (f"child {workflow!r} ({child_id}) failed: "
                     f"{self.error_type}: {self.error_message}",)

    def __str__(self) -> str:
        return self.args[0]


# --- Internal control flow -------------------------------------------------
# These derive from BaseException so that `except Exception` in user workflow
# code cannot accidentally swallow them (same reasoning as CancelledError).


class _Interrupt(BaseException):
    pass


class LeaseLost(_Interrupt):
    """This worker no longer owns the run (lease expired, stolen, or run cancelled)."""


class StoreUnavailable(_Interrupt):
    """The database connection failed mid-run. The worker abandons the run;
    the lease expires and another worker recovers it."""


class JournalExhausted(_Interrupt):
    """Offline replay reached an operation that has no recorded outcome."""

    def __init__(self, key: str):
        self.key = key
        super().__init__(key)
