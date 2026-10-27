"""JSON normalization. Everything journaled goes through a JSON round-trip."""

from __future__ import annotations

import json
import traceback
from typing import Any


def normalize(value: Any) -> Any:
    """Round-trip `value` through JSON.

    Live execution returns the normalized value too (not the original object),
    so a tuple becomes a list both live and on replay. Without this, workflow
    code could behave differently on the first run vs. a recovery.
    """
    try:
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError) as e:
        raise TypeError(f"value must be JSON-serializable: {e}") from e


def error_info(exc: BaseException) -> dict[str, Any]:
    return {
        "type": type(exc).__qualname__,
        "message": str(exc),
        "traceback": "".join(traceback.format_exception(exc))[-8000:],
    }
