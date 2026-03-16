"""TimesheetIQ backend package."""

from __future__ import annotations

import os
import platform


def _fast_machine() -> str:
    arch = os.environ.get("PROCESSOR_ARCHITEW6432") or os.environ.get("PROCESSOR_ARCHITECTURE")
    if arch:
        return arch
    return "x86_64"


# Pandas import may call platform.machine(), which can hang on some Windows setups
# when WMI is slow/unavailable. Force a cheap non-WMI architecture lookup first.
platform.machine = _fast_machine  # type: ignore[assignment]
