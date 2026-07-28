"""
Process memory diagnostics.

Inputs: Current process information from the operating system.
Outputs: Resident set size in bytes when the platform exposes it.
Assumptions:
  - Linux deployments expose current RSS through /proc/self/statm.
  - Other Unix platforms fall back to peak RSS from getrusage().
"""

import os
import resource
import sys


def current_rss_bytes() -> int | None:
    """Return current RSS on Linux, with a best-effort Unix fallback."""
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        pass

    try:
        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (OSError, ValueError):
        return None

    # macOS reports bytes; Linux and other common Unix variants report KiB.
    return int(peak_rss if sys.platform == "darwin" else peak_rss * 1024)
