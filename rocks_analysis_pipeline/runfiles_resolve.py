"""resolve_executable(): resolves a runfiles path to a real, executable file."""

import os
from pathlib import Path

from python.runfiles import runfiles


def resolve_executable(rlocation: str) -> str:
    """Resolves rlocation to a real path via runfiles, and confirms it's an executable file.

    Args:
      rlocation: the runfiles path to resolve, e.g. "katydid+/release/katydid.sh".

    Returns:
      The resolved path.
    """
    r = runfiles.Create()
    path = r.Rlocation(rlocation)
    if path is None:
        raise RuntimeError(
            f"Runfiles has no mapping for '{rlocation}'. If the canonical repo name for the "
            f"underlying module has changed, update the rlocation string passed in."
        )
    if not Path(path).is_file():
        raise RuntimeError(f"Resolved path '{path}' does not exist.")
    if not os.access(path, os.X_OK):
        raise RuntimeError(f"Resolved path '{path}' is not executable.")
    return path
