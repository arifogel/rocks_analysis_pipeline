"""
Validates that ghcss's specsims binary can actually be found and invoked via Bazel
runfiles (see stage1_steps.py's resolve_specsims_path()), without running any real
simulation -- this is meant to be cheap and fast (`bazel test`), specifically to catch a
wrong or stale runfiles path (e.g. after a canonical repo name change, or a go_binary
target rename) before committing to a real, potentially long-running specsims run.
"""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

from rocks_analysis_pipeline.stage1_steps import resolve_specsims_path

# specsims has no dedicated --help output to check against. Instead, this runs specsims
# with no arguments at all: main.go prints this exact usage message to stderr and exits 2
# whenever --config is missing. Checking for this exact, distinctive text (and the exit
# code, which main.go's exitCodeProfilingStopped doc comment establishes as deliberately
# meaningful, not incidental) is what confirms the resolved path is actually specsims, not
# some other stale or unrelated executable that happens to exist there.
_EXPECTED_USAGE_TEXT = "usage: specsims --config"
_EXPECTED_EXIT_CODE = 2


class ResolveSpecsimsPathTest(unittest.TestCase):
    def test_resolves_to_an_existing_file(self) -> None:
        path = resolve_specsims_path()
        self.assertTrue(Path(path).is_file(), f"resolved specsims path {path!r} is not a file")

    def test_resolved_binary_is_actually_specsims(self) -> None:
        path = resolve_specsims_path()
        result = subprocess.run([path], capture_output=True, text=True, check=False)
        self.assertEqual(
            result.returncode,
            _EXPECTED_EXIT_CODE,
            f"specsims with no args exited {result.returncode}, expected "
            f"{_EXPECTED_EXIT_CODE}: stdout={result.stdout!r} stderr={result.stderr!r}",
        )
        self.assertIn(
            _EXPECTED_USAGE_TEXT,
            result.stderr,
            f"specsims stderr was missing the expected usage text: {result.stderr!r}",
        )


if __name__ == "__main__":
    unittest.main()
