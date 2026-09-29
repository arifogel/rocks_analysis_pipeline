"""
Validates that Katydid can actually be found via Bazel runfiles (see
runfiles_resolve.resolve_executable(), used by stage1_steps.resolve_katydid_path()) --
this is meant to be cheap and fast (`bazel test`), specifically to catch a wrong or stale
KATYDID_RLOCATION (e.g. after a canonical repo name change) before committing to a real,
potentially long-running Katydid invocation.

Deliberately doesn't invoke the resolved binary itself, unlike
stage1_steps_specsims_resolution_test.py's specsims check: Katydid is a full analysis run,
not a lightweight CLI with a cheap, side-effect-free no-args usage message to check
against.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from rocks_analysis_pipeline.stage1_steps import resolve_katydid_path


class ResolveKatydidPathTest(unittest.TestCase):
    def test_resolves_to_an_existing_executable_file(self) -> None:
        path = resolve_katydid_path()
        self.assertTrue(Path(path).is_file(), f"resolved katydid path {path!r} is not a file")


if __name__ == "__main__":
    unittest.main()
