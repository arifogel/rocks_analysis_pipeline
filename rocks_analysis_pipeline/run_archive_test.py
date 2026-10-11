"""Tests the stage-2 archive layout and reader."""

import tarfile
import tempfile
import unittest
from pathlib import Path

from rocks_analysis_pipeline.proto_outputs import ProtoOutput
from rocks_analysis_pipeline.run_archive import (
    JobIds,
    RunArchive,
    TaskKey,
    _chunk_archive_pattern,
    archive_filename,
    find_archives,
)
from rocks_analysis_pipeline.stage2_merge import ChunkScope, run_stage2_merge

_NUM_FIELDS = 2
_BANDS = frozenset({ProtoOutput.bands})


def _write_task_files(run_dir: Path, *, subruns: int) -> None:
    for subrun in range(subruns):
        for field_index in range(_NUM_FIELDS):
            task = run_dir / f"subrun_{subrun}" / f"field_{field_index}"
            task.mkdir(parents=True)
            for output in ProtoOutput:
                (task / output.filename).write_bytes(f"{output}-{subrun}-{field_index}".encode())


def _write_tar(path: Path, names: list[str]) -> None:
    with tarfile.open(path, "w") as tar:
        for name in names:
            info = tarfile.TarInfo(name)
            info.size = 0
            tar.addfile(info)


class ArchiveFilenameTest(unittest.TestCase):
    def test_whole_run(self) -> None:
        self.assertEqual(archive_filename(ProtoOutput.bands, job_ids=None), "bands.tar")

    def test_job_id_range(self) -> None:
        self.assertEqual(archive_filename(ProtoOutput.bands, job_ids=JobIds(start=4, end=7)), "bands_4-7.tar")

    def test_chunk_pattern_matches_every_chunk_filename(self) -> None:
        for output in ProtoOutput:
            name = archive_filename(output, job_ids=JobIds(start=0, end=99))
            self.assertTrue(_chunk_archive_pattern(output).fullmatch(name))
            self.assertIsNone(_chunk_archive_pattern(output).fullmatch(archive_filename(output, job_ids=None)))


class RunArchiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.run_dir = Path(self._tmp.name)

    def test_reads_what_stage2_merge_wrote_for_the_whole_run(self) -> None:
        _write_task_files(self.run_dir, subruns=3)
        run_stage2_merge(runs_dir=self.run_dir.parent, run_name=self.run_dir.name)
        archive = RunArchive(run_dir=self.run_dir, outputs=frozenset(ProtoOutput))
        self.assertEqual(len(archive.tasks), 3 * _NUM_FIELDS)
        self.assertEqual(archive.field_indices, [0, 1])
        for output in ProtoOutput:
            location = archive.members[output][TaskKey(subrun_id=2, field_index=1)]
            self.assertEqual(location.read(), f"{output}-2-1".encode())

    def test_reads_what_stage2_merge_wrote_for_chunks(self) -> None:
        _write_task_files(self.run_dir, subruns=2)
        for start, end in [(0, 1), (2, 3)]:
            chunk = ChunkScope(job_id_start=start, job_id_end=end, num_fields=_NUM_FIELDS)
            run_stage2_merge(runs_dir=self.run_dir.parent, run_name=self.run_dir.name, chunk=chunk)
        archive = RunArchive(run_dir=self.run_dir, outputs=frozenset(ProtoOutput))
        self.assertEqual(
            archive.tasks_for_field(1), [TaskKey(subrun_id=0, field_index=1), TaskKey(subrun_id=1, field_index=1)]
        )
        self.assertEqual(archive.members[ProtoOutput.points][TaskKey(subrun_id=1, field_index=0)].read(), b"points-1-0")

    def test_rejects_whole_and_chunk_tars_together(self) -> None:
        _write_tar(self.run_dir / "bands.tar", [])
        _write_tar(self.run_dir / "bands_0-1.tar", [])
        with self.assertRaisesRegex(ValueError, "both"):
            find_archives(self.run_dir, ProtoOutput.bands)

    def test_rejects_missing_output(self) -> None:
        with self.assertRaises(FileNotFoundError):
            RunArchive(run_dir=self.run_dir, outputs=_BANDS)

    def test_rejects_task_in_two_chunks(self) -> None:
        for name in ["bands_0-1.tar", "bands_2-3.tar"]:
            _write_tar(self.run_dir / name, ["subrun_0/field_0/bands.pb.zst"])
        with self.assertRaisesRegex(ValueError, "appears in both"):
            RunArchive(run_dir=self.run_dir, outputs=_BANDS)

    def test_rejects_unexpected_member_name(self) -> None:
        _write_tar(self.run_dir / "bands.tar", ["subrun_0/field_0/events.pb.zst"])
        with self.assertRaisesRegex(ValueError, "unexpected member"):
            RunArchive(run_dir=self.run_dir, outputs=_BANDS)

    def test_rejects_outputs_covering_different_tasks(self) -> None:
        _write_tar(self.run_dir / "bands.tar", ["subrun_0/field_0/bands.pb.zst"])
        _write_tar(self.run_dir / "events.tar", ["subrun_1/field_0/events.pb.zst"])
        with self.assertRaisesRegex(ValueError, "different tasks"):
            RunArchive(run_dir=self.run_dir, outputs=frozenset({ProtoOutput.bands, ProtoOutput.events}))

    def test_rejects_empty_outputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            RunArchive(run_dir=self.run_dir, outputs=frozenset())


if __name__ == "__main__":
    unittest.main()
