"""Layout and reader of a run's stage-2 tar archives.

A run directory holds, per output type, either one `<type>.tar` or one
`<type>_<job id start>-<job id end>.tar` per chunk. Every tar member is one task's file, named
`subrun_<id>/field_<index>/<filename>`. The tars are uncompressed, so walking their headers
locates every member without reading any member data.
"""

import logging
import re
import tarfile
from collections.abc import Mapping, Set
from dataclasses import dataclass
from pathlib import Path

from rocks_analysis_pipeline.proto_outputs import ProtoOutput

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class JobIds:
    """An inclusive job id range."""

    start: int
    end: int


def archive_filename(output: ProtoOutput, *, job_ids: JobIds | None) -> str:
    """Names the archive for an output type.

    Args:
      output: the output type.
      job_ids: the job ids the archive covers, or None for the whole run.

    Returns:
      <name>.tar, or <name>_<job id start>-<job id end>.tar for a job id range.
    """
    if job_ids is None:
        return f"{output}.tar"
    return f"{output}_{job_ids.start}-{job_ids.end}.tar"


def _chunk_archive_pattern(output: ProtoOutput) -> re.Pattern[str]:
    return re.compile(rf"{re.escape(str(output))}_\d+-\d+\.tar")


_MEMBER_NAME = re.compile(r"subrun_(\d+)/field_(\d+)/(.+)")


@dataclass(frozen=True, kw_only=True, order=True)
class TaskKey:
    subrun_id: int
    field_index: int


@dataclass(frozen=True, kw_only=True)
class MemberLocation:
    """Where a tar member's data lives: `size` bytes at `offset` in `tar_path`."""

    tar_path: Path
    offset: int
    size: int

    def read(self) -> bytes:
        """Reads the member's bytes. Raises OSError if the tar ends early."""
        with self.tar_path.open("rb") as f:
            f.seek(self.offset)
            data = f.read(self.size)
        if len(data) != self.size:
            raise OSError(f"{self.tar_path}: expected {self.size} bytes at offset {self.offset}, got {len(data)}")
        return data


def find_archives(run_dir: Path, output: ProtoOutput) -> list[Path]:
    """Returns the tars holding `output`: the whole-run tar, or every chunk's tar.

    Args:
      run_dir: the run directory.
      output: the output type.

    Returns:
      The tars. Raises FileNotFoundError when there are none and ValueError when both forms exist.
    """
    whole = run_dir / archive_filename(output, job_ids=None)
    pattern = _chunk_archive_pattern(output)
    chunks = sorted(p for p in run_dir.iterdir() if pattern.fullmatch(p.name))
    if whole.is_file() and chunks:
        raise ValueError(f"{run_dir} holds both {whole.name} and chunk tars for {output}; refusing to double-count")
    if whole.is_file():
        return [whole]
    if not chunks:
        raise FileNotFoundError(
            f"{run_dir} holds no {output} tars (expected {whole.name} or {output}_<start>-<end>.tar)"
        )
    return chunks


def _index_archive(tar_path: Path, output: ProtoOutput) -> dict[TaskKey, MemberLocation]:
    index: dict[TaskKey, MemberLocation] = {}
    with tarfile.open(tar_path, "r:") as tar:
        for info in tar:
            if not info.isfile():
                continue
            match = _MEMBER_NAME.fullmatch(info.name)
            if match is None or match[3] != output.filename:
                raise ValueError(f"{tar_path}: unexpected member {info.name!r} (expected .../{output.filename})")
            key = TaskKey(subrun_id=int(match[1]), field_index=int(match[2]))
            index[key] = MemberLocation(tar_path=tar_path, offset=info.offset_data, size=info.size)
    return index


class RunArchive:
    """The members of a run directory's tars for the requested outputs.

    Every requested output holds exactly the same set of tasks, and no task appears in more
    than one tar of an output.
    """

    def __init__(self, *, run_dir: Path, outputs: Set[ProtoOutput]) -> None:
        """
        Args:
          run_dir: the run directory.
          outputs: the output types to index. Raises ValueError if empty, if the outputs cover
            different tasks, or if a task appears in two tars of one output.
        """
        if not outputs:
            raise ValueError("outputs must not be empty")
        self.run_dir = run_dir
        members: dict[ProtoOutput, Mapping[TaskKey, MemberLocation]] = {}
        for output in (o for o in ProtoOutput if o in outputs):
            index: dict[TaskKey, MemberLocation] = {}
            for tar_path in find_archives(run_dir, output):
                for key, location in _index_archive(tar_path, output).items():
                    if key in index:
                        raise ValueError(f"{output} task {key} appears in both {index[key].tar_path} and {tar_path}")
                    index[key] = location
            members[output] = index
            logger.info("Indexed %d %s task(s) from %s", len(index), output, run_dir)
        self.members: Mapping[ProtoOutput, Mapping[TaskKey, MemberLocation]] = members
        self._check_same_tasks()

    def _check_same_tasks(self) -> None:
        reference_output, reference = next(iter(self.members.items()))
        for output, index in self.members.items():
            if index.keys() != reference.keys():
                only_ref = sorted(reference.keys() - index.keys())
                only_here = sorted(index.keys() - reference.keys())
                raise ValueError(
                    f"{reference_output} and {output} cover different tasks: "
                    f"{len(only_ref)} only in {reference_output} (e.g. {only_ref[:3]}), "
                    f"{len(only_here)} only in {output} (e.g. {only_here[:3]})"
                )

    @property
    def tasks(self) -> list[TaskKey]:
        """Every task, sorted."""
        return sorted(next(iter(self.members.values())))

    def tasks_for_field(self, field_index: int) -> list[TaskKey]:
        """The tasks of one field, sorted by subrun."""
        return [key for key in self.tasks if key.field_index == field_index]

    @property
    def field_indices(self) -> list[int]:
        """Every field index, sorted."""
        return sorted({key.field_index for key in self.tasks})
