"""Per-task proto outputs: each output's archive type, filename, and the stage-1 step that writes it."""

import dataclasses
import enum

from rocks_analysis_pipeline.stage1_state import Step


@dataclasses.dataclass(frozen=True, kw_only=True)
class _ProtoOutputData:
    filename: str
    step: Step


class ProtoOutput(enum.Enum):
    """The per-task proto outputs. A member's name is its archive type."""

    bands = _ProtoOutputData(filename="bands.pb.zst", step=Step.bands_proto_done)
    dmtracks = _ProtoOutputData(filename="dmtracks.pb.zst", step=Step.dmtracks_proto_done)
    events = _ProtoOutputData(filename="events.pb.zst", step=Step.events_proto_done)
    points = _ProtoOutputData(filename="points.pb.zst", step=Step.points_proto_done)

    @property
    def filename(self) -> str:
        return self.value.filename

    @property
    def step(self) -> Step:
        return self.value.step

    def __str__(self) -> str:
        return self.name
