"""Per-task proto outputs: each output's archive type, filename, and the stage-1 step that writes it."""

import enum

from rocks_analysis_pipeline.stage1_state import Step


class ProtoOutput(enum.Enum):
    """The per-task proto outputs. A member's name is its archive type."""

    bands = ("bands.pb.zst", Step.bands_proto_done)
    dmtracks = ("dmtracks.pb.zst", Step.dmtracks_proto_done)
    events = ("events.pb.zst", Step.events_proto_done)
    points = ("points.pb.zst", Step.points_proto_done)

    def __init__(self, filename: str, step: Step) -> None:
        self.filename = filename
        self.step = step

    def __str__(self) -> str:
        return self.name
