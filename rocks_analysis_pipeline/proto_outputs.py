"""Per-task proto outputs: each output's type name, filename, and the stage-1 step that writes it."""

import dataclasses

from rocks_analysis_pipeline.stage1_state import Step


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProtoOutput:
    name: str
    filename: str
    step: Step


PROTO_OUTPUTS = (
    ProtoOutput(name="bands", filename="bands.pb.zst", step=Step.bands_proto_done),
    ProtoOutput(name="dmtracks", filename="dmtracks.pb.zst", step=Step.dmtracks_proto_done),
    ProtoOutput(name="events", filename="events.pb.zst", step=Step.events_proto_done),
    ProtoOutput(name="points", filename="points.pb.zst", step=Step.points_proto_done),
)

OUTPUT_BY_NAME = {output.name: output for output in PROTO_OUTPUTS}
ARCHIVE_TYPES = tuple(OUTPUT_BY_NAME)
