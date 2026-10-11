"""Per-task proto outputs: each output's type name, filename, and the stage-1 step that writes it."""

from typing import NamedTuple

from rocks_analysis_pipeline.stage1_state import (
    STEP_BANDS_PROTO_DONE,
    STEP_DMTRACKS_PROTO_DONE,
    STEP_EVENTS_PROTO_DONE,
    STEP_POINTS_PROTO_DONE,
    STEPS,
)


class ProtoOutput(NamedTuple):
    name: str
    filename: str
    step: str


PROTO_OUTPUTS = (
    ProtoOutput("bands", "bands.pb.zst", STEP_BANDS_PROTO_DONE),
    ProtoOutput("dmtracks", "dmtracks.pb.zst", STEP_DMTRACKS_PROTO_DONE),
    ProtoOutput("events", "events.pb.zst", STEP_EVENTS_PROTO_DONE),
    ProtoOutput("points", "points.pb.zst", STEP_POINTS_PROTO_DONE),
)

OUTPUT_BY_NAME = {output.name: output for output in PROTO_OUTPUTS}
ARCHIVE_TYPES = tuple(OUTPUT_BY_NAME)


def partition_types(stop_after: str | None) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Splits the output types by whether their step runs at or before stop_after.

    Args:
      stop_after: the last stage-1 step to run, or None for all of them.

    Returns:
      The (ready, withheld) type names, each in ARCHIVE_TYPES order.
    """
    if stop_after is None:
        return ARCHIVE_TYPES, ()
    if stop_after not in STEPS:
        raise ValueError(f"unknown step {stop_after!r}; expected one of {STEPS}")
    last = STEPS.index(stop_after)
    ready = tuple(output.name for output in PROTO_OUTPUTS if STEPS.index(output.step) <= last)
    return ready, tuple(name for name in ARCHIVE_TYPES if name not in ready)
