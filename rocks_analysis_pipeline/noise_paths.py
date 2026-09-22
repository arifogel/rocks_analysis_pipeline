"""Noise-path resolution, factored out of stage1_steps.py into its own
module: resolving a noise_id only needs pandas and rocks_utility's own
he6cres_db_query, not the rest of stage1_steps_lib's dependency chain
(he6-cres-spec-sims, uproot, the api/v1 proto schema) -- kept separate so
a lightweight consumer (see print_noise_paths.py) doesn't have to pull in
any of that just to look up a couple of file paths.
"""

import logging
from pathlib import Path

import pandas as pd

from rocks_analysis_pipeline.rocks_utility import he6cres_db_query

logger = logging.getLogger(__name__)


def resolve_noise_paths_from_id(noise_id: int) -> list[str]:
    """Restores RunSpecSims.get_noise_fp's own SQL-based noise-path
    resolution (run_spec_sims.py, before commit ec2a84c54c3da51efe17c6b01cc06ed19542ca15
    removed it entirely), adapted from a method on that class to a
    standalone function here. Behavior matches the original exactly,
    including its own comments/reasoning:

    - Queries he6cres_runs.spec_files for noise_id, ordered by channel,
      taking the first 2 rows (channel 0 and 1) -- "just takes the first
      file in this run_id (assumption is it's a one file acq)".
    - Groups by file_in_acq and aggregates into one ordered-by-channel
      path list per acquisition, taking the first such group. The dummy
      true_field=0 column exists only because aggregate_paths's own
      aggregation (used elsewhere for real signal files, where true_field
      is meaningful) requires that column to exist -- meaningless for
      noise files, preserved as-is rather than reworked, matching this
      project's own decision to keep this restoration a faithful port,
      not a rewrite.
    - Translates the stored path (relative to /mnt) to wulf's own
      directory structure, and verifies each resolved file actually
      exists before returning -- exactly the check this project's own
      RuntimeError-on-noise-load-failure fix (see this repo's own commit
      history) is the second line of defense for, not a replacement for.
    """
    query_he6_db = """
                    SELECT f.run_id, f.file_path, f.file_in_acq, f.channel
                    FROM he6cres_runs.spec_files as f
                    WHERE f.run_id = {}
                    ORDER BY f.channel
                    LIMIT 2
                  """.format(
        noise_id
    )

    logger.info("resolving noise_id=%s via: %s", noise_id, " ".join(query_he6_db.split()))
    noise_file_df = he6cres_db_query(query_he6_db)

    def aggregate_paths(group):
        ordered_paths = group.sort_values(by="channel")["file_path"].apply(str).tolist()
        return pd.Series({"true_field": group["true_field"].iloc[0], "file_path": ordered_paths})

    # Make dummy true_field column to use agg function. this is dumb fix later
    noise_file_df["true_field"] = 0
    noise_file_df = noise_file_df.groupby("file_in_acq").apply(aggregate_paths).reset_index(drop=True)

    noise_file_path = noise_file_df["file_path"].iloc[0]

    # Convert to directory structure on wulf
    wulf_noise_paths = [Path("/data/raid2/eliza4/he6_cres/") / Path(old).relative_to("/mnt") for old in noise_file_path]

    for noise_file in wulf_noise_paths:
        if not noise_file.is_file():
            raise UserWarning(f"{noise_file} doesn't exist")

    return [str(wnp) for wnp in wulf_noise_paths]
