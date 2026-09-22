#!/usr/bin/env python3
"""
Config generation for spec-sims subruns, duplicated out of run_spec_sims.py /
he6_cres_spec_sims.experiment.Experiment without importing he6_cres_spec_sims
at all.

Why this file exists rather than editing run_spec_sims.py: that file (and the
he6_cres_spec_sims.experiment.Experiment class it calls into) also knows how
to *run* simulations (Experiment.run_sims -> Simulation.run_full()), which is
exactly the code path this project is moving off of in favor of ghcss. Rather
than carve that dead path out of run_spec_sims.py (or leave a he6_cres_spec_sims
import sitting unused at module load time -- he6_cres_spec_sims transitively
imports numpy/scipy, so importing it costs real startup time and requires it
to be installed even though nothing here calls into it), this file duplicates
only the config-generation logic that local_spec_sims.py's
generate_configs_only=True call path actually used:
Experiment.create_configs_for_experiment, Experiment.create_experiment_config_file,
and the get_experiment_dir/get_config_paths helpers -- verbatim in behavior,
translated 1:1 from he6_cres_spec_sims/experiment.py as it stood when this was
written. run_spec_sims.py itself is untouched; only local_spec_sims.py's
import of RunSpecSims changes, to RunSpecSimsGhcss from this file.

generate_configs_only is kept as a parameter on .run() (always True, since
this file never runs simulations) rather than dropped entirely, so
local_spec_sims.py's own call site doesn't need to change beyond the import
line -- keeping this a drop-in replacement for that one call.
"""

import json
import logging
import re
from pathlib import Path
from typing import List

import numpy as np
import yaml
from shutil import copyfile

logger = logging.getLogger(__name__)

# Matches the leading integer index run_spec_sims_ghcss.py itself gives every
# generated config file ("{i}_field_{field}T.yaml") -- used below to sort
# config paths numerically by that index (0, 1, ..., 10, ...) rather than
# lexicographically (which would put "10_..." before "2_..."). A small,
# self-contained substitute for natsort.natsorted(config_paths, key=str)
# (what he6_cres_spec_sims.experiment.get_config_paths uses): natsort handles
# arbitrary mixed alphanumeric strings in general, but every path this
# function actually sorts has this one specific, known leading-integer shape,
# so the general library isn't needed here -- and adding it would mean a new
# pypi dependency this repo's uv.lock doesn't have yet, which can't be
# regenerated without network access this environment doesn't have.
_LEADING_INT_RE = re.compile(r"^(\d+)")


def _leading_int_sort_key(path: Path) -> tuple:
    m = _LEADING_INT_RE.match(path.name)
    if m is None:
        raise ValueError(
            f"Expected a config filename starting with a leading integer index "
            f"(e.g. '3_field_1.02638T.yaml'), got: {path.name}"
        )
    return (int(m.group(1)), path.name)


def get_experiment_dir(experiment_params: dict) -> Path:
    """Verbatim from he6_cres_spec_sims.experiment.get_experiment_dir."""
    if "output_path" in experiment_params:
        return Path(experiment_params["output_path"])
    base_config_path = Path(experiment_params["base_config_path"])
    parent_dir = base_config_path.parents[0]
    experiment_name = experiment_params["experiment_name"]
    experiment_dir = parent_dir / experiment_name
    return experiment_dir


def get_config_paths(experiment_params: dict) -> List[Path]:
    """Verbatim from he6_cres_spec_sims.experiment.get_config_paths."""
    experiment_dir = get_experiment_dir(experiment_params)

    suffix = "T.yaml"
    config_paths = [
        x
        for x in experiment_dir.glob("**/*{}".format(suffix))
        if (x.is_file() and ("ipynb" not in str(x)))
    ]

    if len(config_paths) == 0:
        raise ValueError(
            "No config files found in experiment_dir: {}. \n\
            Have you run the experiment?\
            (run: exp = exp.Experiment(experiment_params))".format(
                experiment_dir
            )
        )

    # Sorted by each file's own leading integer index (0, 1, 2, ..., not
    # lexicographic string order) so the returned list is in the same order
    # the fields/traps arrays were generated in -- see _leading_int_sort_key's
    # own doc comment for why this isn't natsort.
    config_paths = sorted(config_paths, key=_leading_int_sort_key)

    return config_paths


class RunSpecSimsGhcss:
    """Drop-in replacement for run_spec_sims.RunSpecSims, restricted to the
    generate_configs_only=True call path local_spec_sims.py actually uses.
    Same constructor signature as RunSpecSims, minus he6_cres_spec_sims.
    """

    def __init__(self, *, run_name, subrun_id, noise_run_id, yaml_config, json_config, seed, runs_base_dir: Path):
        self.run_name = run_name
        self.subrun_id = subrun_id
        self.noise_run_id = noise_run_id
        self.yaml_config = yaml_config
        self.json_config = json_config
        self.seed = seed
        self.runs_base_dir = runs_base_dir

        self.print_run_summary()

    def print_run_summary(self):
        logger.info(
            "Run Summary: run_name=%s subrun_id=%s seed=%s noise_run_id=%s yaml_config=%s json_config=%s",
            self.run_name,
            self.subrun_id,
            self.seed,
            self.noise_run_id,
            self.yaml_config,
            self.json_config,
        )

    def run(self, generate_configs_only: bool = True) -> List[Path]:
        if not generate_configs_only:
            raise NotImplementedError(
                "RunSpecSimsGhcss only generates configs -- it never runs "
                "simulations itself (that's ghcss's job now). Pass "
                "generate_configs_only=True (the default), or use "
                "run_spec_sims.RunSpecSims if you actually need the old "
                "he6_cres_spec_sims-based run_sims() path."
            )

        with open(self.yaml_config, "r") as f:
            yaml_dict = yaml.load(f, Loader=yaml.FullLoader)

        with open(self.json_config, "r") as f_json_config:
            run_params = json.load(f_json_config)

        run_params["experiment_name"] = self.run_name
        run_params["base_config_path"] = str(self.yaml_config)
        run_params["rand_seeds"] = [self.seed] * len(run_params["fields_T"])

        base_experiment_dir = self.runs_base_dir / Path(self.run_name)
        base_experiment_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Created directory: %s", base_experiment_dir)

        run_params["output_path"] = base_experiment_dir / Path(f"subrun_{self.subrun_id}")

        for key, val in run_params.items():
            logger.info("%s: %s", key, val)

        self._create_configs_for_experiment(run_params, yaml_dict)
        self._create_experiment_config_file(run_params)

        return get_config_paths(run_params)

    def _create_configs_for_experiment(self, experiment_params: dict, config_dict: dict) -> None:
        """Verbatim from Experiment.create_configs_for_experiment, with
        self.config_dict (which that method reads for the "already have a
        parsed dict, don't reopen the file" branch) always available here
        since local_spec_sims.py always passes a yaml_dict through
        RunSpecSims's own equivalent __init__ path -- so the "config_dict is
        None, reopen the file" branch that method also has is dropped as
        dead code for this call path, not ported.
        """
        base_config_path = Path(experiment_params["base_config_path"])
        experiment_dir = get_experiment_dir(experiment_params)

        if experiment_dir.exists() and not experiment_dir.is_dir():
            raise ValueError("Not a directory: {} ".format(experiment_dir))
        experiment_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Created directory: %s", experiment_dir)

        events_to_simulate = experiment_params["events_to_simulate"]
        betas_to_simulate = experiment_params["betas_to_simulate"]
        seeds = experiment_params["rand_seeds"]
        fields = experiment_params["fields_T"]
        traps = experiment_params["traps_A"]

        for i, (seed, field, trap) in enumerate(zip(seeds, fields, traps)):
            # Round the field because there are often small rounding errors.
            field = np.around(field, 6)
            config_path = experiment_dir / "{}_field_{}T{}".format(i, field, base_config_path.suffix)

            copyfile(base_config_path, config_path)

            # Matches the original exactly: config_dict is the same object
            # reused across every iteration of this loop, not copied. That's
            # fine (not a bug to fix) because every iteration overwrites all
            # four of these keys before writing, so nothing from a previous
            # field's write can leak into the next one's file.
            if seed is not None:
                config_dict["Settings"]["rand_seed"] = int(seed)
            else:
                config_dict["Settings"]["rand_seed"] = None
            config_dict["Physics"]["events_to_simulate"] = int(events_to_simulate)
            config_dict["Physics"]["betas_to_simulate"] = int(betas_to_simulate)
            config_dict["EventBuilder"]["main_field"] = float(field)
            config_dict["EventBuilder"]["trap_current"] = float(trap)

            with open(config_path, "w") as f:
                yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    def _create_experiment_config_file(self, experiment_params: dict) -> None:
        """Verbatim from Experiment.create_experiment_config_file."""
        experiment_dir = get_experiment_dir(experiment_params)
        experiment_config = experiment_dir / (experiment_params["experiment_name"] + "_exp.yaml")

        with open(experiment_config, "w") as f:
            yaml.dump(experiment_params, f, default_flow_style=False, sort_keys=False)
