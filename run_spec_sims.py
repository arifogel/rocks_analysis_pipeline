#!/usr/bin/env python3
import time
import argparse

import os
import pandas as pd

from pathlib import Path
import yaml
import sys
import subprocess as sp
import json

from rocks_utility import (
    he6cres_db_query,
    get_pst_time,
    set_permissions,
    log_file_break,
)

# Import settings.
pd.set_option("display.max_columns", 100)
pd.set_option('display.max_rows', 500)


# Path to imports
import he6_cres_spec_sims.experiment as exp

############################################################################

def main():
    umask = sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    # Parse command line arguments.
    par = argparse.ArgumentParser()
    arg = par.add_argument
    arg(
        "-r",
        "--run_name",
        type=str,
        help="labelled run name for MC",
        required=True,
    )
    arg(
        "-nid",
        "--noise_run_id",
        type=int,
        help="run_id to use for noise floor in katydid run. If -1 then will use self as noise file.",
        required=True,
    )
    arg(
        "-y",
        "--yaml_config",
        type=str,
        help="base .yaml config file, should exist in base config directory.",
        required=True,
    )
    arg(
        "-j",
        "--json_config",
        type=str,
        help="base .json config file, should exist in base config directory.",
        required=True,
    )
    arg(
        "-s",
        "--seed",
        type=int,
        help="random seed sent to the Monte Carlo",
        required=True,
    )
    arg(
        "-sr",
        "--subrun_id",
        type=int,
        help="subrun ID [one unique seed per subrun, everything else identical]",
        required=True,
    )
    arg(
        "-rb",
        "--runs_base_dir",
        type=str,
        help="Base output directory for runs",
        required=True,
    )

    args = par.parse_args()

    print(f"\nRunning spec-sims. STARTING at PST time: {get_pst_time()}\n")

    # Print summary of spec_sims running.
    print(f"\nProcessing: subrun_id: {args.subrun_id}.\n")

    # Force a write to the log.
    sys.stdout.flush()

    # Done at the beginning and end of main to ensure all users have appropriate access
    set_permissions()

    # Begin running spec-sims
    run_spec_sims = RunSpecSims(
        run_name=args.run_name,
        subrun_id=args.subrun_id,
        noise_run_id=args.noise_run_id,
        yaml_config=args.yaml_config,
        json_config=args.json_config,
        seed=args.seed,
        runs_base_dir=args.runs_base_dir,
    )

    # set_permissions()

    print(f"\nRunning spec-sims on {args.run_name} {args.subrun_id} DONE at PST time: {get_pst_time()}\n")
    run_spec_sims.run()

    log_file_break()
    return None


class RunSpecSims:
    def __init__(self, *, run_name, subrun_id, noise_run_id, yaml_config, json_config, seed, runs_base_dir: Path):

        self.run_name = run_name
        self.subrun_id = subrun_id
        self.noise_run_id = noise_run_id
        self.yaml_config = yaml_config
        self.json_config = json_config
        self.seed = seed
        self.runs_base_dir = runs_base_dir

        self.print_run_summary()

        return None

    def print_run_summary(self):
        print("\nRun Summary:")
        print(f"run_name: {self.run_name}")
        print(f"subrun_id: {self.subrun_id}")
        print(f"seed: {self.seed}")
        print(f"noise_run_id: {self.noise_run_id}")
        print(f"yaml_config: {self.yaml_config}")
        print(f"json_config: {self.json_config}\n")
        return None

    # Define a function to aggregate file_path into a list ordered by channel
    def aggregate_paths(self, group):
        ordered_paths = group.sort_values(by='channel')['file_path'].apply(str).tolist()
        return pd.Series({
            'run_id': group['run_id'].iloc[0],
            'true_field': group['true_field'].iloc[0],
            'file_path': ordered_paths
        })

    def run(self, generate_configs_only: bool = False):
        # Force a write to the log.
        sys.stdout.flush()

        yaml_config_full = self.yaml_config
        json_config_full = self.json_config

        #Load the yaml configuration file
        with open(yaml_config_full, "r") as f:
            try:
                yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
            except yaml.YAMLError as e:
                print(e)

        # Open the JSON file and load its content into a dictionary
        with open(json_config_full, "r") as f_json_config:
            run_params = json.load(f_json_config)

        ##### run_params format
        #run_params = {
        #    "experiment_name": "RUN_NAME",
        #    "base_config_path": "YAML_NAME",
        #    "events_to_simulate": -1,
        #    "betas_to_simulate": 1000,
        #    "isotope": "Ne19",
        #    "rand_seeds": rand_seeds,
        #    "fields_T" : fields.tolist(),
        #    "traps_A": traps.tolist()
        #}

        run_params["experiment_name"] = self.run_name
        run_params["base_config_path"] = str(yaml_config_full)
        run_params["rand_seeds"] = [self.seed] * len(run_params["fields_T"])


        #define where the MC results are going to be written to
        base_experiment_dir = self.runs_base_dir / Path(self.run_name)
        print(base_experiment_dir)

        ## Make the base_run_dir if it doesn't exist
        base_experiment_dir.mkdir(parents=True, exist_ok=True)
        print("Created directory: {} ".format(base_experiment_dir))

        run_params["output_path"] = base_experiment_dir / Path(f"subrun_{self.subrun_id}")
        print(run_params["output_path"])

        print(yaml_dict)

        t_start = time.process_time()

        ####################Do the Run!##############################
        for key, val in run_params.items():
            print("{}: {}".format(key, val))

        #exp.Experiment(run_params)
        experiment = exp.Experiment(
            run_params, yaml_dict, generate_configs_only=generate_configs_only
        )
        #############################################################
        t_stop = time.process_time()
        elapsed = t_stop - t_start
        print(elapsed)

        return experiment.config_paths

if __name__ == "__main__":
    if os.environ.get("HE6_PROFILE"):
        import cProfile
        import pstats

        profiler = cProfile.Profile()
        profiler.enable()
        try:
                main()
        finally:
            profiler.disable()
            out_path = os.environ.get("HE6_PROFILE_OUT", "profile.out")
            stats = pstats.Stats(profiler).sort_stats("cumulative")
            stats.dump_stats(out_path)
            print(f"\n=== Top 40 by cumulative time (also written to {out_path}) ===")
            stats.print_stats(40)
    else:
        main()