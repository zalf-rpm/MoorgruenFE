# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import subprocess as sp
import sys

local_run = False


def update_config(config, argv, print_config=False, allow_new_keys=False):
    if len(argv) > 1:
        for arg in argv[1:]:
            kv = arg.split("=", maxsplit=1)
            if len(kv) < 2:
                continue
            k, v = kv
            if len(k) > 1 and k[:2] == "--":
                k = k[2:]
            if allow_new_keys or k in config:
                config[k] = v.lower() == "true" if v.lower() in ["true", "false"] else v
        if print_config:
            print(config)


def run_parallel_calibrations(server=None, prod_port=None, cons_port=None):
    config = {
        "mode": "hpc-local-remote",
        "prod-port": prod_port if prod_port else "6666",
        "cons-port": cons_port if cons_port else "7777",
        "server": server if server else "login01.cluster.zalf.de",
        "setups-file": "sim_setups_calibration.csv",
        "path_to_out": "out/",
        "run-setup": "1",
        "path_to_python": "python" if local_run else "/home/rpm/.conda/envs/clim4cast/bin/python",
        "repetitions": "10",
        "path_to_meta_csv": "./data/Meta.csv",
        "rcp": "26",
        "path_to_grassmind_biomass_files": "/beegfs/rpm/projects/monica/project/MoorGruenFE/rcp{rcp}/",
        "observation_filename_template": "parameter_R{row}C{col}I41.bt"
    }

    update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    rcp = config["rcp"]
    setup_id = int(config["run-setup"])

    out_dir = f"{config['path_to_out']}/rcp{rcp}/calib_setup{setup_id}_all_points/"
    obs_dir = config["path_to_grassmind_biomass_files"].format(rcp=rcp)

    p = sp.Popen([
        config["path_to_python"],
        "run-calibration.py",
        f"mode={config['mode']}",
        f"server={config['server']}",
        f"prod-port={config['prod-port']}",
        f"cons-port={config['cons-port']}",
        f"setups-file={config['setups-file']}",
        f"run-setups=[{config['run-setup']}]",
        f"path_to_out={out_dir}",
        f"repetitions={config['repetitions']}",
        f"path_to_meta_csv={config['path_to_meta_csv']}",
        f"path_to_grassmind_biomass_files={obs_dir}",
        f"observation_filename_template={config['observation_filename_template']}",
    ])

    p.wait()

    print(f"Finished calibration setup {setup_id} for all points (RCP {rcp})")


if __name__ == "__main__":
    run_parallel_calibrations()
