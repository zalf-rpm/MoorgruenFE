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

from collections import defaultdict
from datetime import datetime, date, timedelta
import csv
import os
import sys
import time

import matplotlib.pyplot as plt
import pandas as pd
import spotpy
import monica_run_lib

import calibration_spotpy_setup_MONICA as spotpy_monica_connector

local_run = True


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


def parse_run_setups(run_setups_str):
    out = []
    for token in run_setups_str.strip()[1:-1].split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            a, b = token.split("-", maxsplit=1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(token))
    return out


def read_grassmind_biomass(path):
    year_to_biomasses = defaultdict(list)
    with open(path) as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=";,\t")
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader, None)
        next(reader, None)
        next(reader, None)
        start_date = date(2021, 1, 1)
        for row in reader:
            if len(row) < 2:
                continue
            doy = int(round(float(row[0]) * 365))
            current_date = start_date + timedelta(days=doy)
            biomass = float(row[1]) * 10000 * 1000  # t/m^2 to kg/ha
            if (current_date.month, current_date.day) in [(6, 15), (9, 1)]:
                year_to_biomasses[current_date.year].append(biomass)
    return year_to_biomasses


def read_calibration_params(path="calibratethese.csv"):
    params = []
    with open(path) as params_csv:
        dialect = csv.Sniffer().sniff(params_csv.read(), delimiters=";,\t")
        params_csv.seek(0)
        reader = csv.reader(params_csv, dialect)
        next(reader, None)
        for row in reader:
            p = {"name": row[0]}
            if len(row[1]) > 0:
                p["array"] = int(row[1])
            for n, i in [("low", 2), ("high", 3), ("step", 4), ("optguess", 5), ("minbound", 6), ("maxbound", 7)]:
                if len(row[i]) > 0:
                    p[n] = float(row[i])
            if len(row) == 9 and len(row[8]) > 0:
                p["derive_function"] = lambda _, _2: eval(row[8])
            params.append(p)
    return params


def build_points_and_observations(config):
    meta_df = pd.read_csv(config["path_to_meta_csv"], sep=";")
    meta_df["Experiment"] = meta_df["Experiment"].astype(str)

    points = []
    observations_by_exp_year = {}

    for _, meta in meta_df.iterrows():
        exp_id = str(meta["Experiment"])

        if pd.isna(meta["Lat"]) or pd.isna(meta["Long"]):
            print(f"Skipping {exp_id}: missing Lat/Long")
            continue

        if pd.isna(meta["GrassmindRow"]) or pd.isna(meta["GrassmindCol"]):
            print(f"Skipping {exp_id}: missing GrassmindRow/GrassmindCol")
            continue

        row = int(float(meta["GrassmindRow"]))
        col = int(float(meta["GrassmindCol"]))

        obs_file = config["observation_filename_template"].format(
            row=row,
            col=col,
            experiment=exp_id,
        )

        obs_path = os.path.join(config["path_to_grassmind_biomass_files"], obs_file)

        if not os.path.exists(obs_path):
            print(f"Skipping {exp_id}: observation file not found: {obs_path}")
            continue

        obs = read_grassmind_biomass(obs_path)
        if not obs:
            print(f"Skipping {exp_id}: empty observations")
            continue

        points.append(meta.to_dict())
        observations_by_exp_year[exp_id] = obs

    return points, observations_by_exp_year


def run_calibration(server=None, prod_port=None, cons_port=None):
    config = {
        "mode": "hpc-local-remote",
        "prod-port": prod_port if prod_port else "6666",
        "cons-port": cons_port if cons_port else "7777",
        "server": server if server else "login01.cluster.zalf.de",
        "sim.json": "sim_calibration.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_calibration.csv",
        "path_to_out": "out/",
        "run-setups": "[1]",
        "repetitions": "2000",
        "path_to_meta_csv": "./data/Meta.csv",
        "path_to_grassmind_biomass_files": None,
        "observation_filename_template": "parameter_R{row}C{col}I41.bt",
    }
    update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    if not config["path_to_grassmind_biomass_files"]:
        raise ValueError("Please pass path_to_grassmind_biomass_files=/path/to/rcpXX/")

    path_to_out_folder = config["path_to_out"]
    os.makedirs(path_to_out_folder, exist_ok=True)

    with open(f"{path_to_out_folder}/run-calibration.out", "a") as f:
        f.write(f"{datetime.now()} config: {config}\n")

    calib_points, observations_by_exp_year = build_points_and_observations(config)
    print(f"Using {len(calib_points)} calibration points with {n_obs} observations")

    if not calib_points:
        raise RuntimeError("No calibration points with matching Grassmind observations found.")

    params = read_calibration_params("calibratethese.csv")
    setups = monica_run_lib.read_sim_setups(config["setups-file"])

    for setup_id in parse_run_setups(config["run-setups"]):
        start_time = time.time()
        setup = setups.get(setup_id, None)
        if not setup:
            continue

        spot_setup = spotpy_monica_connector.spot_setup(
            params,
            observations_by_exp_year,
            monicas_host=config["server"],
            monicas_in_port=config["prod-port"],
            monicas_out_port=config["cons-port"],
            calib_points=calib_points,
            setup_id=setup_id,
            setup=setup,
            path_to_out=path_to_out_folder,
            mode=config["mode"],
        )

        rep = int(config["repetitions"])
        sampler = spotpy.algorithms.sceua(
            spot_setup,
            dbname=f"{path_to_out_folder}/SCEUA_monica_results_setup{setup_id}",
            dbformat="csv",
        )
        sampler.sample(rep, ngs=len(params) * 2 + 1, kstop=100, peps=0.0001, pcento=0.0001)

        with open(f"{path_to_out_folder}/best_setup{setup_id}.out", "a") as f:
            sampler.status.print_status_final(f)

        results = spotpy.analyser.load_csv_results(f"{path_to_out_folder}/SCEUA_monica_results_setup{setup_id}")
        fig = plt.figure(1, figsize=(9, 6))
        plt.plot(results["like1"], "r+")
        plt.ylabel("RMSE")
        plt.xlabel("Iteration")
        fig.savefig(f"{path_to_out_folder}/SCEUA_objectivefunctiontrace_MONICA_setup{setup_id}.png", dpi=150)
        plt.close(fig)

        print(f"Finished setup {setup_id} for all points in {time.time() - start_time:.2f} s")


if __name__ == "__main__":
    run_calibration()
