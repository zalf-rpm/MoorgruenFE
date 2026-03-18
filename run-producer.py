#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import copy
import json
import os
import sys
import zmq
from collections import defaultdict
import pandas as pd

import monica_io3
import shared


def run_producer(server=None, port=None):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "mode": "re-local-remote",
        "server-port": port if port else "6666",
        "server": server if server else "localhost",
        "sim.json": os.path.join(os.path.dirname(__file__), "sim.json"),
        "crop.json": os.path.join(os.path.dirname(__file__), "crop.json"),
        "site.json": os.path.join(os.path.dirname(__file__), "site.json"),
        "monica_path_to_climate_dir": r"C:\Users\escueta\PycharmProjects\MoorgruenFE\data",
        "path_to_data_dir": "./data/",
        "path_to_out": "out/",
    }
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    socket.connect("tcp://" + config["server"] + ":" + config["server-port"])

    with open(config["sim.json"]) as _:
        sim_json = json.load(_)

    with open(config["site.json"]) as _:
        site_json = json.load(_)

    with open(config["crop.json"]) as _:
        crop_json = json.load(_)

    # Extract template from crop configuration
    fert_min_template = crop_json.pop("fert_min_template")

    # Read soil data
    soil_df = pd.read_csv(f"{config['path_to_data_dir']}/Soil.csv", sep=';')

    soil_profiles = defaultdict(list)
    prev_depth_m = 0
    prev_soil_name = None
  
    for _, row in soil_df.iterrows():
        soil_name = row['Soil']
        if soil_name != prev_soil_name:
            prev_soil_name = soil_name
            prev_depth_m = 0.0
        current_depth_m = float(row['Depth']) / 100.0
        thickness = current_depth_m - prev_depth_m
        prev_depth_m = current_depth_m

        layer = {
            "Thickness": [thickness, "m"],
            "SoilBulkDensity": [float(row['Bulk_density']) , "kg/m3"],
            "SoilOrganicCarbon": [float(row['Corg']) / 100.0, "%"],
            "Clay": [float(row['Clay']), "m3/m3"],
            "Sand": [float(row['Sand']), "m3/m3"],
            "Silt": [float(row['Silt']), "m3/m3"],
            # "pH": [float(row['pH']), "pH"]
        }
        soil_profiles[soil_name].append(layer)

    # Read metadata and management
    metadata_df = pd.read_csv(f"{config['path_to_data_dir']}/Meta.csv", sep=';')
    metadata_df["Crop"] = metadata_df["Crop"].astype(str).str.upper()

    df_gr = metadata_df[metadata_df["Crop"] == "GR"].copy()

    # Skip experiments
    skip_experiments = {"EX174", "EX175", "EX176", "EX177"}
    df_gr = df_gr[~df_gr["Experiment"].astype(str).isin(skip_experiments)].copy()

    # Test
    # TEST_EXPERIMENT = "EX1"
    # TEST_EXPERIMENT = None

    # if TEST_EXPERIMENT is not None:
    #     df_gr = df_gr[df_gr["Experiment"] == TEST_EXPERIMENT].copy()
    #     print(f"Running producer only for Experiment={TEST_EXPERIMENT}")

    df_gr["Year"] = pd.to_numeric(df_gr["Year"], errors="coerce").astype("Int64")

    no_of_exps = 0
    last_env = None

    for _, meta in df_gr.iterrows():
        year = int(meta["Year"])

        if pd.isna(meta["Soil"]):
            print(f"Skipping {meta['Experiment']}: Soil is missing")
            continue

        if meta["Soil"] not in soil_profiles:
            print(f"Skipping {meta['Experiment']}: soil profile '{meta['Soil']}' not found in Soil.csv")
            continue

        env = monica_io3.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        env["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
        env["csvViaHeaderOptions"]["start-date"] = f"{year}-01-01"
        env["csvViaHeaderOptions"]["end-date"] = f"{year}-12-31"

        env["pathToClimateCSV"] = f"{config['monica_path_to_climate_dir']}/weather/{meta['Weather']}.csv"

        env["params"]["siteParameters"]["SoilProfileParameters"] = soil_profiles[meta["Soil"]]
        env["params"]["siteParameters"]["HeightNN"] = float(meta['Elevation'])
        env["params"]["siteParameters"]["Latitude"] = float(meta['Lat'])

        # Build worksteps
        ws_template = copy.deepcopy(env["cropRotation"][0]["worksteps"])
        ws_out = []

        for ws in ws_template:
            ws_copy = copy.deepcopy(ws)
            if "date" in ws_copy and isinstance(ws_copy["date"], str) and len(ws_copy["date"]) >= 10:
                ws_copy["date"] = f"{year}-{ws_copy['date'][5:]}"
            ws_out.append(ws_copy)

        env["cropRotation"] = [{"worksteps": ws_out}]

        env["customId"] = {
            "nodata": False,
            "experiment": str(meta["Experiment"]),
            "soil_name": str(meta["Soil"]),
        }

        socket.send_json(env)
        last_env = copy.deepcopy(env)
        no_of_exps += 1
        print(f"{os.path.basename(__file__)} sent job {no_of_exps} for {meta['Experiment']}")

        # Save the sent env_template as a json file
        # with open(f"out/env_template_{meta['Experiment']}.json", "w") as _:
        #     json.dump(env, _, indent=4)

    # Send final nodata message
    if last_env is None:
        print(f"{os.path.basename(__file__)} no experiments sent")
        return

    last_env["customId"] = {
        "no_of_exps": no_of_exps,
        "nodata": True,
        "crop": "GR"
    }
    socket.send_json(last_env)
    print(f"{os.path.basename(__file__)} done")


if __name__ == "__main__":
    run_producer()
