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
from pyproj import CRS
import monica_run_lib as Mrunlib

import monica_io3
import shared

PATHS = {
    "re-local-remote": {
        "path-to-climate-dir": "./data/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/"
    },
    "remoteProducer-remoteMonica": {
        "path-to-climate-dir": "/data/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/"
    }
}


def run_producer(server=None, port=None):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "mode": "re-local-remote",
        "server-port": port if port else "6667",
        "server": server if server else "login01.cluster.zalf.de",
        "sim.json": os.path.join(os.path.dirname(__file__), "sim_proj_ipp.json"),
        "crop.json": os.path.join(os.path.dirname(__file__), "crop.json"),
        "site.json": os.path.join(os.path.dirname(__file__), "site.json"),
        "path_to_out": "out/",
        "setups-file": "sim_setups_projection.csv",
        "run-setups": "[9]",
    }
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    paths = PATHS[config["mode"]]

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
    soil_df = pd.read_csv(os.path.join(paths["path-to-data-dir"], "Soil.csv"), sep=';')

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
            "SoilOrganicCarbon": [float(row['Corg']), "%"],
            "Clay": [float(row['Clay']), "m3/m3"],
            "Sand": [float(row['Sand']), "m3/m3"],
            "Silt": [float(row['Silt']), "m3/m3"],
            # "pH": [float(row['pH']), "pH"]
        }
        soil_profiles[soil_name].append(layer)

    # Read metadata and management
    metadata_df = pd.read_csv(os.path.join(paths["path-to-data-dir"], "Meta.csv"), sep=';')
    metadata_df["Crop"] = metadata_df["Crop"].astype(str).str.upper()

    # Read groundwater
    metadata_df["groundwaterMIN"] = pd.to_numeric(metadata_df["groundwaterMIN"], errors="coerce")
    metadata_df["groundwaterMAX"] = pd.to_numeric(metadata_df["groundwaterMAX"], errors="coerce")

    df_gr = metadata_df[metadata_df["Crop"] == "GR"].copy()

    # Read setups
    setups_df = pd.read_csv(config["setups-file"], sep=",")

    run_setups = []
    rs_ranges = config["run-setups"][1:-1].split(",")

    for rsr in rs_ranges:
        rs_r = rsr.split("-")
        if len(rs_r) == 2:
            run_setups.extend(range(int(rs_r[0]), int(rs_r[1]) + 1))
        elif len(rs_r) == 1 and rs_r[0].strip():
            run_setups.append(int(rs_r[0]))

    wgs84_crs = CRS.from_epsg(4326)

    cdict = {}

    latlon_path = os.path.join(paths["path-to-climate-dir"], str(setups_df.iloc[0]["climate_path_to_latlon_file"])
                               .strip("/"), "latlon-to-rowcol.json")
    # latlon_path = "./data/latlon-to-rowcol.json"

    climate_data_interpolator = Mrunlib.create_climate_geoGrid_interpolator_from_json_file(latlon_path, wgs84_crs,
                                                                                           wgs84_crs, cdict)

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

    for _, setup in setups_df[setups_df["id"].isin(run_setups)].iterrows():
        gcm = setup["gcm"]
        rcm = setup["rcm"]
        scenario = setup["scenario"]
        ensmem = setup["ensmem"]
        version = setup["version"]
        climate_base = setup["climate_path_to_csvs"]

        groundwater_level = setup["groundwater-level"]

        if groundwater_level not in {"FALSE", "MIN", "MAX", "MINMAX"}:
            raise ValueError(f"Invalid groundwater-level '{groundwater_level}' in setup id {setup['id']}")

        for _, meta in df_gr.iterrows():
            start_date = str(setup["start_date"])
            end_date = str(setup["end_date"])
            start_year = int(start_date[:4])

            if pd.isna(meta["Soil"]):
                print(f"Skipping {meta['Experiment']}: Soil is missing")
                continue

            if meta["Soil"] not in soil_profiles:
                print(f"Skipping {meta['Experiment']}: soil profile '{meta['Soil']}' not found in Soil.csv")
                continue

            if groundwater_level in {"MIN", "MINMAX"} and pd.isna(meta["groundwaterMIN"]):
                print(f"Skipping {meta['Experiment']}: groundwaterMIN is missing")
                continue

            if groundwater_level in {"MAX", "MINMAX"} and pd.isna(meta["groundwaterMAX"]):
                print(f"Skipping {meta['Experiment']}: groundwaterMAX is missing")
                continue

            env = monica_io3.create_env_json_from_json_config({
                "crop": crop_json,
                "site": site_json,
                "sim": sim_json,
                "climate": ""
            })

            env["csvViaHeaderOptions"] = copy.deepcopy(sim_json["climate.csv-options"])
            env["csvViaHeaderOptions"]["start-date"] = start_date
            env["csvViaHeaderOptions"]["end-date"] = end_date

            lat = float(meta["Lat"])
            lon = float(meta["Long"])

            crow, ccol = climate_data_interpolator(lon, lat)

            crow = int(crow)
            ccol = int(ccol)

            env["pathToClimateCSV"] = [
                f"{paths['monica-path-to-climate-dir'].rstrip('/')}/"
                f"{str(climate_base).strip('/')}/"
                f"{gcm}/{rcm}/{scenario}/{ensmem}/{version}/row-{crow}/col-{ccol}.csv"
            ]

            # print(env["pathToClimateCSV"])

            env["params"]["siteParameters"]["SoilProfileParameters"] = soil_profiles[meta["Soil"]]
            env["params"]["siteParameters"]["HeightNN"] = float(meta['Elevation'])
            env["params"]["siteParameters"]["Latitude"] = float(meta['Lat'])

            if groundwater_level == "MINMAX":
                env["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
                env["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [float(meta["groundwaterMIN"]), "m"]
                env["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [float(meta["groundwaterMAX"]), "m"]

            elif groundwater_level == "MIN":
                env["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
                env["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [float(meta["groundwaterMIN"]), "m"]
                env["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [float(meta["groundwaterMIN"]), "m"]

            elif groundwater_level == "MAX":
                env["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
                env["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [float(meta["groundwaterMAX"]), "m"]
                env["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [float(meta["groundwaterMAX"]), "m"]

            # Build worksteps
            # Sow only once
            # ws_template = copy.deepcopy(env["cropRotation"][0]["worksteps"])
            # end_year = int(end_date[:4])
            # ws_out = []
            #
            # for ws in ws_template:
            #     if ws["type"] == "Sowing":
            #         ws_copy = copy.deepcopy(ws)
            #         ws_copy["date"] = f"{start_year}-{ws_copy['date'][5:]}"
            #         ws_out.append(ws_copy)
            #
            # for year in range(start_year, end_year + 1):
            #     for ws in ws_template:
            #         if ws["type"] == "Cutting":
            #             ws_copy = copy.deepcopy(ws)
            #             ws_copy["date"] = f"{year}-{ws_copy['date'][5:]}"
            #             ws_out.append(ws_copy)
            #
            # ws_out.sort(key=lambda x: x["date"])
            # env["cropRotation"] = [{"worksteps": ws_out}]

            # Sow every year
            ws_template = copy.deepcopy(env["cropRotation"][0]["worksteps"])
            end_year = int(end_date[:4])
            ws_out = []

            for year in range(start_year, end_year + 1):
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
                "gcm": str(gcm),
                "rcm": str(rcm),
                "scenario": str(scenario),
                "ensmem": str(ensmem),
                "version": str(version),
                "setup_id": int(setup["id"]),
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
