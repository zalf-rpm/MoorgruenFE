#!/usr/bin/python
# -*- coding: UTF-8 -*-
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

from collections import defaultdict
import copy
from datetime import datetime
import json
import os
import time
import uuid

import pandas as pd
from pyproj import CRS
import spotpy
import zmq

import monica_io3
import monica_run_lib as Mrunlib


PATHS = {
    "re-local-remote": {
        "path-to-climate-dir": "/data/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
    },
    "mbm-local-remote": {
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
    },
    "mbm-local-local": {
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        "monica-path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        "path-to-data-dir": "./data/",
    },
    "hpc-local-remote": {
        "path-to-climate-dir": "/beegfs/common/data/climate/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
    }
}

DATA_SOIL = "data/Soil.csv"

TEMPLATE_PATH_LATLON = "{path_to_climate_dir}/latlon-to-rowcol.json"
TEMPLATE_PATH_CLIMATE_CSV = "{gcm}/{rcm}/{scenario}/{ensmem}/{version}/row-{crow}/col-{ccol}.csv"


def flatten_exp_year_dict(values_by_exp_year, experiment_order):
    flat = []
    for exp_id in experiment_order:
        by_year = values_by_exp_year.get(str(exp_id), {})
        for year in sorted(by_year.keys()):
            flat.extend(by_year[year])
    return flat


def read_soil_profiles(path_to_soil_csv):
    soil_df = pd.read_csv(path_to_soil_csv, sep=";")
    soil_profiles = defaultdict(list)
    prev_depth_m = 0.0
    prev_soil_name = None

    for _, row in soil_df.iterrows():
        soil_name = str(row["Soil"])
        if soil_name != prev_soil_name:
            prev_soil_name = soil_name
            prev_depth_m = 0.0

        current_depth_m = float(row["Depth"]) / 100.0
        thickness = current_depth_m - prev_depth_m
        prev_depth_m = current_depth_m

        soil_profiles[soil_name].append({
            "Thickness": [thickness, "m"],
            "SoilBulkDensity": [float(row["Bulk_density"]), "kg/m3"],
            "SoilOrganicCarbon": [float(row["Corg"]), "%"],
            "Clay": [float(row["Clay"]), "m3/m3"],
            "Sand": [float(row["Sand"]), "m3/m3"],
            "Silt": [float(row["Silt"]), "m3/m3"],
        })
    return soil_profiles


def apply_crop_calibration_parameters(crop_params, params):
    ps = copy.deepcopy(crop_params)
    for pname, pval in params.items():
        base_name = pname.rsplit("_", 1)[0] if pname.rsplit("_", 1)[-1].isdigit() else pname

        if base_name in ["SpecificLeafArea", "StageKcFactor"]:
            for i in range(len(ps["cultivar"][base_name][0])):
                ps["cultivar"][base_name][0][i] *= pval
        elif base_name == "DroughtStressThreshold":
            for i in range(len(ps["cultivar"][base_name])):
                ps["cultivar"][base_name][i] *= pval
        elif base_name == "CropSpecificMaxRootingDepth":
            ps["cultivar"][base_name] = pval

    return ps


class spot_setup(object):
    def __init__(self, user_params, observations_by_exp_year, monicas_host, monicas_in_port, monicas_out_port,
                 calib_points, setup_id, setup, path_to_out, mode):
        self.user_params = user_params
        self.params = []
        self.observations_by_exp_year = observations_by_exp_year
        self.calib_points = calib_points
        self.experiment_order = [str(p["Experiment"]) for p in calib_points]
        self.obs_flat_list = flatten_exp_year_dict(observations_by_exp_year, self.experiment_order)
        self.setup_id = setup_id
        self.setup = setup
        self.path_to_out = path_to_out
        self.paths = PATHS[mode]

        self.path_to_soil_csv = DATA_SOIL
        self.path_to_latlon_json = TEMPLATE_PATH_LATLON.format(
            path_to_climate_dir=self.paths["path-to-climate-dir"].rstrip("/") + "/"
                                + str(self.setup["climate_path_to_latlon_file"]).strip("/"))

        self.context = zmq.Context()
        self.prod_socket = self.context.socket(zmq.PUSH)
        self.prod_socket.connect(f"tcp://{monicas_host}:{monicas_in_port}")
        self.cons_socket = self.context.socket(zmq.DEALER)
        self.shared_id = str(uuid.uuid4())
        self.cons_socket.setsockopt_string(zmq.ROUTING_ID, self.shared_id)
        self.cons_socket.RCVTIMEO = 60000
        self.cons_socket.connect(f"tcp://{monicas_host}:{monicas_out_port}")

        os.makedirs(self.path_to_out, exist_ok=True)
        self.path_to_prod_out_file = f"{self.path_to_out}/producer.out"
        self.path_to_cons_out_file = f"{self.path_to_out}/consumer.out"

        self.init_producer()

        with open(self.path_to_prod_out_file, "a") as f:
            f.write(f"experiment_order: {self.experiment_order}\n")
            f.write(f"obs_flat_list length: {len(self.obs_flat_list)}\n")

        for par in user_params:
            par = dict(par)
            par_name = par["name"]
            if "array" in par:
                par["name"] = f"{par_name}_{par['array']}"
                del par["array"]
            if "derive_function" not in par:
                self.params.append(spotpy.parameter.Uniform(**par))

    def init_producer(self):
        with open(f"{self.path_to_out}/spot_setup.out", "a") as f:
            f.write(f"{datetime.now()} start producer init\n")

        self.soil_profiles = read_soil_profiles(self.path_to_soil_csv)

        wgs84_crs = CRS.from_epsg(4326)
        self.cdict = {}
        self.climate_data_interpolator = Mrunlib.create_climate_geoGrid_interpolator_from_json_file(
            self.path_to_latlon_json, wgs84_crs, wgs84_crs, self.cdict
        )

        with open(self.setup["sim.json"]) as f:
            sim_json = json.load(f)
        if self.setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(self.setup["start_date"])
        if self.setup["end_date"]:
            sim_json["climate.csv-options"]["end-date"] = str(self.setup["end_date"])

        with open(self.setup["site.json"]) as f:
            site_json = json.load(f)
        scenario = self.setup["scenario"]
        if len(scenario) > 0 and scenario[:3].lower() == "rcp":
            site_json["EnvironmentParameters"]["rcp"] = scenario

        with open(self.setup["crop.json"]) as f:
            crop_json = json.load(f)
        crop_json.pop("fert_min_template", None)
        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = self.setup[
            "use_vernalisation_fix"] if "use_vernalisation_fix" in self.setup else False

        self.env_template = monica_io3.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": "",
        })
        self.env_template["sharedId"] = self.shared_id
        self.env_template["csvViaHeaderOptions"] = copy.deepcopy(sim_json["climate.csv-options"])
        self.env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = self.setup["LeafExtensionModifier"]
        self.env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = self.setup["fertilization"]
        self.env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = self.setup["irrigation"]
        self.env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = self.setup["NitrogenResponseOn"]
        self.env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = self.setup["WaterDeficitResponseOn"]
        self.env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = self.setup["EmergenceMoistureControlOn"]
        self.env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = self.setup["EmergenceFloodingControlOn"]

        self.orig_crop_params = copy.deepcopy(
            self.env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"])

    def _build_env_for_point(self, meta, params, env_id):
        env = copy.deepcopy(self.env_template)
        env["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"] = apply_crop_calibration_parameters(
            self.orig_crop_params, params
        )

        start_date = str(self.setup.get("start_date") or env["csvViaHeaderOptions"].get("start-date"))
        end_date = str(self.setup.get("end_date") or env["csvViaHeaderOptions"].get("end-date"))
        start_year = int(start_date[:4])
        env["csvViaHeaderOptions"] = copy.deepcopy(env["csvViaHeaderOptions"])
        env["csvViaHeaderOptions"]["start-date"] = start_date
        env["csvViaHeaderOptions"]["end-date"] = end_date

        soil_name = str(meta["Soil"])
        if soil_name not in self.soil_profiles:
            raise ValueError(f"Soil profile '{soil_name}' not found in {self.path_to_soil_csv}")
        env["params"]["siteParameters"]["SoilProfileParameters"] = self.soil_profiles[soil_name]

        lat = float(meta["Lat"])
        lon = float(meta["Long"])
        crow, ccol = self.climate_data_interpolator(lon, lat)
        crow, ccol = int(crow), int(ccol)

        climate_base = str(self.setup["climate_path_to_csvs"]).strip("/")
        subpath = TEMPLATE_PATH_CLIMATE_CSV.format(
            gcm=self.setup["gcm"], rcm=self.setup["rcm"], scenario=self.setup["scenario"],
            ensmem=self.setup["ensmem"], version=self.setup["version"], crow=crow, ccol=ccol,
        )
        env["pathToClimateCSV"] = [
            f"{self.paths['monica-path-to-climate-dir'].rstrip('/')}/{climate_base}/{subpath}"
        ]

        if self.setup["incl_hist"]:
            hist_subpath = TEMPLATE_PATH_CLIMATE_CSV.format(
                gcm=self.setup["gcm"], rcm=self.setup["rcm"], scenario="historical",
                ensmem=self.setup["ensmem"], version=self.setup["version"], crow=crow, ccol=ccol,
            )
            env["pathToClimateCSV"].insert(0,
                                           f"{self.paths['monica-path-to-climate-dir'].rstrip('/')}/{climate_base}/{hist_subpath}")

        if "Elevation" in meta and not pd.isna(meta["Elevation"]):
            env["params"]["siteParameters"]["HeightNN"] = float(meta["Elevation"])
            env["params"]["siteParameters"]["heightNN"] = float(meta["Elevation"])
        env["params"]["siteParameters"]["Latitude"] = lat

        if self.setup["CO2"]:
            env["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = float(self.setup["CO2"])
        if self.setup["O3"]:
            env["params"]["userEnvironmentParameters"]["AtmosphericO3"] = float(self.setup["O3"])

        # Build worksteps
        # Sow once
        ws_template = copy.deepcopy(env["cropRotation"][0]["worksteps"])
        end_year = int(end_date[:4])
        ws_out = []

        for ws in ws_template:
            if ws["type"] == "Sowing":
                ws_copy = copy.deepcopy(ws)
                ws_copy["date"] = f"{start_year}-{ws_copy['date'][5:]}"
                ws_out.append(ws_copy)

        for year in range(start_year, end_year + 1):
            for ws in ws_template:
                if ws["type"] == "Cutting":
                    ws_copy = copy.deepcopy(ws)
                    ws_copy["date"] = f"{year}-{ws_copy['date'][5:]}"
                    ws_out.append(ws_copy)

        ws_out.sort(key=lambda ws: ws["date"])
        env["cropRotation"] = [{"worksteps": ws_out}]

        # Sow every year
        # ws_template = copy.deepcopy(env["cropRotation"][0]["worksteps"])
        # end_year = int(end_date[:4])
        # ws_out = []
        #
        # for year in range(start_year, end_year + 1):
        #     for ws in ws_template:
        #         ws_copy = copy.deepcopy(ws)
        #         if "date" in ws_copy and isinstance(ws_copy["date"], str) and len(ws_copy["date"]) >= 10:
        #             ws_copy["date"] = f"{year}-{ws_copy['date'][5:]}"
        #         ws_out.append(ws_copy)

        env["cropRotation"] = [{"worksteps": ws_out}]

        env["customId"] = {
            "setup_id": self.setup_id,
            "experiment": str(meta["Experiment"]),
            "soil_name": soil_name,
            "crow": crow,
            "ccol": ccol,
            "env_id": env_id,
            "nodata": False,
            "shared-id": self.shared_id,
        }
        return env

    def run_producer(self, params):
        sent_env_count = 0
        last_env = None
        start_setup_time = time.perf_counter()

        for meta in self.calib_points:
            exp_id = str(meta["Experiment"])
            if exp_id not in self.observations_by_exp_year:
                with open(self.path_to_prod_out_file, "a") as f:
                    f.write(f"Skipping {exp_id}: no observations\n")
                continue
            try:
                env = self._build_env_for_point(meta, params, sent_env_count)

                # if sent_env_count == 0:
                #     with open(f"{self.path_to_out}/first_env.json", "w") as f:
                #         json.dump(env, f, indent=2)
            except Exception as exc:
                with open(self.path_to_prod_out_file, "a") as f:
                    f.write(f"Skipping point {exp_id}: {exc}\n")
                continue

            self.prod_socket.send_json(env)
            sent_env_count += 1
            last_env = env
            print("sent point env", sent_env_count, "customId:", env["customId"])

        if sent_env_count > 0:
            last_env = copy.deepcopy(last_env)
            last_env["pathToClimateCSV"] = ""
            last_env["customId"] = {
                "no_of_sent_envs": sent_env_count,
                "nodata": True
            }
            self.prod_socket.send_json(last_env)

        with open(self.path_to_prod_out_file, "a") as f:
            f.write(
                f"{datetime.now()} sent {sent_env_count} point envs in {time.perf_counter() - start_setup_time:.2f}s\n")

    def run_consumer(self):
        simulated_by_exp_year = defaultdict(lambda: defaultdict(list))
        envs_received = 0
        no_of_envs_expected = None

        while True:
            try:
                msg = self.cons_socket.recv_json()
                custom_id = msg.get("customId", {})
                if "no_of_sent_envs" in custom_id:
                    no_of_envs_expected = custom_id["no_of_sent_envs"]
                else:
                    envs_received += 1
                    exp_id = str(custom_id.get("experiment"))
                    for data in msg.get("data", []):
                        for vals in data.get("results", []):
                            if "Year" in vals and "exportedCutBiomass" in vals:
                                simulated_by_exp_year[exp_id][int(vals["Year"])].append(vals["exportedCutBiomass"])

                if no_of_envs_expected == envs_received:
                    with open(self.path_to_cons_out_file, "a") as f:
                        f.write(f"{datetime.now()} last expected point env received\n")
                    return simulated_by_exp_year

            except zmq.error.Again:
                with open(self.path_to_cons_out_file, "a") as f:
                    f.write(f"no response from server after {self.cons_socket.RCVTIMEO} ms\n")
                print(f"no response from server after timeout={self.cons_socket.RCVTIMEO} ms")
                break
        return None

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector):
        params = dict(zip(vector.name, vector))
        self.run_producer(params)
        simulated_by_exp_year = self.run_consumer()
        if simulated_by_exp_year is None:
            return None
        sim_list = flatten_exp_year_dict(simulated_by_exp_year, self.experiment_order)
        print("len(sim_list):", len(sim_list), "== len(obs):", len(self.obs_flat_list), flush=True)
        with open(self.path_to_prod_out_file, "a") as f:
            f.write(f"{datetime.now()} len(sim_list): {len(sim_list)} == len(obs): {len(self.obs_flat_list)}\n")
        assert len(sim_list) == len(self.obs_flat_list)
        return sim_list if len(sim_list) > 0 else None

    def evaluation(self):
        return self.obs_flat_list

    def objectivefunction(self, simulation, evaluation, params=None):
        if simulation is None:
            raise RuntimeError("Simulation returned None.")
        return spotpy.objectivefunctions.rmse(evaluation, simulation)
