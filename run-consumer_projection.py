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

import csv
import os
import sys
import zmq
import shared

import monica_io3

PATHS = {
    "re-local-remote": {
        "path-to-output-dir": "./out"
    },
    "remoteConsumer-remoteMonica": {
        "path-to-output-dir": "/out/"
    }
}

def run_consumer(server=None, port=None):
    config = {
        "mode": "re-local-remote",
        "port": port if port else "7778",
        "server": server if server else "login01.cluster.zalf.de",
    }
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    paths = PATHS[config["mode"]]

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = 6000

    path_to_out_dir = paths["path-to-output-dir"]
    if not os.path.exists(path_to_out_dir):
        try:
            os.makedirs(path_to_out_dir)
        except OSError:
            print(f"{os.path.basename(__file__)} Couldn't create dir {path_to_out_dir}! Exiting.")
            exit(1)

    cbal_header_row = [f"CBal_{i}" for i in range (1, 5)]
    soc_header_row = [f"SOC_{i}" for i in range(1, 21)]
    # socxy_header_row = [f"SOC-X-Y_{i}" for i in range(1, 21)]
    socxy_header_row = [
        "SOC-X-Y_30cm",
        "SOC-X-Y_60cm",
        "SOC-X-Y_90cm",
    ]

    def write_rows_for_result(setup_id, exp_id, rows):
        setup_dir = os.path.join(path_to_out_dir, f"setup_{setup_id}")
        os.makedirs(setup_dir, exist_ok=True)

        filepath = os.path.join(setup_dir, f"{exp_id}.csv")
        file_exists = os.path.exists(filepath)

        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=",")

            if not file_exists:
                writer.writerow(["Exp",
                                 "setup_id",
                                 "gcm",
                                 "rcm",
                                 "scenario",
                                 "ensmem",
                                 "version",
                                 "Crop",
                                 "Date",
                                 "AbBiom",
                                 "RootBiomass",
                                 "NPP",
                                 "GPP",
                                 "Ra",
                                 "RaRoot",
                                 "RaLeaf",
                                 "RaShoot"
                                 ] + cbal_header_row + soc_header_row + socxy_header_row)

            writer.writerows(rows)

    no_of_gr_ids_to_receive = None
    no_of_gr_ids_received = 0

    while no_of_gr_ids_to_receive is None or no_of_gr_ids_received < no_of_gr_ids_to_receive:
        try:
            # Receive message
            msg: dict = socket.recv_json()

            if msg.get("errors", []):
                print(f"{os.path.basename(__file__)} received errors: {msg['errors']}")
                no_of_gr_ids_received += 1
                continue

            custom_id = msg.get("customId", {})

            # Check if all grassland ids are received
            if custom_id.get("nodata", False):
                no_of_gr_ids_to_receive = custom_id.get("no_of_exps", None)
                continue

            exp_id = custom_id.get("experiment")
            setup_id = custom_id.get("setup_id", "unknown")

            gcm = custom_id.get("gcm")
            rcm = custom_id.get("rcm")
            scenario = custom_id.get("scenario")
            ensmem = custom_id.get("ensmem")
            version = custom_id.get("version")

            if "data" not in msg or not msg["data"]:
                print(
                    f"{os.path.basename(__file__)} received message without data "
                    f"for experiment={exp_id}"
                )
                continue

            no_of_gr_ids_received += 1

            print(
                f"{os.path.basename(__file__)} received result experiment: {exp_id} "
            )

            rows_to_write = []

            # Process data from the message
            for i, data in enumerate(msg.get("data", [])):
                results = data.get("results", [])

                for vals in results:
                    cbal_data = vals.get("CBal", [])
                    soc_data = vals.get("SOC", [])
                    # socxy_data = vals.get("SOC-X-Y", [])
                    socxy_data = [
                        vals.get("SOC-X-Y_30cm"),
                        vals.get("SOC-X-Y_60cm"),
                        vals.get("SOC-X-Y_90cm"),
                    ]

                    row = [exp_id,
                           setup_id,
                           gcm,
                           rcm,
                           scenario,
                           ensmem,
                           version,
                           vals.get("Crop"),
                           vals.get("Date"),
                           vals.get("AbBiom"),
                           vals.get("RootBiomass"),
                           vals.get("NPP"),
                           vals.get("GPP"),
                           vals.get("Ra"),
                           vals.get("RaRoot"),
                           vals.get("RaLeaf"),
                           vals.get("RaShoot")
                           ] + cbal_data + soc_data + socxy_data
                    rows_to_write.append(row)

            write_rows_for_result(setup_id, exp_id, rows_to_write)

        except zmq.Again:
            continue
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception: {e}")

    print(f"{os.path.basename(__file__)} exiting run_consumer()")


def write_monica_out(exp_id, msg):
    path_to_out_dir = "out"
    if not os.path.exists(path_to_out_dir):
        try:
            os.makedirs(path_to_out_dir)
        except OSError:
            print("c: Couldn't create dir:", path_to_out_dir, "! Exiting.")
            exit(1)

    path_to_file = path_to_out_dir + "/gr_id-" + str(exp_id) + ".csv"
    with open(path_to_file, "w", newline='') as _:
        writer = csv.writer(_, delimiter=";")
        for data_ in msg.get("data", []):
            results = data_.get("results", [])
            orig_spec = data_.get("origSpec", "")
            output_ids = data_.get("outputIds", [])
            if len(results) > 0:
                writer.writerow([orig_spec.replace("\"", "")])
                for row in monica_io3.write_output_header_rows(output_ids,
                                                               include_header_row=True,
                                                               include_units_row=False,
                                                               include_time_agg=False):
                    writer.writerow(row)
                for row in monica_io3.write_output_obj(output_ids, results):
                    writer.writerow(row)
            writer.writerow([])
    print("wrote:", path_to_file)


if __name__ == "__main__":
    run_consumer()
