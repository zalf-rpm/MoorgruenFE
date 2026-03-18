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


def run_consumer(server=None, port=None):
    config = {
        "port": port if port else "7777",
        "server": server if server else "localhost",
        "path-to-output-dir": "./out",
    }
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = 6000

    path_to_out_dir = config["path-to-output-dir"]
    if not os.path.exists(path_to_out_dir):
        try:
            os.makedirs(path_to_out_dir)
        except OSError:
            print(f"{os.path.basename(__file__)} Couldn't create dir {path_to_out_dir}! Exiting.")
            exit(1)

    daily_filepath = f"{path_to_out_dir}/grassland_sim_output.csv"
    with open(daily_filepath, "wt", newline="", encoding="utf-8") as daily_f:
        daily_writer = csv.writer(daily_f, delimiter=",")

        cbal_header_row = [f"CBal_{i}" for i in range (1, 5)]
        soc_header_row = [f"SOC_{i}" for i in range(1, 21)]
        socxy_header_row = [f"SOC-X-Y_{i}" for i in range(1, 21)]
        daily_writer.writerow([
            "Exp",
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

        no_of_gr_ids_to_receive = None
        no_of_gr_ids_received = 0

        while no_of_gr_ids_to_receive is None or no_of_gr_ids_received < no_of_gr_ids_to_receive:
            try:
                # Receive message
                msg: dict = socket.recv_json()

                if msg.get("errors", []):
                    print(f"{os.path.basename(__file__)} received errors: {msg['errors']}")
                    continue

                custom_id = msg.get("customId", {})

                # Check if all grassland ids are received
                if custom_id.get("nodata", False):
                    no_of_gr_ids_to_receive = custom_id.get("no_of_exps", None)
                    continue

                exp_id = custom_id.get("experiment")

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

                # Process data from the message
                for i, data in enumerate(msg.get("data", [])):
                    results = data.get("results", [])

                    for vals in results:
                        cbal_data = vals.get("CBal", [])
                        soc_data = vals.get("SOC", [])
                        socxy_data = vals.get("SOC-X-Y", [])

                        row = [exp_id,
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
                        daily_writer.writerow(row)

                daily_f.flush()

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

    # with open("out/out-" + str(i) + ".csv", 'wb') as _:
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
