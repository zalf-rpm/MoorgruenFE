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

    def write_rows_for_result(setup_id, exp_id, rows):
        setup_dir = os.path.join(path_to_out_dir, f"setup_{setup_id}")
        os.makedirs(setup_dir, exist_ok=True)

        filepath = os.path.join(setup_dir, f"{exp_id}.csv")
        file_exists = os.path.exists(filepath)

        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=",")

            if not file_exists:
                if not file_exists:
                    metrics = ["CutBiomass", "AbBiom", "SOC", "Mois", "N2O", "NLeach", "Eto", "Pot_ET"]
                    header = ["Exp", "setup_id", "groundwater", "scenario", "Crop", "Year"]
                    header += [f"{m}_06-15" for m in metrics]
                    header += [f"{m}_09-01" for m in metrics]
                    writer.writerow(header)

            writer.writerows(rows)

    no_of_gr_ids_to_receive = None
    no_of_gr_ids_received = 0

    metrics = ["CutBiomass", "AbBiom", "SOC", "Mois", "N2O", "NLeach", "Eto", "Pot_ET"]
    pending_rows = {}

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

            scenario = custom_id.get("scenario")

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

            # rows_to_write = []

            # Process data from the message
            for data in msg.get("data", []):
                for vals in data.get("results", []):
                    key = (
                        exp_id,
                        setup_id,
                        scenario,
                        vals.get("Crop"),
                        vals.get("Year"),
                    )

                    rec = pending_rows.setdefault(
                        key,
                        {
                            "Exp": exp_id,
                            "setup_id": setup_id,
                            "groundwater": "MINMAX",
                            "scenario": scenario,
                            "Crop": vals.get("Crop"),
                            "Year": vals.get("Year"),
                        },
                    )

                    for m in metrics:
                        k06 = f"{m}_06-15"
                        k09 = f"{m}_09-01"

                        v06 = vals.get(k06)
                        v09 = vals.get(k09)

                        if v06 is not None:
                            rec[k06] = v06
                        if v09 is not None:
                            rec[k09] = v09

        except zmq.Again:
            continue
        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception: {e}")

    rows_by_file = {}
    for rec in pending_rows.values():
        row = [
            rec.get("Exp"),
            rec.get("setup_id"),
            rec.get("groundwater"),
            rec.get("scenario"),
            rec.get("Crop"),
            rec.get("Year"),
        ]
        row.extend(rec.get(f"{m}_06-15") for m in metrics)
        row.extend(rec.get(f"{m}_09-01") for m in metrics)

        file_key = (rec.get("setup_id"), rec.get("Exp"))
        rows_by_file.setdefault(file_key, []).append(row)

    for (setup_id_, exp_id_), rows in rows_by_file.items():
        write_rows_for_result(setup_id_, exp_id_, rows)

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
