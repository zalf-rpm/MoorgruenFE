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

import monica_run_lib
import numpy as np


def update_config(config, argv, print_config=False, allow_new_keys=False):
    if len(argv) > 1:
        for arg in argv[1:]:
            k, v = arg.split("=", maxsplit=1)
            if allow_new_keys or k in config:
                config[k] = v.lower() == "true" if v.lower() in ["true", "false"] else v
        if print_config:
            print(config)

def get_lat_0_lon_0_resolution_from_grid_metadata(metadata):
    lat_0 = float(metadata["yllcorner"]) \
                + (float(metadata["cellsize"]) * float(metadata["nrows"])) \
                - (float(metadata["cellsize"]) / 2.0)
    lon_0 = float(metadata["xllcorner"]) + (float(metadata["cellsize"]) / 2.0)
    resolution = float(metadata["cellsize"])
    return {"lat_0": lat_0, "lon_0": lon_0, "res": resolution}


def load_grid_cached(path_to_grid, val_type, print_path=False):
    if not hasattr(load_grid_cached, "cache"):
        load_grid_cached.cache = {}

    if path_to_grid in load_grid_cached.cache:
        return load_grid_cached.cache[path_to_grid]

    md, _ = monica_run_lib.read_header(path_to_grid)
    grid = np.loadtxt(path_to_grid, dtype=type, skiprows=len(md))
    print("read: ", path_to_grid)
    ll0r = get_lat_0_lon_0_resolution_from_grid_metadata(md)

    def col(lon):
        return int((lon - ll0r["lon_0"]) / ll0r["res"])

    def row(lat):
        return int((ll0r["lat_0"] - lat) / ll0r["res"])

    def value(lat, lon, return_no_data=False):
        c = col(lon)
        r = row(lat)
        if 0 <= r < md["nrows"] and 0 <= c < md["ncols"]:
            val = val_type(grid[r, c])
            if val != md["nodata_value"] or return_no_data:
                return val
        return None

    cache_entry = {
        "metadata": md, "grid": grid, "ll0r": ll0r,
        "col": lambda lon: col(lon),
        "row": lambda lat: row(lat),
        "value": lambda lat, lon, ret_no_data: value(lat, lon, ret_no_data)
    }
    load_grid_cached.cache[path_to_grid] = cache_entry
    return cache_entry

