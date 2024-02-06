# -*- coding: utf-8 -*-
"""
Created on Tue Nov  7 16:49:48 2023

@author: Invincible
"""

import pandas as pd

demand_am = pd.read_csv("vkm_by_speed_and_type_%s_%s_AM.csv" % ("2018", "car")).drop(["Year"], axis=1)
demand_pm = pd.read_csv("vkm_by_speed_and_type_%s_%s_PM.csv" % ("2018", "car")).drop(["Year"], axis=1)
demand_ip = pd.read_csv("vkm_by_speed_and_type_%s_%s_IP.csv" % ("2018", "car")).drop(["Year"], axis=1)

for vehicle_type in ["car", "lgv", "hgv"]:
    for year in ["2018","2020","2025","2030","2035","2040","2045","2050"]:
        demand_am = pd.read_csv("vkm_by_speed_and_type_%s_%s_AM.csv" % (year, vehicle_type))
        demand_pm = pd.read_csv("vkm_by_speed_and_type_%s_%s_PM.csv" % (year, vehicle_type))
        demand_ip = pd.read_csv("vkm_by_speed_and_type_%s_%s_IP.csv" % (year, vehicle_type))
        demand = pd.concat([demand_am, demand_ip, demand_pm], ignore_index=True)
        demand  = demand.groupby(["origin_zone", "road_type"], as_index=False).sum()
        demand.to_csv("vkm_by_speed_and_type_%s_%s.csv" % (year, vehicle_type))
        