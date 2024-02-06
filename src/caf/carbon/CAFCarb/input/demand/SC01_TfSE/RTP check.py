# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 12:32:32 2023

@author: Invincible
"""
import math

import pandas as pd

car_comparsion =  pd.read_csv("vkm_by_speed_and_type_2020_car.csv")

car_comparsion["Total"] =  car_comparsion["vkm_0-10_kph"] + car_comparsion["vkm_10-30_kph"] + car_comparsion["vkm_30-50_kph"] + car_comparsion["vkm_50-70_kph"] + car_comparsion["vkm_70-90_kph"] + car_comparsion["vkm_90-110_kph"] + car_comparsion["vkm_110-130_kph"]

car_total =  car_comparsion.sum()

car_comparsion_pre =  pd.read_csv("E:/GitHub/NorTMS-noham_to_nocarb/NoHAM/noham_to_nocarb/SE outputs/SE vkm/SE_19_Zone_Car.csv")

car_total_pre =  car_comparsion_pre.sum()

rtf_forecast = 7.36 * math.pow(10,10)