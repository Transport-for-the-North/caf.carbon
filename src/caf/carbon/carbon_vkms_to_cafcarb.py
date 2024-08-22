# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 14:42:35 2024

@author: Renewed
"""

import pandas as pd

year = 2043
input_folder = "A:/QCR- assignments/03.Assignments/h5files/outputs/VKMs-carbon_Northruns/BY_speed_banded_12-08-24/2043/" 
output_folder = "D:/GitHub/caf.carbon/CAFCarb/input/demand/TfN/detailed/SC05/"
tfn_zones = pd.read_csv(fr"D:\GitHub\caf.carbon\CAFCarb\lookup\stb_area_lookup.csv")
code_lookup = pd.read_csv(r"D:\GitHub\caf.carbon\CAFCarb\lookup\North_MSOA_filter.csv")
code_lookup = code_lookup[["zone_cd", "zone"]]


for time_period in ["TS1", "TS2", "TS3"]:
    car = pd.DataFrame()
    lgv = pd.DataFrame()
    hgv = pd.DataFrame()
    car_chunk = pd.DataFrame()
    
    for userclass in ["uc1", "uc2", "uc3"]:
        car_slice = pd.read_csv(input_folder + f"NoHAM_QCR_DM_Core_2043_{time_period}_v107_SatPig_{userclass}_aggregated-routes_through.csv")
        car_slice["user_class"] = userclass
        car_chunk = pd.concat([car_chunk, car_slice])
        
    lgv_chunk = pd.read_csv(input_folder +  f"NoHAM_QCR_DM_Core_2043_{time_period}_v107_SatPig_uc4_aggregated-routes_through.csv")
    lgv_chunk["user_class"] = "uc4"
    
    hgv_chunk = pd.read_csv(input_folder +  f"NoHAM_QCR_DM_Core_2043_{time_period}_v107_SatPig_uc5_aggregated-routes_through.csv")
    hgv_chunk["user_class"] = "uc5"
    
    car_chunk["vkm_90-110_kph"] = car_chunk["vkm_90-110_kph"] + car_chunk["vkm_110+_kph"]
    lgv_chunk["vkm_90-110_kph"] = lgv_chunk["vkm_90-110_kph"] + lgv_chunk["vkm_110+_kph"]
    hgv_chunk["vkm_90-110_kph"] = hgv_chunk["vkm_90-110_kph"] + hgv_chunk["vkm_110+_kph"]

    car_chunk = car_chunk[["origin", "destination", "through",  # "time_period",
                                "user_class", "total_through_vkms", "trip_band",
                                "vkm_0-30_kph",
                                "vkm_30-50_kph",
                                "vkm_50-70_kph",
                                "vkm_70-90_kph",
                                "vkm_90-110_kph"]]
    car_chunk = car_chunk.rename(columns={"vkm_0-30_kph": "vkm_10-30_kph"})

    lgv_chunk = lgv_chunk[["origin", "destination", "through",  # "time_period",
                                "user_class", "total_through_vkms", "trip_band",
                                "vkm_0-30_kph",
                                "vkm_30-50_kph",
                                "vkm_50-70_kph",
                                "vkm_70-90_kph",
                                "vkm_90-110_kph"]]
    lgv_chunk = lgv_chunk.rename(columns={"vkm_0-30_kph": "vkm_10-30_kph"})

    hgv_chunk = hgv_chunk[["origin", "destination", "through",  # "time_period",
                                "user_class", "total_through_vkms", "trip_band",
                                "vkm_0-30_kph",
                                "vkm_30-50_kph",
                                "vkm_50-70_kph",
                                "vkm_70-90_kph",
                                "vkm_90-110_kph"]]
    hgv_chunk = hgv_chunk.rename(columns={"vkm_0-30_kph": "vkm_10-30_kph"})

    car = pd.concat([car, car_chunk])
    lgv = pd.concat([lgv, lgv_chunk])
    hgv = pd.concat([hgv, hgv_chunk])

    car = car.groupby(["origin", "destination", "through", "trip_band"], as_index=False).sum()
    lgv = lgv.groupby(["origin", "destination", "through", "trip_band"], as_index=False).sum()
    hgv = hgv.groupby(["origin", "destination", "through", "trip_band"], as_index=False).sum()
    
    car = car.merge(code_lookup, how="left", left_on="origin", right_on="zone")
    car = car.drop(columns=["origin", "zone"])
    lgv = lgv.merge(code_lookup, how="left", left_on="origin", right_on="zone")
    lgv = lgv.drop(columns=["origin", "zone"])
    hgv = hgv.merge(code_lookup, how="left", left_on="origin", right_on="zone")
    hgv = hgv.drop(columns=["origin", "zone"])
    
    car = car.rename(columns={"zone_cd": "origin"})
    lgv = lgv.rename(columns={"zone_cd": "origin"})
    hgv = hgv.rename(columns={"zone_cd": "origin"})
    
    component_no, component_size = 0, 10
    unique_origins = lgv["origin"].unique()
    while component_no <= len(unique_origins):
        component_min = component_no
        if component_no + component_size > len(unique_origins):
            component_max = len(unique_origins)
        else:
            component_max = component_no + component_size
        car_component = car[car["origin"].isin(
            unique_origins[component_min:component_max])
            ]
        lgv_component = lgv[lgv["origin"].isin(
            unique_origins[component_min:component_max])
            ]
        hgv_component = hgv[hgv["origin"].isin(
            unique_origins[component_min:component_max])
            ]
        
        if component_no == 0:
            car_component.to_hdf(
                output_folder + f"vkm_by_speed_and_type_{year}_{time_period}_car.h5",
                f"{component_no}", mode='w', complevel=1, format="table",
                index=False,
            )
        else:
            car_component.to_hdf(
                output_folder + f"vkm_by_speed_and_type_{year}_{time_period}_car.h5",
                f"{component_no}", mode='a', complevel=1, append=True, format="table",
                index=False,
            )
        if component_no == 0:
            lgv_component.to_hdf(
                output_folder + f"vkm_by_speed_and_type_{year}_{time_period}_lgv.h5",
                f"{component_no}", mode='w', complevel=1, format="table",
                index=False,
            )
        else:
            lgv_component.to_hdf(
                output_folder + f"vkm_by_speed_and_type_{year}_{time_period}_lgv.h5",
                f"{component_no}", mode='a', complevel=1, append=True, format="table",
                index=False,
            )
        if component_no == 0:
            hgv_component.to_hdf(
                output_folder + f"vkm_by_speed_and_type_{year}_{time_period}_hgv.h5",
                f"{component_no}", mode='w', complevel=1, format="table",
                index=False,
            )
        else:
            hgv_component.to_hdf(
                output_folder + f"vkm_by_speed_and_type_{year}_{time_period}_hgv.h5",
                f"{component_no}", mode='a', complevel=1, append=True, format="table",
                index=False,
            )
        print(component_no)
        component_no = component_no + component_size
