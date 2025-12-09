# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 14:42:35 2024

@author: Renewed
"""

import pandas as pd


class VKMPreprocessing:

    def __init__(self, year, parameters):
        """Initialise functions and set class variables."""
        self.year = year
        self.trip_bands = [2, 5, 10, 20, 50]
        self.code_lookup = self.code_lookup[["zone_cd", "zone"]]
        self.__generate_inputs(parameters)

    def __generate_inputs(self, parameters):

        def apply_trip_bands(dataframe):
            S = len(self.trip_bands) - 1
            dataframe.loc[dataframe["route_distance"] <= self.trip_bands[0], "trip_band"] = (
                f"0-{self.trip_bands[0]}km"
            )
            for boundary in range(0, S):
                dataframe.loc[
                    (dataframe["route_distance"] > self.trip_bands[boundary])
                    & (dataframe["route_distance"] <= self.trip_bands[boundary + 1]),
                    "trip_band",
                ] = f"{self.trip_bands[boundary]}-{self.trip_bands[boundary + 1]}km"
            dataframe.loc[dataframe["route_distance"] > self.trip_bands[S], "trip_band"] = (
                f"{self.trip_bands[S]}+km"
            )
            return dataframe

        def rename_speed_bands(data_frame):
            print(data_frame.columns)
            if "vkm_30-50_kph" not in data_frame.columns:
                data_frame = data_frame.rename(
                    columns={
                        "0-10": "vkm_0-10_kph",
                        "10-30": "vkm_0-30_kph",
                        "110+": "vkm_110+_kph",
                        "30-50": "vkm_30-50_kph",
                        "50-70": "vkm_50-70_kph",
                        "70-90": "vkm_70-90_kph",
                        "90-110": "vkm_90-110_kph",
                    }
                )
            print(data_frame.columns)
            if "vkm_0-10_kph" in data_frame.columns:
                data_frame["vkm_0-30_kph"] = (
                    data_frame["vkm_0-10_kph"] + data_frame["vkm_0-30_kph"]
                )
            if "vkm_110+_kph" in data_frame.columns:
                data_frame["vkm_90-110_kph"] = (
                    data_frame["vkm_90-110_kph"] + data_frame["vkm_110+_kph"]
                )
            return data_frame

        for time_period in ["TS1", "TS2", "TS3"]:
            car = pd.DataFrame()
            lgv = pd.DataFrame()
            hgv = pd.DataFrame()
            car_chunk = pd.DataFrame()

            for userclass in ["uc1", "uc2", "uc3"]:
                car_slice = pd.read_csv(
                    parameters.vkm_input_folder
                    + f"{self.year}/NoHAM_QCR_DM_Core_2043_{time_period}_v107_SatPig_{userclass}_aggregated-routes_through.csv"
                )
                car_slice["user_class"] = userclass
                car_chunk = pd.concat([car_chunk, car_slice])

            lgv_chunk = pd.read_csv(
                parameters.vkm_input_folder
                + f"{self.year}/NoHAM_QCR_DM_Core_2043_{time_period}_v107_SatPig_uc4_aggregated-routes_through.csv"
            )
            lgv_chunk["user_class"] = "uc4"

            hgv_chunk = pd.read_csv(
                parameters.vkm_input_folder
                + f"{self.year}/NoHAM_QCR_DM_Core_2043_{time_period}_v107_SatPig_uc5_aggregated-routes_through.csv"
            )
            hgv_chunk["user_class"] = "uc5"

            car_chunk = rename_speed_bands(car_chunk)
            lgv_chunk = rename_speed_bands(lgv_chunk)
            hgv_chunk = rename_speed_bands(hgv_chunk)

            car_chunk = apply_trip_bands(car_chunk)
            lgv_chunk = apply_trip_bands(lgv_chunk)
            hgv_chunk = apply_trip_bands(hgv_chunk)

            car_chunk = car_chunk[
                [
                    "origin",
                    "destination",
                    "through",
                    "user_class",
                    "trip_band",
                    "vkm_0-30_kph",
                    "vkm_30-50_kph",
                    "vkm_50-70_kph",
                    "vkm_70-90_kph",
                    "vkm_90-110_kph",
                ]
            ]
            car_chunk = car_chunk.rename(columns={"vkm_0-30_kph": "vkm_10-30_kph"})

            lgv_chunk = lgv_chunk[
                [
                    "origin",
                    "destination",
                    "through",
                    "user_class",
                    "trip_band",
                    "vkm_0-30_kph",
                    "vkm_30-50_kph",
                    "vkm_50-70_kph",
                    "vkm_70-90_kph",
                    "vkm_90-110_kph",
                ]
            ]
            lgv_chunk = lgv_chunk.rename(columns={"vkm_0-30_kph": "vkm_10-30_kph"})

            hgv_chunk = hgv_chunk[
                [
                    "origin",
                    "destination",
                    "through",
                    "user_class",
                    "trip_band",
                    "vkm_0-30_kph",
                    "vkm_30-50_kph",
                    "vkm_50-70_kph",
                    "vkm_70-90_kph",
                    "vkm_90-110_kph",
                ]
            ]
            hgv_chunk = hgv_chunk.rename(columns={"vkm_0-30_kph": "vkm_10-30_kph"})

            car = pd.concat([car, car_chunk])
            lgv = pd.concat([lgv, lgv_chunk])
            hgv = pd.concat([hgv, hgv_chunk])

            car = car.groupby(
                ["origin", "destination", "through", "trip_band", "user_class"], as_index=False
            ).sum()
            lgv = lgv.groupby(
                ["origin", "destination", "through", "trip_band", "user_class"], as_index=False
            ).sum()
            hgv = hgv.groupby(
                ["origin", "destination", "through", "trip_band", "user_class"], as_index=False
            ).sum()

            car = car.merge(
                parameters.code_lookup, how="left", left_on="origin", right_on="zone"
            )
            car = car.drop(columns=["origin", "zone"])
            lgv = lgv.merge(
                parameters.code_lookup, how="left", left_on="origin", right_on="zone"
            )
            lgv = lgv.drop(columns=["origin", "zone"])
            hgv = hgv.merge(
                parameters.code_lookup, how="left", left_on="origin", right_on="zone"
            )
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
                car_component = car[
                    car["origin"].isin(unique_origins[component_min:component_max])
                ]
                lgv_component = lgv[
                    lgv["origin"].isin(unique_origins[component_min:component_max])
                ]
                hgv_component = hgv[
                    hgv["origin"].isin(unique_origins[component_min:component_max])
                ]

                if component_no == 0:
                    car_component.to_hdf(
                        parameters.vkm_demand
                        + f"vkm_by_speed_and_type_{self.year}_{time_period}_car.h5",
                        f"{component_no}",
                        mode="w",
                        complevel=1,
                        format="table",
                        index=False,
                    )
                else:
                    car_component.to_hdf(
                        parameters.vkm_demand
                        + f"vkm_by_speed_and_type_{self.year}_{time_period}_car.h5",
                        f"{component_no}",
                        mode="a",
                        complevel=1,
                        append=True,
                        format="table",
                        index=False,
                    )
                if component_no == 0:
                    lgv_component.to_hdf(
                        parameters.vkm_demand
                        + f"vkm_by_speed_and_type_{self.year}_{time_period}_lgv.h5",
                        f"{component_no}",
                        mode="w",
                        complevel=1,
                        format="table",
                        index=False,
                    )
                else:
                    lgv_component.to_hdf(
                        parameters.vkm_demand
                        + f"vkm_by_speed_and_type_{self.year}_{time_period}_lgv.h5",
                        f"{component_no}",
                        mode="a",
                        complevel=1,
                        append=True,
                        format="table",
                        index=False,
                    )
                if component_no == 0:
                    hgv_component.to_hdf(
                        parameters.vkm_demand
                        + f"vkm_by_speed_and_type_{self.year}_{time_period}_hgv.h5",
                        f"{component_no}",
                        mode="w",
                        complevel=1,
                        format="table",
                        index=False,
                    )
                else:
                    hgv_component.to_hdf(
                        parameters.vkm_demand
                        + f"vkm_by_speed_and_type_{self.year}_{time_period}_hgv.h5",
                        f"{component_no}",
                        mode="a",
                        complevel=1,
                        append=True,
                        format="table",
                        index=False,
                    )
                print(component_no)
                component_no = component_no + component_size
