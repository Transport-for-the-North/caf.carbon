# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 12:24:08 2024

@author: Renewed
"""
import pandas as pd


class VKMEmissionsModel:
    """Calculate fleet emissions from fleet and demand data."""

    def __init__(self, parameters):
        for scenario in parameters.vkm_scenarios:
            self.scenario = scenario
            print(f"Running for Scenario {scenario}")
            self.emission_profiles = CreateSimpleProfiles(parameters, self.scenario)
            for year in parameters.run_list:
                for time_period in ["TS1", "TS2", "TS3"]:
                    print(f"Running {time_period} {year}")
                    print("This may take some time...")
                    self.first_enumeration = True
                    keys_to_enumerate = pd.HDFStore(
                        f"{parameters.vkm_demand}/{scenario}/{year}/vkm_by_speed_and_type_{year}_{time_period}_car.h5",
                        mode="r",
                    ).keys()
                    data_count = 0
                    for demand_key in keys_to_enumerate:
                        print(f"Processing demand for key {demand_key}", end="\r")
                        demand_data = Demand(
                            year, time_period, demand_key, self.scenario, parameters
                        )
                        print(f"Allocating emissions for key {demand_key}", end="\r")
                        AllocateEmissions(
                            demand_data.demand,
                            year,
                            time_period,
                            data_count,
                            self.scenario,
                            self.emission_profiles.scenario_profile,
                            self.first_enumeration,
                            parameters,
                        )
                        data_count = data_count + 1
                        if self.first_enumeration:
                            print("Enumeration changed")
                            self.first_enumeration = False


class CreateSimpleProfiles:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, parameters, scenario):
        self.run_fresh = parameters.run_fresh
        self.scenario = scenario
        self.grid_emissions_profiles = parameters.grid_profiles
        self.tail_pipe_emissions_profiles = parameters.tail_pipe_profiles
        self.index_fleet = parameters.index_fleet_path
        if self.run_fresh:

            def linear_interpolation(df1, df2, df1year, df2year, target_year):
                """Conduct the Interpolation"""
                df1["year"] = df1year
                df2["year"] = df2year
                df1 = df1[["speed_band", "vehicle_type", "total_gco2"]].rename(
                    columns={"total_gco2": "df1_gco2"}
                )
                df2 = df2[["speed_band", "vehicle_type", "total_gco2"]].rename(
                    columns={"total_gco2": "df2_gco2"}
                )
                target = df1.merge(df2, how="outer", on=["speed_band", "vehicle_type"])

                target["total_gco2"] = (df2year - target_year) * target["df1_gco2"]
                target["total_gco2"] = (
                    target["total_gco2"] + (target_year - df1year) * target["df2_gco2"]
                )
                target["total_gco2"] = -1 * target["total_gco2"] / (df1year - df2year)
                target["year"] = target_year
                target = target[["speed_band", "year", "vehicle_type", "total_gco2"]]
                return target

            grid_emissions = pd.read_csv(self.grid_emissions_profiles)
            tailpipe_emissions = pd.read_csv(self.tail_pipe_emissions_profiles)
            scenario_vkm_splits = pd.read_csv(
                rf"{self.vkm_demand}\lookup\{self.scenario}_vkm_splits.csv"
            )
            fleet = pd.read_csv(self.index_fleet)
            fleet = (
                fleet[["cohort", "vehicle_type", "segment", "tally"]]
                .groupby(["cohort", "vehicle_type", "segment"], as_index=False)
                .sum()
            )

            tailpipe_emissions = tailpipe_emissions.dropna()
            tailpipe_emissions = tailpipe_emissions[
                tailpipe_emissions["tailpipe_gco2"] < 1000000
            ]
            grid_emissions = fleet.merge(
                grid_emissions, how="inner", on=["cohort", "vehicle_type", "segment"]
            )
            grid_emissions = grid_emissions[grid_emissions["fuel"] == "bev"]
            grid_emissions["grid_gco2"] = grid_emissions["grid_gco2"] * grid_emissions["tally"]
            grid_emissions = (
                grid_emissions[["vehicle_type", "year", "tally", "grid_gco2"]]
                .groupby(["vehicle_type", "year"], as_index=False)
                .sum()
            )
            grid_emissions["grid_gco2"] = grid_emissions["grid_gco2"] / grid_emissions["tally"]
            grid_emissions = grid_emissions[["vehicle_type", "year", "grid_gco2"]]

            tailpipe_emissions = tailpipe_emissions.rename(columns={"e_cohort": "year"})
            tailpipe_emissions = tailpipe_emissions[
                ["fuel", "segment", "year", "speed_band", "vehicle_type", "tailpipe_gco2"]
            ]
            tailpipe_emissions = tailpipe_emissions[
                tailpipe_emissions["fuel"].isin(["petrol", "diesel"])
            ]

            fleet = (
                fleet[["vehicle_type", "segment", "tally"]]
                .groupby(["vehicle_type", "segment"], as_index=False)
                .sum()
            )
            tailpipe_emissions = fleet.merge(
                tailpipe_emissions, how="inner", on=["vehicle_type", "segment"]
            )
            tailpipe_emissions["tailpipe_gco2"] = (
                tailpipe_emissions["tailpipe_gco2"] * tailpipe_emissions["tally"]
            )
            tailpipe_emissions = (
                tailpipe_emissions[
                    ["fuel", "vehicle_type", "year", "tally", "speed_band", "tailpipe_gco2"]
                ]
                .groupby(["fuel", "vehicle_type", "speed_band", "year"], as_index=False)
                .sum()
            )
            tailpipe_emissions["tailpipe_gco2"] = (
                tailpipe_emissions["tailpipe_gco2"] / tailpipe_emissions["tally"]
            )
            tailpipe_emissions = tailpipe_emissions[
                ["speed_band", "fuel", "vehicle_type", "year", "tailpipe_gco2"]
            ]
            diesel_emissions = tailpipe_emissions[tailpipe_emissions["fuel"] == "diesel"]
            diesel_emissions = diesel_emissions.rename(
                columns={"tailpipe_gco2": "diesel_gco2"}
            )
            petrol_emissions = tailpipe_emissions[tailpipe_emissions["fuel"] == "petrol"]
            petrol_emissions = petrol_emissions.rename(
                columns={"tailpipe_gco2": "petrol_gco2"}
            )

            scenario_emissions = diesel_emissions.merge(
                scenario_vkm_splits, how="left", on=["year", "vehicle_type"]
            )
            scenario_emissions = scenario_emissions.merge(
                petrol_emissions, how="left", on=["year", "vehicle_type", "speed_band"]
            )
            scenario_emissions = scenario_emissions.fillna(0)
            scenario_emissions = scenario_emissions.merge(
                grid_emissions, how="left", on=["year", "vehicle_type"]
            )
            # scenario_emissions["total_gco2"] = scenario_emissions["petrol_gco2"] * scenario_emissions["petrol"]
            # scenario_emissions["total_gco2"] = scenario_emissions["total_gco2"] + (
            #         scenario_emissions["diesel_gco2"] * scenario_emissions["diesel"])
            scenario_emissions["total_gco2"] = 0
            scenario_emissions["total_gco2"] = (
                scenario_emissions["grid_gco2"] * scenario_emissions["electric"]
            )  # + scenario_emissions["total_gco2"]
            scenario_emissions = scenario_emissions[
                ["speed_band", "year", "vehicle_type", "total_gco2"]
            ]

            interpol_data_2019 = linear_interpolation(
                scenario_emissions[scenario_emissions["year"] == 2018],
                scenario_emissions[scenario_emissions["year"] == 2020],
                2018,
                2020,
                2019,
            )
            scenario_emissions = pd.concat([scenario_emissions, interpol_data_2019])
            for year in range(2021, 2025):
                interpol_data = linear_interpolation(
                    scenario_emissions[scenario_emissions["year"] == 2020],
                    scenario_emissions[scenario_emissions["year"] == 2025],
                    2020,
                    2025,
                    year,
                )
                scenario_emissions = pd.concat([scenario_emissions, interpol_data])
            for year in range(2026, 2030):
                interpol_data = linear_interpolation(
                    scenario_emissions[scenario_emissions["year"] == 2025],
                    scenario_emissions[scenario_emissions["year"] == 2030],
                    2025,
                    2030,
                    year,
                )
                scenario_emissions = pd.concat([scenario_emissions, interpol_data])
            for year in range(2031, 2035):
                interpol_data = linear_interpolation(
                    scenario_emissions[scenario_emissions["year"] == 2030],
                    scenario_emissions[scenario_emissions["year"] == 2035],
                    2030,
                    2035,
                    year,
                )
                scenario_emissions = pd.concat([scenario_emissions, interpol_data])
            for year in range(2036, 2040):
                interpol_data = linear_interpolation(
                    scenario_emissions[scenario_emissions["year"] == 2035],
                    scenario_emissions[scenario_emissions["year"] == 2040],
                    2035,
                    2040,
                    year,
                )
                scenario_emissions = pd.concat([scenario_emissions, interpol_data])
            for year in range(2041, 2045):
                interpol_data = linear_interpolation(
                    scenario_emissions[scenario_emissions["year"] == 2040],
                    scenario_emissions[scenario_emissions["year"] == 2045],
                    2040,
                    2045,
                    year,
                )
                scenario_emissions = pd.concat([scenario_emissions, interpol_data])
            for year in range(2046, 2050):
                interpol_data = linear_interpolation(
                    scenario_emissions[scenario_emissions["year"] == 2045],
                    scenario_emissions[scenario_emissions["year"] == 2050],
                    2045,
                    2050,
                    year,
                )
                scenario_emissions = pd.concat([scenario_emissions, interpol_data])
            scenario_co2_reductions = pd.read_csv(
                rf"{parameters.vkm_demand}\lookup\{self.scenario}_co2_management.csv"
            )
            scenario_co2_reductions["reduction"] = 1 - scenario_co2_reductions["reduction"]
            scenario_emissions = scenario_emissions.merge(
                scenario_co2_reductions, how="left", on=["year", "vehicle_type"]
            )
            scenario_emissions["total_gco2"] = (
                scenario_emissions["reduction"] * scenario_emissions["total_gco2"]
            )
            scenario_emissions = scenario_emissions.drop(columns=["reduction"])

            self.scenario_profile = scenario_emissions
            self.scenario_profile.to_csv(
                rf"{parameters.vkm_demand}\
                input\{self.scenario}_emissions_profiles.csv",
                index=False,
            )
        else:
            self.scenario_profile = pd.read_csv(
                rf"{parameters.vkm_demand}\input\{self.scenario}_emissions_profiles.csv"
            )


class AllocateEmissions:
    """Load in and preprocess scenario variant tables."""

    def __init__(
        self,
        demand,
        year,
        time_period,
        key,
        scenario,
        scenario_profile,
        first_enumeration,
        parameters,
    ):
        self.year = year
        self.scenario = scenario
        self.vkm_demand = parameters.vkm_demand
        self.scenario_profile = scenario_profile
        self.first_enumeration = first_enumeration
        self.time_period = time_period
        self.key = f"{self.time_period}_{self.year}_{key}"
        self.out_path = parameters.vkm_out_path
        self.demand = demand
        self.__assign_emissions()

    def __assign_emissions(self):
        print("Creating emissions", end="\r")
        scenario_vkm_reductions = pd.read_csv(
            rf"{self.vkm_demand}\lookup\{self.scenario}_vkm_management.csv"
        )
        scenario_vkm_reductions["reduction"] = 1 - scenario_vkm_reductions["reduction"]

        scenario_emissions_data = self.demand.merge(
            scenario_vkm_reductions, how="left", on=["year", "vehicle_type"]
        )
        scenario_emissions_data["vkm"] = (
            scenario_emissions_data["reduction"] * scenario_emissions_data["vkm"]
        )
        scenario_emissions_data = scenario_emissions_data.drop(columns=["reduction"])
        scenario_emissions_data = scenario_emissions_data.merge(
            self.scenario_profile, how="left", on=["speed_band", "year", "vehicle_type"]
        )
        scenario_emissions_data["total_gco2"] = (
            scenario_emissions_data["total_gco2"] * scenario_emissions_data["vkm"]
        )
        scenario_emissions_data = scenario_emissions_data[
            [
                "destination",
                "through",
                "origin",
                "user_class",
                "vehicle_type",
                "vkm",
                "total_gco2",
            ]
        ]  # "trip_band"
        scenario_emissions_data = scenario_emissions_data.groupby(
            ["destination", "through", "origin", "vehicle_type", "user_class"], as_index=False
        ).sum()  # "trip_band"

        print("Writing out", end="\r")
        if self.first_enumeration:
            scenario_emissions_data.to_hdf(
                f"{self.out_path}/{self.scenario}_emissions_{self.year}_{self.time_period}.h5",
                f"{self.key}",
                mode="w",
                complevel=1,
                format="table",
                index=False,
            )
            print("First enumerated")
        else:
            scenario_emissions_data.to_hdf(
                f"{self.out_path}/{self.scenario}_emissions_{self.year}_{self.time_period}.h5",
                f"{self.key}",
                mode="a",
                complevel=1,
                append=True,
                format="table",
                index=False,
            )


class Demand:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, demand_year, time_period, demand_key, scenario, parameters):
        self.year = demand_year
        self.vkm_demand = parameters.vkm_demand
        self.scenario = scenario
        self.key = demand_key
        self.time_period = time_period
        self.__merge_demand()

    def __merge_demand(self):
        """Concatenate car, van and HGV demand data."""
        demand = self.__load_demand("car")
        demand = pd.concat([demand, self.__load_demand("lgv")], ignore_index=True)
        self.demand = pd.concat([demand, self.__load_demand("hgv")], ignore_index=True)

    def __load_car_demand(self):
        """Load the car demand for a specified scenario."""
        demand = pd.read_hdf(
            f"{self.vkm_demand}/{self.scenario}/{self.year}/"
            f"vkm_by_speed_and_type_{self.year}_{self.time_period}_car.h5",
            self.key,
            mode="r",
        )
        return demand

    def __load_hgv_demand(self):
        """Load the hgv demand for a specified scenario."""
        hgv_demand = pd.read_hdf(
            f"{self.vkm_demand}/{self.scenario}/{self.year}/"
            f"vkm_by_speed_and_type_{self.year}_{self.time_period}_hgv.h5",
            self.key,
            mode="r",
        )
        hgv_demand["vkm_70-90_kph"] = (
            hgv_demand["vkm_90-110_kph"] + hgv_demand["vkm_70-90_kph"]
        )
        hgv_demand["vkm_90-110_kph"] = 0
        return hgv_demand

    def __load_lgv_demand(self):
        """Load the lgv demand for a specified scenario."""
        lgv_demand = pd.read_hdf(
            f"{self.vkm_demand}/{self.scenario}/{self.year}/"
            f"vkm_by_speed_and_type_{self.year}_{self.time_period}_lgv.h5",
            self.key,
            mode="r",
        )
        return lgv_demand

    def __load_demand(self, vehicle_type):
        """Load in and preprocess demand data for a given vehicle type."""
        new_cols = [
            "origin",
            "destination",
            "through",
            "user_class",
            "10_30",
            "30_50",
            "50_70",
            "70_90",
            "90_110",
            # "trip_band",
        ]
        original_cols = [
            "origin",
            "destination",
            "through",
            "user_class",
            "vkm_10-30_kph",
            "vkm_30-50_kph",
            "vkm_50-70_kph",
            "vkm_70-90_kph",
            "vkm_90-110_kph",
            # "trip_band",
        ]
        if vehicle_type == "car":
            demand = self.__load_car_demand()
        elif vehicle_type == "hgv":
            demand = self.__load_hgv_demand()
        else:
            demand = self.__load_lgv_demand()
        demand["vehicle_type"] = vehicle_type

        rename_cols = dict(zip(original_cols, new_cols))
        demand = demand.rename(columns=rename_cols)
        demand = demand[
            [
                "origin",
                "destination",
                "through",
                "vehicle_type",
                "user_class",
                "10_30",
                "30_50",
                "50_70",
                "70_90",
                "90_110",
                # "trip_band",
            ]
        ]
        demand = pd.melt(
            demand,
            id_vars=[
                "origin",
                "destination",
                "through",
                "vehicle_type",
                "user_class",
                # "trip_band",
            ],
            var_name="speed_band",
            value_name="vkm",
        )
        demand = demand[demand["vkm"] != 0]
        demand["year"] = self.year
        if self.time_period == "TS1":
            demand["vkm"] *= 3
        elif self.time_period == "TS2":
            demand["vkm"] *= 6
        elif self.time_period == "TS3":
            demand["vkm"] *= 3
        else:
            print("Warning: couldn't determine time period")

        if vehicle_type == "car":
            demand["vkm"] *= 348 * (1 + 0.238)
        elif vehicle_type == "hgv":
            demand["vkm"] *= 297 * (1 + 0.331) / 2.5
        else:
            demand["vkm"] *= 329 * (1 + 0.216)

        return demand
