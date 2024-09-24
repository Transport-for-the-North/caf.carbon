# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 12:24:08 2024

@author: Renewed
"""
import pandas as pd

path = "D:\GitHub\caf.carbon\CAFCarb\input\demand\TfN\detailed\SC05"
code_lookup = pd.read_csv(
            r"A:\QCR- assignments\03.Assignments\h5files\Other Inputs\North_through_lookup-MSOA11_lta.csv")

run_list = [[2018, ["TS2", "TS3"]],
            [2028, ["TS1","TS2", "TS3"]],
            [2038, ["TS1","TS2", "TS3"]],
            [2043, ["TS1", "TS3"]],
            [2048, ["TS1","TS2", "TS3"]]]

class VKMEmissionsModel:
    """Calculate fleet emissions from fleet and demand data."""
    def __init__(self, run_list, run_fresh
    ):
        self.emission_profiles = CreateSimpleProfiles(run_fresh)
        for i in run_list:
            print(i)
            year = i[0]
            for time_period in i[1]:
                print(f"Running {time_period} {year}")
                self.first_enumeration = True
                keystoenum = pd.HDFStore(f"{path}/vkm_by_speed_and_type_{year}_{time_period}_car.h5", mode="r").keys()
                data_count = 0
                for demand_key in keystoenum:
                    print(f"Processing demand for key {demand_key}")
                    demand_data = Demand(year, time_period, demand_key)
                    print(f"Allocating emissions for key {demand_key}")
                    AllocateEmissions(demand_data.demand, self.emission_profiles, year, time_period, self.first_enumeration, data_count)
                    data_count = data_count + 1
                    if self.first_enumeration:
                        print("enumeration changed")
                        self.first_enumeration = False
                    

class CreateSimpleProfiles:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, run_fresh):
        self.run_fresh = run_fresh
        if self.run_fresh:
            
            def lininterpol(df1, df2, df1year, df2year, year):
                """ Conduct the Interpolation """
                df1["year"] = df1year
                df2["year"] = df2year
                df1 = df1[["speed_band", "vehicle_type", "total_gco2"]].rename(columns={"total_gco2": "df1_gco2"})
                df2 = df2[["speed_band", "vehicle_type", "total_gco2"]].rename(columns={"total_gco2": "df2_gco2"})
                target = df1.merge(df2, how="outer", on=["speed_band", "vehicle_type"])
              
                target["total_gco2"] = (df2year - year) * target["df1_gco2"]
                target["total_gco2"] =  target["total_gco2"] + (year - df1year) * target["df2_gco2"]
                target["total_gco2"] = -1 *  target["total_gco2"] / (df1year - df2year)
                target["year"] = year
                target = target[["speed_band", "year", "vehicle_type", "total_gco2"]]
                return target
            
            grid_emissions = pd.read_csv(r"D:\GitHub\caf.carbon\src\caf\carbon\grid_emissions_profile.csv")
            tailpipe_emissions = pd.read_csv(r"D:\GitHub\carbon_fleet_tool\emissions profile.csv").drop(columns={"Unnamed: 0"})
            bau_vkm_splits = pd.read_csv(r"A:\caf.carbon\CAFCarb data inputs\lookup\bau_vkm_splits.csv")
            ue_vkm_splits = pd.read_csv(r"A:\caf.carbon\CAFCarb data inputs\lookup\ue_vkm_splits.csv")
            
            fleet = pd.read_csv(r"D:\GitHub\caf.carbon\CAFCarb\outputs\audit\index_fleet.csv")
            fleet = fleet[["cohort", "vehicle_type", "segment", "tally"]].groupby(["cohort", "vehicle_type", "segment"], as_index=False).sum()
            
            tailpipe_emissions = tailpipe_emissions.dropna()
            tailpipe_emissions = tailpipe_emissions[tailpipe_emissions["tailpipe_gco2"] < 1000000]
            grid_emissions = grid_emissions[grid_emissions["year"] > 2017]
            tailpipe_emissions = tailpipe_emissions[tailpipe_emissions["year"] > 2017]
            bau_vkm_splits = bau_vkm_splits[bau_vkm_splits["year"] > 2017]
            ue_vkm_splits = ue_vkm_splits[ue_vkm_splits["year"] > 2017]
            
            grid_emissions = grid_emissions[grid_emissions["year"] < 2051]
            tailpipe_emissions = tailpipe_emissions[tailpipe_emissions["year"] < 2051]
            bau_vkm_splits = bau_vkm_splits[bau_vkm_splits["year"] < 2051]
            ue_vkm_splits = ue_vkm_splits[ue_vkm_splits["year"] < 2051]
            
            
            grid_emissions = fleet.merge(grid_emissions, how="inner", on=["cohort", "vehicle_type", "segment"])
            grid_emissions = grid_emissions[grid_emissions["fuel"]=="bev"]
            grid_emissions["grid_gco2"] = grid_emissions["grid_gco2"] * grid_emissions["tally"]
            grid_emissions = grid_emissions[["vehicle_type", "year", "tally",
                                              "grid_gco2"]].groupby(["vehicle_type"
                                                                    , "year"], as_index=False).sum()
            grid_emissions["grid_gco2"] = grid_emissions["grid_gco2"] / grid_emissions["tally"]
            grid_emissions = grid_emissions[["vehicle_type", "year", "grid_gco2"]]   
            
            tailpipe_emissions = tailpipe_emissions.rename(columns={"e_cohort": "year"})
            tailpipe_emissions = tailpipe_emissions[["fuel", "segment", "year", "speed_band", "vehicle_type", "tailpipe_gco2"]]
            tailpipe_emissions = tailpipe_emissions[tailpipe_emissions["fuel"].isin(["petrol", "diesel"])]
            
            fleet = fleet[["vehicle_type", "segment", "tally"]].groupby(["vehicle_type", "segment"], as_index=False).sum()
            tailpipe_emissions = fleet.merge(tailpipe_emissions, how="inner", on=["vehicle_type", "segment"])
            tailpipe_emissions["tailpipe_gco2"] = tailpipe_emissions["tailpipe_gco2"] * tailpipe_emissions["tally"]
            tailpipe_emissions = tailpipe_emissions[["fuel", "vehicle_type", "year", "tally", "speed_band",
                                              "tailpipe_gco2"]].groupby(["fuel", "vehicle_type", "speed_band"
                                                                    , "year"], as_index=False).sum()
            tailpipe_emissions["tailpipe_gco2"] = tailpipe_emissions["tailpipe_gco2"] / tailpipe_emissions["tally"]
            tailpipe_emissions = tailpipe_emissions[["speed_band", "fuel", "vehicle_type", "year", "tailpipe_gco2"]]
            diesel_emissions = tailpipe_emissions[tailpipe_emissions["fuel"]=="diesel"]
            diesel_emissions = diesel_emissions.rename(columns={"tailpipe_gco2": "diesel_gco2"})
            petrol_emissions = tailpipe_emissions[tailpipe_emissions["fuel"]=="petrol"]
            petrol_emissions = petrol_emissions.rename(columns={"tailpipe_gco2": "petrol_gco2"})
            
            ue_emissions = diesel_emissions.merge(ue_vkm_splits, how="left", on=["year", "vehicle_type"])
            ue_emissions = ue_emissions.merge(petrol_emissions, how="left", on=["year", "vehicle_type", "speed_band"])
            ue_emissions = ue_emissions.fillna(0)
            ue_emissions = ue_emissions.merge(grid_emissions, how="left",  on=["year", "vehicle_type"])
            ue_emissions["total_gco2"] = ue_emissions["petrol_gco2"] * ue_emissions["petrol"]
            ue_emissions["total_gco2"] = ue_emissions["total_gco2"] + (ue_emissions["diesel_gco2"] * ue_emissions["diesel"])
            ue_emissions["total_gco2"] = ue_emissions["total_gco2"] + (ue_emissions["grid_gco2"] * ue_emissions["electric"])
            ue_emissions = ue_emissions[["speed_band", "year", "vehicle_type", "total_gco2"]]
            
            
            bau_emissions = diesel_emissions.merge(bau_vkm_splits, how="left", on=["year", "vehicle_type"])
            bau_emissions = bau_emissions.merge(petrol_emissions, how="left", on=["year", "vehicle_type", "speed_band"])
            bau_emissions = bau_emissions.fillna(0)
            bau_emissions = bau_emissions.merge(grid_emissions, how="left",  on=["year", "vehicle_type"])
            bau_emissions["total_gco2"] = bau_emissions["petrol_gco2"] * bau_emissions["petrol"]
            bau_emissions["total_gco2"] = bau_emissions["total_gco2"] + (bau_emissions["diesel_gco2"] * bau_emissions["diesel"])
            bau_emissions["total_gco2"] = bau_emissions["total_gco2"] + (bau_emissions["grid_gco2"] * bau_emissions["electric"])
            bau_emissions = bau_emissions[["speed_band", "year", "vehicle_type", "total_gco2"]]

            interpol_data_2019 = lininterpol(bau_emissions[bau_emissions["year"]==2018], bau_emissions[bau_emissions["year"]==2020], 2018, 2020, 2019)
            bau_emissions = pd.concat([bau_emissions, interpol_data_2019])
            for year in range(2021, 2025):
                interpol_data = lininterpol(bau_emissions[bau_emissions["year"]==2020], bau_emissions[bau_emissions["year"]==2025], 2020, 2025, year)
                bau_emissions = pd.concat([bau_emissions, interpol_data])
            for year in range(2026, 2030):
                interpol_data = lininterpol(bau_emissions[bau_emissions["year"]==2025], bau_emissions[bau_emissions["year"]==2030], 2025, 2030, year)
                bau_emissions = pd.concat([bau_emissions, interpol_data])
            for year in range(2031, 2035):
                interpol_data = lininterpol(bau_emissions[bau_emissions["year"]==2030], bau_emissions[bau_emissions["year"]==2035], 2030, 2035, year)
                bau_emissions = pd.concat([bau_emissions, interpol_data])
            for year in range(2036, 2040):
                interpol_data = lininterpol(bau_emissions[bau_emissions["year"]==2035], bau_emissions[bau_emissions["year"]==2040], 2035, 2040, year)
                bau_emissions = pd.concat([bau_emissions, interpol_data])
            for year in range(2041, 2045):
                interpol_data = lininterpol(bau_emissions[bau_emissions["year"]==2040], bau_emissions[bau_emissions["year"]==2045], 2040, 2045, year)
                bau_emissions = pd.concat([bau_emissions, interpol_data])
            for year in range(2046, 2050):
                interpol_data = lininterpol(bau_emissions[bau_emissions["year"]==2045], bau_emissions[bau_emissions["year"]==2050], 2045, 2050, year)
                bau_emissions = pd.concat([bau_emissions, interpol_data])
                
            for year in range(2019, 2020):
                interpol_data = lininterpol(ue_emissions[ue_emissions["year"]==2018], ue_emissions[ue_emissions["year"]==2020], 2018, 2020, year)
                ue_emissions = pd.concat([ue_emissions, interpol_data])
            for year in range(2021, 2025):
                interpol_data = lininterpol(ue_emissions[ue_emissions["year"]==2020], ue_emissions[ue_emissions["year"]==2025], 2020, 2025, year)
                ue_emissions = pd.concat([ue_emissions, interpol_data])
            for year in range(2026, 2030):
                interpol_data = lininterpol(ue_emissions[ue_emissions["year"]==2025], ue_emissions[ue_emissions["year"]==2030], 2025, 2030, year)
                ue_emissions = pd.concat([ue_emissions, interpol_data])
            for year in range(2031, 2035):
                interpol_data = lininterpol(ue_emissions[ue_emissions["year"]==2030], ue_emissions[ue_emissions["year"]==2035], 2030, 2035, year)
                ue_emissions = pd.concat([ue_emissions, interpol_data])
            for year in range(2036, 2040):
                interpol_data = lininterpol(ue_emissions[ue_emissions["year"]==2035], ue_emissions[ue_emissions["year"]==2040], 2035, 2040, year)
                ue_emissions = pd.concat([ue_emissions, interpol_data])
            for year in range(2041, 2045):
                interpol_data = lininterpol(ue_emissions[ue_emissions["year"]==2040], ue_emissions[ue_emissions["year"]==2045], 2040, 2045, year)
                ue_emissions = pd.concat([ue_emissions, interpol_data])
            for year in range(2046, 2050):
                interpol_data = lininterpol(ue_emissions[ue_emissions["year"]==2045], ue_emissions[ue_emissions["year"]==2050], 2045, 2050, year)
                ue_emissions = pd.concat([ue_emissions, interpol_data])
              
            
            self.bau_profile = bau_emissions
            self.ue_profile = ue_emissions
            
            self.bau_profile.to_csv(r"A:\caf.carbon\CAFCarb data inputs\input\bau_simplified_emissions_profiles.csv")
            self.ue_profile.to_csv(r"A:\caf.carbon\CAFCarb data inputs\input\ue_simplified_emissions_profiles.csv")
            
                        
        else:
            self.bau_profile = pd.read_csv(r"A:\caf.carbon\CAFCarb data inputs\input\bau_simplified_emissions_profiles.csv")
            self.ue_profile = pd.read_csv(r"A:\caf.carbon\CAFCarb data inputs\input\ue_simplified_emissions_profiles.csv")
            
            

class AllocateEmissions:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, demand, profiles, year, time_period, enumeration, key):
        self.year = year
        self.time_period = time_period
        self.key = f"{self.time_period}_{self.year}_{key}"
        self.bau_profile = profiles.bau_profile
        self.ue_profile = profiles.ue_profile
        self.first_enumeration = enumeration
        self.outpath = r"D:\GitHub\caf.carbon\CAFCarb\outputs"
        self.demand = demand
        self.__assign_emissions()


    def __assign_emissions(self):
        print("creating emissions")
        bau_emissions_data = self.demand.merge(self.bau_profile, how="left", on=["speed_band", "year", "vehicle_type"])
        bau_emissions_data["total_gco2"] = bau_emissions_data["total_gco2"] * bau_emissions_data["vkm"]
        bau_emissions_data = bau_emissions_data[["destination", "through", "origin", "trip_band", "user_class", "vehicle_type", "total_gco2"]]#.fillna(-1)
        bau_emissions_data = bau_emissions_data.groupby(["destination", "through", "origin", "trip_band", "vehicle_type", "user_class"], as_index=False).sum()
        
        ue_emissions_data = self.demand.merge(self.ue_profile, how="left", on=["speed_band", "year", "vehicle_type"])
        ue_emissions_data["total_gco2"] = ue_emissions_data["total_gco2"] * ue_emissions_data["vkm"]
        ue_emissions_data = ue_emissions_data[["destination", "through", "origin", "trip_band", "vehicle_type", "user_class", "total_gco2"]]#.fillna(-1)
        ue_emissions_data = ue_emissions_data.groupby(["destination", "through", "origin", "trip_band", "vehicle_type", "user_class"], as_index=False).sum()
        

        print("writing out")
        if self.first_enumeration:
            bau_emissions_data.to_hdf(
                f"{self.outpath}/BAU_fleet_emissions_{self.year}_{self.time_period}.h5",
                f"{self.key}", mode='w', complevel=1, format="table",
                index=False,
            )
            print("First enumerated")
        else:
            bau_emissions_data.to_hdf(
                f"{self.outpath}/BAU_fleet_emissions_{self.year}_{self.time_period}.h5",
                f"{self.key}", mode='a', complevel=1, append=True,
                format="table",
                index=False,
            )
        
        if self.first_enumeration:
                ue_emissions_data.to_hdf(
                    f"{self.outpath}/UE_fleet_emissions_{self.year}_{self.time_period}.h5",
                    f"{self.key}", mode='w', complevel=1, format="table",
                    index=False,
                )
                print("First enumerated")
                self.first_enumeration = False
        else:
            ue_emissions_data.to_hdf(
                f"{self.outpath}/UE_fleet_emissions_{self.year}_{self.time_period}.h5",
                f"{self.key}", mode='a', complevel=1, append=True,
                format="table",
                index=False,
            )


class Demand:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, demand_year, time_period, demand_key):
        self.year = demand_year
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
        demand = pd.read_hdf(f"{path}/vkm_by_speed_and_type_{self.year}_{self.time_period}_car.h5", self.key,
                             mode="r")
        car_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return car_demand

    def __load_hgv_demand(self):
        """Load the hgv demand for a specified scenario."""
        demand = pd.read_hdf(f"{path}/vkm_by_speed_and_type_{self.year}_{self.time_period}_hgv.h5", self.key,
                             mode="r")
        hgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        hgv_demand["vkm_70-90_kph"] = hgv_demand["vkm_90-110_kph"] + hgv_demand["vkm_70-90_kph"]
        hgv_demand["vkm_90-110_kph"] = 0
        return hgv_demand

    def __load_lgv_demand(self):
        """Load the lgv demand for a specified scenario."""
        demand = pd.read_hdf(f"{path}/vkm_by_speed_and_type_{self.year}_{self.time_period}_lgv.h5", self.key,
                             mode="r")
        lgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
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
            "trip_band",
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
            "trip_band",
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
                "trip_band",
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
                "trip_band",
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
            demand["vkm"] *= 348*(1 + 0.238)
        elif vehicle_type == "hgv":
            demand["vkm"] *= 297*(1 + 0.331)/ 2.5
        else:
            demand["vkm"] *= 329*(1 + 0.216)

        # demand = demand[demand.through != -1]
        # demand = demand[demand.through != 22]
        return demand


VKMEmissionsModel(run_list, False)