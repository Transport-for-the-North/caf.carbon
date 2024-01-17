# Third Party
import pandas as pd


class Demand:
    """Predict emissions using emissions, demand and projected fleet data."""

    def __init__(self, config, time, time_period, invariant_obj, scenario_obj, ev_redistribution, outpath):
        """Initialise functions and set class variables.

        Parameters
        ----------
        config : configparser
            Filepath lookup to import tables.
        invariant_obj : class obj
            Includes the baseline fleet and scenario invariant tables.
        scenario_obj : class obj
            Includes scenario tables.
        outpath : str
            Filepath to export preprocessed tables.
        """

        self.years = ["2018"]
        self.link_lookup = pd.read_csv(r"E:\GitHub\caf.carbon\src\caf\carbon\Fleet Emissions Tool\2018\noham2018_lad21.csv")

        for year in self.years:
            self.year_demand = self.process_demand(year)


    def process_demand(self, year):
        year_demand_ts1 = pd.read_csv(
            r"E:\GitHub\caf.carbon\src\caf\carbon\Fleet Emissions Tool\%s\link_table_NoHAM_%s_TS1_v106_I6.csv" % (year, year))
        year_demand_ts2 = pd.read_csv(
            r"E:\GitHub\caf.carbon\src\caf\carbon\Fleet Emissions Tool\%s\link_table_NoHAM_%s_TS2_v106_I6.csv" % (year, year))
        year_demand_ts3 = pd.read_csv(
            r"E:\GitHub\caf.carbon\src\caf\carbon\Fleet Emissions Tool\%s\link_table_NoHAM_%s_TS3_v106_I6.csv" % (year, year))

        year_demand = pd.concat([year_demand_ts1, year_demand_ts2, year_demand_ts3], ignore_index=True)

        year_demand = year_demand.merge(self.link_lookup, how="left", on=["A", "B"])
        year_demand = year_demand[["A", "B", "Distance", "Flow_UC1", "Flow_UC2", "Flow_UC3", "Flow_UC4",
             "Flow_UC5", "Speed_UC1", "Speed_UC2", "Speed_UC3", "Speed_UC4", "Speed_UC5", "lad21_id",
                                             "factor"]]
        for flow in ["Flow_UC1", "Flow_UC2", "Flow_UC3", "Flow_UC4", "Flow_UC5"]:
            year_demand[flow] = year_demand[flow] * year_demand["factor"]

        year_demand["car_flow"] = year_demand["Flow_UC1"] + year_demand["Flow_UC2"] + year_demand["Flow_UC3"]
        year_demand["car_speed"] = ((year_demand["Speed_UC1"] * year_demand["Flow_UC1"]) + (year_demand["Speed_UC2"] * year_demand["Flow_UC2"]) + (year_demand["Speed_UC3"] * year_demand["Flow_UC3"]))/year_demand["car_flow"]
        year_demand = year_demand.rename(columns={"Flow_UC4": "lgv_flow", "Flow_UC5": "hgv_flow",
                                                            "Speed_UC4": "lgv_speed", "Speed_UC5": "hgv_speed"})

        for vehicle in ["car", "lgv", "hgv"]:
            year_demand.loc[year_demand["%s_speed" % vehicle] < 30, "%s_speed_category" % vehicle] = "10_30"
            year_demand.loc[((year_demand["%s_speed" % vehicle] >= 30) & (
                    year_demand["%s_speed" % vehicle] < 50)), "%s_speed_category" % vehicle] = "30_50"
            year_demand.loc[((year_demand["%s_speed" % vehicle] >= 50) & (
                    year_demand["%s_speed" % vehicle] < 70)), "%s_speed_category" % vehicle] = "50_70"
            year_demand.loc[((year_demand["%s_speed" % vehicle] >= 70) & (
                    year_demand["%s_speed" % vehicle] < 90)), "%s_speed_category" % vehicle] = "70_90"
            year_demand.loc[((year_demand["%s_speed" % vehicle] >= 90) & (
                    year_demand["%s_speed" % vehicle] < 110)), "%s_speed_category" % vehicle] = "90_110"
            year_demand.loc[year_demand["%s_speed" % vehicle] >= 130, "%s_speed_category" % vehicle] = "110_130"

        year_demand_car = year_demand[["A", "B", "Distance", "car_flow", "car_speed_category", "lad21_id"]]
        year_demand_lgv = year_demand[["A", "B", "Distance", "lgv_flow", "lgv_speed_category", "lad21_id"]]
        year_demand_hgv = year_demand[["A", "B", "Distance", "hgv_flow", "hgv_speed_category", "lad21_id"]]

        year_demand_car["car_vkm"] = year_demand_car["distance"] * year_demand_car["car_flow"]
        year_demand_lgv["lgv_vkm"] = year_demand_lgv["distance"] * year_demand_lgv["lgv_flow"]
        year_demand_hgv["hgv_vkm"] = year_demand_hgv["distance"] * year_demand_hgv["hgv_flow"]

        year_demand_car = year_demand_car.groupby(["lad21_id", "car_speed_category"], as_index=False).sum()
        year_demand_lgv = year_demand_lgv.groupby(["lad21_id", "lgv_speed_category"], as_index=False).sum()
        year_demand_hgv = year_demand_hgv.groupby(["lad21_id", "hgv_speed_category"], as_index=False).sum()

        year_demand_car = pd.pivot(year_demand_car, index="lad21_id", columns="car_speed_category",
                                 values="car_vkm").reset_index()
        year_demand_lgv = pd.pivot(year_demand_lgv, index="lad21_id", columns="lgv_speed_category",
                                   values="lgv_vkm").reset_index()
        year_demand_car = pd.pivot(year_demand_hgv, index="lad21_id", columns="hgv_speed_category",
                                   values="hgv_vkm").reset_index()

        year_demand_car = year_demand_car.rename(columns={"lad21_id": "origin_zone", "10_30": "vkm_10-30_kph",
                                                          "30_50": "vkm_30-50_kph", "50_70": "vkm_50-70_kph",
                                                          "70_90": "vkm_70-90_kph", "90_110": "vkm_90-110_kph",
                                                          "110_130": "vkm_110-130_kph"})
        year_demand_car["Year"] = year

        year_demand_lgv = year_demand_lgv.rename(columns={"lad21_id": "origin_zone", "10_30": "vkm_10-30_kph",
                                                          "30_50": "vkm_30-50_kph", "50_70": "vkm_50-70_kph",
                                                          "70_90": "vkm_70-90_kph", "90_110": "vkm_90-110_kph",
                                                          "110_130": "vkm_110-130_kph"})
        year_demand_lgv["Year"] = year

        year_demand_hgv = year_demand_hgv.rename(columns={"lad21_id": "origin_zone", "10_30": "vkm_10-30_kph",
                                                          "30_50": "vkm_30-50_kph", "50_70": "vkm_50-70_kph",
                                                          "70_90": "vkm_70-90_kph", "90_110": "vkm_90-110_kph",
                                                          "110_130": "vkm_110-130_kph"})
        year_demand_hgv["Year"] = year

        year_demand_car.to_csv("vkm_by_speed_and_type_%s_car" % year)
        year_demand_lgv.to_csv("vkm_by_speed_and_type_%s_lgv" % year)
        year_demand_hgv.to_csv("vkm_by_speed_and_type_%s_hgv" % year)