import pandas as pd
from caf.carbon import utility as ut
import configparser as cf
from caf.carbon.load_data import SEG_SHARE, FUEL_SHARE, DEMAND_PATH


class Scenario:
    """Load in and preprocess scenario variant tables."""
    def __init__(self, scenario_name, invariant_obj, pathway="none"):
        """Initialise functions and set class variables.
        Parameters
        ----------
        scenario_name : str
            Full name of scenario.
        invariant_obj : class obj
            Includes the baseline fleet and scenario invariant tables.
        pathway : str
            Determines whether 'pathway' or standard scenario inputs are called in.
        """
        self.configuration = cf.ConfigParser(interpolation=cf.ExtendedInterpolation())
        self.type = "scenario"
        self.scenario_name = scenario_name
        self.invariant = invariant_obj
        self.index_year = invariant_obj.index_year
        self.__load_keys()
        self.__load_scenario(pathway)
        self.__warp_tables()

    def __load_keys(self):
        """Gives the initials and scenario number for a given scenario."""
        self.scenario_initials = {
            "Business As Usual Core": "BAU",
            "Business As Usual High": "BAUH",
            "Business As Usual Low": "BAUL",
            "Accelerated EV Core": "AE",
            "Just About Managing": "JAM",
            "Prioritised Places": "PP",
            "Digitally Distributed": "DD",
            "Urban Zero Carbon": "UZC"
        }[self.scenario_name]
        self.scenario_code = {"JAM": "SC01",
                              "PP": "SC02",
                              "DD": "SC03",
                              "UZC": "SC04",
                              "BAU": "SC05",
                              "AE": "SC06"}[self.scenario_initials]

    def __load_scenario(self, pathway="none"):
        """Load in scenario tables."""
        if self.index_year == 2018:
            self.seg_share_of_year_type_sales = ut.load_scenario_tables(
                self.scenario_name, "segSales_propOfTypeYear", suffix=pathway
            )
            self.fuel_share_of_year_seg_sales = ut.load_scenario_tables(
                self.scenario_name, "fuelSales_propOfSegYear", suffix=pathway
            )
            self.type_fleet_size_growth = ut.load_scenario_tables(
                self.scenario_name, "fleetSize_totOfYear", suffix=pathway
            )
            self.co2_reductions = ut.load_scenario_tables(self.scenario_name, "co2Reduction", suffix=pathway
                                                              )
            self.km_index_reductions = ut.load_scenario_tables(
                self.scenario_name, "ChainageReduction", suffix=pathway
            )
            self.co2_reductions = ut.load_scenario_tables(self.scenario_name, "co2Reduction", suffix=pathway)
            self.km_index_reductions = ut.load_scenario_tables(
                self.scenario_name, "ChainageReduction", suffix=pathway
            )
        else:
            self.seg_share_of_year_type_sales = ut.new_load_scenario_tables(
                self.scenario_name, "segSales_propOfTypeYear", suffix=pathway
            )
            self.fuel_share_of_year_seg_sales = ut.new_load_scenario_tables(
                self.scenario_name, "fuelSales_propOfSegYear", suffix=pathway
            )
            self.type_fleet_size_growth = ut.new_load_scenario_tables(
                self.scenario_name, "fleetSize_totOfYear", suffix=pathway
            )
            self.co2_reductions = ut.new_load_scenario_tables(self.scenario_name, "co2Reduction", suffix=pathway
                                                              )
            self.km_index_reductions = ut.new_load_scenario_tables(
                self.scenario_name, "ChainageReduction", suffix=pathway
            )
            self.co2_reductions = ut.new_load_scenario_tables(self.scenario_name, "co2Reduction", suffix=pathway)
            self.km_index_reductions = ut.new_load_scenario_tables(
                self.scenario_name, "ChainageReduction", suffix=pathway
            )
        # File paths are different if the index year is 2015
        # Carry out some preprocessing so the tables are consistent with 2018 inputs
        if self.index_year == 2015:  # !
            seg_share_2015 = pd.read_csv(SEG_SHARE)
            print(self.fuel_share_of_year_seg_sales)
            seg_share_2015.columns = ["segment", 2015]
            self.seg_share_of_year_type_sales = self.seg_share_of_year_type_sales.merge(
                seg_share_2015, on="segment", how="left"
            )
            fuel_share_2015 = pd.read_csv(FUEL_SHARE)
            fuel_share_2015.columns = ["segment", "fuel", 2015]
            self.fuel_share_of_year_seg_sales = self.fuel_share_of_year_seg_sales.merge(
                fuel_share_2015, on=["segment", "fuel"], how="left"
            )
            print(self.fuel_share_of_year_seg_sales)
            self.seg_share_of_year_type_sales = self.seg_share_of_year_type_sales.fillna(0)
            self.fuel_share_of_year_seg_sales = self.fuel_share_of_year_seg_sales.fillna(0)

    def __warp_tables(self):
        """Preprocess and transform scenario inputs."""
        self.seg_share_of_year_type_sales = ut.interpolate_timeline(
            self.seg_share_of_year_type_sales,
            grouping_vars=["segment"],
            value_var="segment_sales_distribution",
        )
        print(self.seg_share_of_year_type_sales)
        self.fuel_share_of_year_seg_sales = ut.interpolate_timeline(
            self.fuel_share_of_year_seg_sales,
            grouping_vars=["segment", "fuel"],
            value_var="segment_fuel_sales_distribution",
        )
        print(self.fuel_share_of_year_seg_sales)
        # Set index year
        self.type_fleet_size_growth[self.index_year] = 0
        self.type_fleet_size_growth = pd.melt(
            self.type_fleet_size_growth,
            id_vars=["vehicle_type"],
            var_name=["year"],
            value_name="fleet_growth",
        )
        self.type_fleet_size_growth["year"] = self.type_fleet_size_growth["year"].astype(
            "int32"
        )

        self.type_fleet_size_growth = self.type_fleet_size_growth.sort_values(by=["year"])
        # Convert year-on-year change to proportion of previous year
        # Cumulative product gives proportion of index year
        self.type_fleet_size_growth["fleet_growth"] = (
            1 + self.type_fleet_size_growth["fleet_growth"]
        )
        self.type_fleet_size_growth["fleet_growth"] = pd.to_numeric(self.type_fleet_size_growth["fleet_growth"])
        self.type_fleet_size_growth["index_fleet_growth"] = (
            self.type_fleet_size_growth.groupby("vehicle_type")["fleet_growth"].cumprod()
        )
        self.type_fleet_size_growth = self.type_fleet_size_growth[
            ["year", "vehicle_type", "index_fleet_growth"]
        ]
        print(self.type_fleet_size_growth)
        self.km_index_reductions[self.index_year] = 0
        self.km_index_reductions = pd.melt(
            self.km_index_reductions,
            id_vars=["vehicle_type", "msoa_area_type"],
            var_name=["year"],
            value_name="km_reduction",
        )

        self.km_index_reductions["year"] = self.km_index_reductions["year"].astype("int32")
        # Convert change in proportion of index year to proportion of index year
        self.km_index_reductions["km_reduction"] = 1 + self.km_index_reductions["km_reduction"]
        print(self.km_index_reductions)

class Demand:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, scenario_obj, demand_year, time_period, demand_key):
        """Initialise functions and set class variables.

        Parameters
        ----------
        scenario_obj : class obj
            Includes the baseline scenario dependent tables.
        demand_year : str
            Sets the demand year data to be pulled
        time_period : str
            Sets the time period data to be pulled
        """

        self.invariant = scenario_obj
        self.year = demand_year
        self.key = demand_key
        self.time_period = time_period
        self.index_year = scenario_obj.index_year
        self.scenario_code = scenario_obj.scenario_code
        self.__merge_demand()

    def __merge_demand(self):
        """Concatenate car, van and HGV demand data."""
        demand = self.__load_demand("car")
        demand = pd.concat([demand, self.__load_demand("lgv")], ignore_index=True)
        self.demand = pd.concat([demand, self.__load_demand("hgv")], ignore_index=True)
        # self.demand = self.demand.loc[
        #     self.demand["zone"].isin(self.region_filter["msoa11_id"])
        # ].reset_index(drop=True)

    def __load_car_demand(self):
        """Load the car demand for a specified scenario."""
        path = str(DEMAND_PATH) + f"/{self.scenario_code}/"
        # self.configuration["filePaths"]["DemandFile"]
        # Iterate through all model years loading and appending demand.
        demand = pd.read_hdf(f"{path}vkm_by_speed_and_type_{self.year}_{self.time_period}_car.h5", self.key,
                             mode="r")
        # Drop extra columns
        car_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return car_demand

    def __load_hgv_demand(self):
        """Load the hgv demand for a specified scenario."""
        path = str(DEMAND_PATH) + f"/{self.scenario_code}/"
        # Iterate through all model years loading and appending demand.
        demand = pd.read_hdf(f"{path}vkm_by_speed_and_type_{self.year}_{self.time_period}_hgv.h5", self.key,
                             mode="r")
        # Drop extra columns
        hgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return hgv_demand

    def __load_lgv_demand(self):
        """Load the lgv demand for a specified scenario."""
        path = str(DEMAND_PATH) + f"/{self.scenario_code}/"

        # Iterate through all model years loading and appending demand.
        demand = pd.read_hdf(f"{path}vkm_by_speed_and_type_{self.year}_{self.time_period}_lgv.h5", self.key,
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
        # this column only relevant for when hgv/lgv growth is not modeled
        demand["km_growth"] = 1

        # Rename columns so they are consistent across vehicle type
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
                "km_growth",
            ]
        ]
        # Reshape demand to long form with distance travelled called "chainage"
        demand = pd.melt(
            demand,
            id_vars=[
                "origin",
                "destination",
                "through",
                "vehicle_type",
                "user_class",
                "trip_band",
                "km_growth",
            ],
            var_name="speed_band",
            value_name="vkm",
        )
        demand["vkm"] = demand["vkm"] * demand["km_growth"]
        demand = demand.drop(columns=["km_growth"])
        demand = demand[demand["vkm"] != 0]
        # references to road_type have been removed
        # demand.loc[demand["road_type"] == "motorway", "road_type"] = "Motorway"
        # demand.loc[demand["road_type"] == "urban", "road_type"] = "Urban"
        # demand.loc[demand["road_type"] == "rural", "road_type"] = "Rural"
        return demand


# class Scenario:
#     """Load in and preprocess scenario variant tables."""
#
#     def __init__(self, region_filter, scenario_name, invariant_obj, pathway="none"):
#         """Initialise functions and set class variables.
#
#         Parameters
#         ----------
#         scenario_name : str
#             Full name of scenario.
#         invariant_obj : class obj
#             Includes the baseline fleet and scenario invariant tables.
#         pathway : str
#             Determines whether 'pathway' or standard scenario inputs are called in.
#         """
#         self.configuration = cf.ConfigParser(interpolation=cf.ExtendedInterpolation())
#         self.type = "scenario"
#         self.region_filter = region_filter
#         self.scenario_name = scenario_name
#         self.invariant = invariant_obj
#         self.index_year = invariant_obj.index_year
#         self.__load_keys()
#         self.__load_scenario(pathway)
#         self.__warp_tables()
#         self.__merge_demand()
#
#     def __load_keys(self):
#         """Gives the initials and scenario number for a given scenario."""
#         self.scenario_initials = {
#             "Business As Usual Core": "BAU",
#             "Accelerated EV Core": "AE",
#             "Just About Managing": "JAM",
#             "Prioritised Places": "PP",
#             "Digitally Distributed": "DD",
#             "Urban Zero Carbon": "UZC"
#         }[self.scenario_name]
#         self.scenario_code = {"JAM": "SC01",
#                               "PP": "SC02",
#                               "DD": "SC03",
#                               "UZC": "SC04",
#                               "BAU": "SC05",
#                               "AE": "SC06"}[self.scenario_initials]
#
#     def __load_scenario(self, pathway="none"):
#         """Load in scenario tables."""
#         self.seg_share_of_year_type_sales = ut.new_load_scenario_tables(
#             self.scenario_name, "segSales_propOfTypeYear", suffix=pathway
#         )
#         self.fuel_share_of_year_seg_sales = ut.new_load_scenario_tables(
#             self.scenario_name, "fuelSales_propOfSegYear", suffix=pathway
#         )
#         self.type_fleet_size_growth = ut.new_load_scenario_tables(
#             self.scenario_name, "fleetSize_totOfYear", suffix=pathway
#         )
#         self.co2_reductions = ut.new_load_scenario_tables(self.scenario_name, "co2Reduction", suffix=pathway
#         )
#         self.km_index_reductions = ut.new_load_scenario_tables(
#             self.scenario_name, "ChainageReduction", suffix=pathway
#         )
#
#     def __warp_tables(self):
#         """Preprocess and transform scenario inputs."""
#         self.seg_share_of_year_type_sales = ut.interpolate_timeline(
#             self.seg_share_of_year_type_sales,
#             grouping_vars=["segment"],
#             value_var="segment_sales_distribution",
#         )
#
#         self.fuel_share_of_year_seg_sales = ut.interpolate_timeline(
#             self.fuel_share_of_year_seg_sales,
#             grouping_vars=["segment", "fuel"],
#             value_var="segment_fuel_sales_distribution",
#         )
#
#         # Set index year
#         self.type_fleet_size_growth[self.index_year] = 0
#         self.type_fleet_size_growth = pd.melt(
#             self.type_fleet_size_growth,
#             id_vars=["vehicle_type"],
#             var_name="year",
#             value_name="fleet_growth",
#         )
#         self.type_fleet_size_growth["year"] = self.type_fleet_size_growth["year"].astype(
#             "int32"
#         )
#
#         self.type_fleet_size_growth = self.type_fleet_size_growth.sort_values(by=["year"])
#         # Convert year-on-year change to proportion of previous year
#         # Cumulative product gives proportion of index year
#         self.type_fleet_size_growth["fleet_growth"] = (
#             1 + self.type_fleet_size_growth["fleet_growth"]
#         )
#         self.type_fleet_size_growth["fleet_growth"] = pd.to_numeric(self.type_fleet_size_growth["fleet_growth"])
#         self.type_fleet_size_growth["index_fleet_growth"] = (
#             self.type_fleet_size_growth.groupby("vehicle_type")["fleet_growth"].cumprod()
#         )
#         self.type_fleet_size_growth = self.type_fleet_size_growth[
#             ["year", "vehicle_type", "index_fleet_growth"]
#         ]
#
#         self.km_index_reductions[self.index_year] = 0
#         self.km_index_reductions = pd.melt(
#             self.km_index_reductions,
#             id_vars=["vehicle_type", "msoa_area_type"],
#             var_name="year",
#             value_name="km_reduction",
#         )
#
#         self.km_index_reductions["year"] = self.km_index_reductions["year"].astype("int32")
#         # Convert change in proportion of index year to proportion of index year
#         self.km_index_reductions["km_reduction"] = 1 + self.km_index_reductions["km_reduction"]
#
#     def __merge_demand(self):
#         """Concatenate car, van and HGV demand data."""
#         demand = self.__load_demand("car")
#         demand = pd.concat([demand, self.__load_demand("lgv")], ignore_index=True)
#         self.demand = pd.concat([demand, self.__load_demand("hgv")], ignore_index=True)
#
#     def __load_car_demand(self):
#         """Load the car demand for a specified scenario."""
#         path = str(DEMAND_PATH) + f"/{self.scenario_code}/"
#         # self.configuration["filePaths"]["DemandFile"]
#         # Iterate through all model years loading and appending demand.
#         demand = pd.read_csv(f"{path}vkm_by_speed_and_type_{self.index_year}_car.csv")
#         demand["year"] = self.index_year
#         for i in range(2025, 2055, 5):
#             x = pd.read_csv(f"{path}vkm_by_speed_and_type_{i}_car.csv")
#             x["year"] = i
#             demand = demand._append(x)
#         # Drop extra columns
#         car_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
#         return car_demand
#
#     def __load_hgv_demand(self):
#         """Load the hgv demand for a specified scenario."""
#         path = str(DEMAND_PATH) + f"/{self.scenario_code}/"
#
#         # Iterate through all model years loading and appending demand.
#         demand = pd.read_csv(f"{path}vkm_by_speed_and_type_{self.index_year}_hgv.csv")
#         demand["year"] = self.index_year
#         for i in range(2025, 2055, 5):
#             x = pd.read_csv(f"{path}vkm_by_speed_and_type_{i}_hgv.csv")
#             x["year"] = i
#             demand = demand._append(x)
#
#         # Drop extra columns
#         hgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
#         return hgv_demand
#
#     def __load_lgv_demand(self):
#         """Load the lgv demand for a specified scenario."""
#         path = str(DEMAND_PATH) + f"/{self.scenario_code}/"
#
#         # Iterate through all model years loading and appending demand.
#         demand = pd.read_csv(f"{path}vkm_by_speed_and_type_{self.index_year}_lgv.csv")
#         demand["year"] = self.index_year
#         for i in range(2025, 2055, 5):
#             x = pd.read_csv(f"{path}vkm_by_speed_and_type_{i}_lgv.csv")
#             x["year"] = i
#             demand = demand._append(x)
#             # Drop extra columns
#         lgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
#         return lgv_demand
#
#     def __load_demand(self, vehicle_type):
#         """Load in and preprocess demand data for a given vehicle type."""
#         new_cols = [
#             "origin",
#             "destination",
#             "through",
#             "time_period",
#             "user_class",
#             "10_30",
#             "30_50",
#             "50_70",
#             "70_90",
#             "90_110",
#             "trip_band",
#             "year"
#         ]
#         original_cols = [
#             "origin",
#             "destination",
#             "through",
#             "time_period",
#             "user_class",
#             "vkm_10-30_kph",
#             "vkm_30-50_kph",
#             "vkm_50-70_kph",
#             "vkm_70-90_kph",
#             "vkm_90-110_kph",
#             "trip_band",
#             "year"
#         ]
#         if vehicle_type == "car":
#             demand = self.__load_car_demand()
#         elif vehicle_type == "hgv":
#             demand = self.__load_hgv_demand()
#         else:
#             demand = self.__load_lgv_demand()
#         demand["vehicle_type"] = vehicle_type
#         # this column only relevant for when hgv/lgv growth is not modeled
#         demand["km_growth"] = 1
#
#         # Rename columns so they are consistent across vehicle type
#         rename_cols = dict(zip(original_cols, new_cols))
#         demand = demand.rename(columns=rename_cols)
#         demand = demand[
#             [
#                 "origin",
#                 "destination",
#                 "through",
#                 "vehicle_type",
#                 "time_period",
#                 "user_class",
#                 "year",
#                 "10_30",
#                 "30_50",
#                 "50_70",
#                 "70_90",
#                 "90_110",
#                 "trip_band",
#                 "km_growth",
#             ]
#         ]
#         # Reshape demand to long form with distance travelled called "chainage"
#         demand.to_csv("unmelted demand.csv")
#         demand = pd.melt(
#             demand,
#             id_vars=[
#                 "origin",
#                 "destination",
#                 "through",
#                 "vehicle_type",
#                 "year",
#                 "user_class",
#                 "time_period",
#                 "trip_band",
#                 "km_growth",
#             ],
#             var_name="speed_band",
#             value_name="vkm",
#         )
#         demand.to_csv("speedbands.csv")
#         demand["vkm"] = demand["vkm"] * demand["km_growth"]
#         demand = demand.drop(columns=["km_growth"])
#         # references to road_type have been removed
#         # demand.loc[demand["road_type"] == "motorway", "road_type"] = "Motorway"
#         # demand.loc[demand["road_type"] == "urban", "road_type"] = "Urban"
#         # demand.loc[demand["road_type"] == "rural", "road_type"] = "Rural"
#         return demand
