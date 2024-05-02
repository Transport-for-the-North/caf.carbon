# Built-Ins
import configparser as cf

# Third Party
import pandas as pd

# Local Imports
from caf.carbon import utility as ut
from caf.carbon.load_data import DEMAND_PATH


class Scenario:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, region_filter, scenario_name, invariant_obj, pathway="none"):
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
        self.region_filter = region_filter
        self.scenario_name = scenario_name
        self.invariant = invariant_obj
        self.index_year = invariant_obj.index_year
        self.__load_keys()
        self.__load_scenario(pathway)
        self.__warp_tables()
        self.__merge_demand()

    def __load_keys(self):
        """Gives the initials and scenario number for a given scenario."""
        self.scenario_initials = {
            "Business As Usual Core": "BAU",
            "Accelerated EV Core": "AE",
        }[self.scenario_name]
        self.scenario_code = {"BAU": "SC01", "AE": "SC02"}[self.scenario_initials]

    def __load_scenario(self, pathway="none"):
        """Load in scenario tables."""
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

    def __warp_tables(self):
        """Preprocess and transform scenario inputs."""
        self.seg_share_of_year_type_sales = ut.interpolate_timeline(
            self.seg_share_of_year_type_sales,
            grouping_vars=["segment"],
            value_var="segment_sales_distribution",
        )

        self.fuel_share_of_year_seg_sales = ut.interpolate_timeline(
            self.fuel_share_of_year_seg_sales,
            grouping_vars=["segment", "fuel"],
            value_var="segment_fuel_sales_distribution",
        )

        # Set index year
        self.type_fleet_size_growth[self.index_year] = 0
        self.type_fleet_size_growth = pd.melt(
            self.type_fleet_size_growth,
            id_vars=["vehicle_type"],
            var_name="year",
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
        self.type_fleet_size_growth["index_fleet_growth"] = (
            self.type_fleet_size_growth.groupby("vehicle_type")["fleet_growth"].cumprod()
        )
        self.type_fleet_size_growth = self.type_fleet_size_growth[
            ["year", "vehicle_type", "index_fleet_growth"]
        ]

        self.km_index_reductions[self.index_year] = 0
        self.km_index_reductions = pd.melt(
            self.km_index_reductions,
            id_vars=["vehicle_type", "msoa_area_type"],
            var_name="year",
            value_name="km_reduction",
        )

        self.km_index_reductions["year"] = self.km_index_reductions["year"].astype("int32")
        # Convert change in proportion of index year to proportion of index year
        self.km_index_reductions["km_reduction"] = 1 + self.km_index_reductions["km_reduction"]

    def __merge_demand(self):
        """Concatenate car, van and HGV demand data."""
        demand = self.__load_demand("car")
        demand = pd.concat([demand, self.__load_demand("lgv")], ignore_index=True)
        self.demand = pd.concat([demand, self.__load_demand("hgv")], ignore_index=True)

    def __load_car_demand(self):
        """Load the car demand for a specified scenario."""
        path = str(DEMAND_PATH) + f"/{self.scenario_code}/"
        # self.configuration["filePaths"]["DemandFile"]
        # Iterate through all model years loading and appending demand.
        demand = pd.read_csv(f"{path}vkm_by_speed_and_type_{self.index_year}_car.csv")
        demand["year"] = self.index_year
        for i in range(2025, 2055, 5):
            x = pd.read_csv(f"{path}vkm_by_speed_and_type_{i}_car.csv")
            x["year"] = i
            demand = demand._append(x)
        # Drop extra columns
        car_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return car_demand

    def __load_hgv_demand(self):
        """Load the hgv demand for a specified scenario."""
        path = str(DEMAND_PATH) + f"/{self.scenario_code}/"

        # Iterate through all model years loading and appending demand.
        demand = pd.read_csv(f"{path}vkm_by_speed_and_type_{self.index_year}_hgv.csv")
        demand["year"] = self.index_year
        for i in range(2025, 2055, 5):
            x = pd.read_csv(f"{path}vkm_by_speed_and_type_{i}_hgv.csv")
            x["year"] = i
            demand = demand._append(x)

        # Drop extra columns
        hgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return hgv_demand

    def __load_lgv_demand(self):
        """Load the lgv demand for a specified scenario."""
        path = str(DEMAND_PATH) + f"/{self.scenario_code}/"

        # Iterate through all model years loading and appending demand.
        demand = pd.read_csv(f"{path}vkm_by_speed_and_type_{self.index_year}_lgv.csv")
        demand["year"] = self.index_year
        for i in range(2025, 2055, 5):
            x = pd.read_csv(f"{path}vkm_by_speed_and_type_{i}_lgv.csv")
            x["year"] = i
            demand = demand._append(x)
            # Drop extra columns
        lgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return lgv_demand

    def __load_demand(self, vehicle_type):
        """Load in and preprocess demand data for a given vehicle type."""
        new_cols = [
            "zone",
            "road_type",
            "10_30",
            "30_50",
            "50_70",
            "70_90",
            "90_110",
            "total_vehicle_km",
            "year",
        ]
        original_cols = [
            "origin_zone",
            "road_type",
            "vkm_10-30_kph",
            "vkm_30-50_kph",
            "vkm_50-70_kph",
            "vkm_70-90_kph",
            "vkm_90-110_kph",
            "total_vehicle_km",
            "year",
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
                "zone",
                "road_type",
                "year",
                "10_30",
                "30_50",
                "50_70",
                "70_90",
                "90_110",
                "total_vehicle_km",
                "vehicle_type",
                "km_growth",
            ]
        ]
        # Reshape demand to long form with distance travelled called "chainage"
        demand = pd.melt(
            demand,
            id_vars=[
                "zone",
                "road_type",
                "total_vehicle_km",
                "vehicle_type",
                "year",
                "km_growth",
            ],
            var_name="speed_band",
            value_name="chainage",
        )
        demand["chainage"] = demand["chainage"] * demand["km_growth"]
        demand = demand.drop(columns=["total_vehicle_km", "km_growth"])
        demand.loc[demand["road_type"] == "motorway", "road_type"] = "Motorway"
        demand.loc[demand["road_type"] == "urban", "road_type"] = "Urban"
        demand.loc[demand["road_type"] == "rural", "road_type"] = "Rural"
        return demand
