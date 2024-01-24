import pandas as pd
import utility as ut
from load_data import SEG_SHARE, FUEL_SHARE, DEMAND_PATH


class Scenario:
    """Load in and preprocess scenario variant tables."""

    def __init__(self, config, filter, time_period, time, scenario_name, invariant_obj, pathway="none"):
        """Initialise functions and set class variables.

        Parameters
        ----------
        config : configparser obj
            Filepath lookup to import tables.
        time_period : bool
            If Noham inputs split into AM, IP and PM
        scenario_name : str
            Full name of scenario.
        invariant_obj : class obj
            Includes the baseline fleet and scenario invariant tables.
        pathway : str
            Determines whether 'pathway' or standard scenario inputs are called in.
        """
        self.configuration = config
        self.time_period = time_period
        self.time = time
        self.type = "scenario"
        self.region_filter = filter
        self.scenario_name = scenario_name
        self.index_year = invariant_obj.index_year
        self.invariant = invariant_obj
        self.__load_keys()
        self.__load_scenario(pathway)
        self.__warp_tables()
        self.__merge_demand()

    def __load_keys(self):
        """Gives the initials and scenario number for a given scenario."""
        self.scenario_initials = {
            "Business As Usual Core": "BAU",
            "Business As Usual High": "BAUH",
            "Business As Usual Low": "BAUL",
            "Accelerated EV Core": "AE",
            "Accelerated EV High": "AEH",
            "Accelerated EV Low": "AEL"}[self.scenario_name]
        self.scenario_code = {
            "BAU": "SC01",
            "BAUH": "SC03",
            "BAUL": "SC05",
            "AE": "SC02",
            "AEH": "SC04",
            "AEL": "SC06"}[self.scenario_initials]

    def __load_scenario(self, pathway="none"):
        """Load in scenario tables."""
        self.seg_share_of_year_type_sales = ut.load_table(self, "segSales_propOfTypeYear", suffix=pathway.capitalize())
        self.fuel_share_of_year_seg_sales = ut.load_table(self, "fuelSales_propOfSegYear", suffix=pathway.capitalize())
        self.type_fleet_size_growth = ut.load_table(self, "fleetSize_totOfYear", suffix=pathway.capitalize())
        self.co2_reductions = ut.load_table(self, "co2Reduction", suffix=pathway.capitalize())
        self.km_index_reductions = ut.load_table(self, "ChainageReduction", suffix=pathway.capitalize())
        # File paths are different if the index year is 2015
        # Carry out some preprocessing so the tables are consistent with 2018 inputs
        if self.index_year == 2015:   # !
            seg_share_2015 = pd.read_csv(SEG_SHARE)
            seg_share_2015.columns = ["segment", 2015]
            self.seg_share_of_year_type_sales = self.seg_share_of_year_type_sales.merge(seg_share_2015, on="segment",
                                                                                        how="left")
            fuel_share_2015 = pd.read_csv(FUEL_SHARE)
            fuel_share_2015.columns = ["segment", "fuel", 2015]
            self.fuel_share_of_year_seg_sales = self.fuel_share_of_year_seg_sales.merge(fuel_share_2015,
                                                                                        on=["segment", "fuel"],
                                                                                        how="left")
            self.seg_share_of_year_type_sales = self.seg_share_of_year_type_sales.fillna(0)
            self.fuel_share_of_year_seg_sales = self.fuel_share_of_year_seg_sales.fillna(0)

    def __warp_tables(self):
        """Preprocess and transform scenario inputs."""
        self.seg_share_of_year_type_sales = ut.interpolate_timeline(
            self.seg_share_of_year_type_sales,
            grouping_vars=["segment"],
            value_var="segment_sales_distribution")

        self.fuel_share_of_year_seg_sales = ut.interpolate_timeline(
            self.fuel_share_of_year_seg_sales,
            grouping_vars=["segment", "fuel"],
            value_var="segment_fuel_sales_distribution")

        # Set index year
        self.type_fleet_size_growth[self.index_year] = 0
        self.type_fleet_size_growth = pd.melt(self.type_fleet_size_growth,
                                              id_vars=["vehicle_type"],
                                              var_name=["year"],
                                              value_name="fleet_growth")
        self.type_fleet_size_growth["year"] = self.type_fleet_size_growth["year"].astype("int32")
        self.type_fleet_size_growth = self.type_fleet_size_growth.sort_values(by=["year"])
        # Convert year-on-year change to proportion of previous year
        # Cumulative product gives proportion of index year
        self.type_fleet_size_growth["fleet_growth"] = 1 + self.type_fleet_size_growth["fleet_growth"]
        self.type_fleet_size_growth["index_fleet_growth"] = self.type_fleet_size_growth.groupby("vehicle_type")["fleet_growth"].cumprod()
        self.type_fleet_size_growth = self.type_fleet_size_growth[["year", "vehicle_type", "index_fleet_growth"]]
               
        self.km_index_reductions[self.index_year] = 0
        self.km_index_reductions = pd.melt(self.km_index_reductions,
                                           id_vars=["vehicle_type", "msoa_area_type"],
                                           var_name=["year"],
                                           value_name="km_reduction")

        self.km_index_reductions["year"] = self.km_index_reductions["year"].astype("int32")
        # Convert change in proportion of index year to proportion of index year
        self.km_index_reductions["km_reduction"] = 1 + self.km_index_reductions["km_reduction"]

    def __merge_demand(self):
        """Concatenate car, van and HGV demand data."""
        demand = self.__load_demand("car")
        demand = pd.concat([demand, self.__load_demand("lgv")], ignore_index=True)
        self.demand = pd.concat([demand, self.__load_demand("hgv")], ignore_index=True)

        self.demand = self.demand.loc[self.demand['zone'].isin(self.region_filter["msoa11_id"])].reset_index(drop=True)

    def __load_car_demand(self):
        """Load the car demand for a specified scenario."""
        if self.time_period:
            path = DEMAND_PATH + "/{}/".format(self.scenario_code)

            # Iterate through all model years loading and appending demand.
            demand = pd.read_csv("{path}vkm_by_speed_and_type_{self.index_year}_car_{self.time}.csv".format(**locals()))
            demand["Year"] = self.index_year
            for i in range(2020, 2055, 5):
                x = pd.read_csv("{path}vkm_by_speed_and_type_{i}_car_{self.time}.csv".format(**locals()))
                x["Year"] = i
                demand = demand._append(x)

        else:
            path = DEMAND_PATH + "/{}/".format(self.scenario_code)
            # self.configuration["filePaths"]["DemandFile"]
            # Iterate through all model years loading and appending demand.
            demand = pd.read_csv("{path}vkm_by_speed_and_type_{self.index_year}_car.csv".format(**locals()))
            demand["Year"] = self.index_year
            for i in range(2020, 2055, 5):
                x = pd.read_csv("{path}vkm_by_speed_and_type_{i}_car.csv".format(**locals()))
                x["Year"] = i
                demand = demand._append(x)

            # Drop extra columns
        car_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return car_demand

    def __load_hgv_demand(self):
        """Load the hgv demand for a specified scenario."""
        if self.time_period:
            path = DEMAND_PATH + "/{}/".format(self.scenario_code)

            # Iterate through all model years loading and appending demand.
            demand = pd.read_csv("{path}vkm_by_speed_and_type_{self.index_year}_hgv_{self.time}.csv".format(**locals()))
            demand["Year"] = self.index_year
            for i in range(2020, 2055, 5):
                x = pd.read_csv("{path}vkm_by_speed_and_type_{i}_hgv_{self.time}.csv".format(**locals()))
                x["Year"] = i
                demand = demand._append(x)
        else:
            path = DEMAND_PATH + "/{}/".format(self.scenario_code)

            # Iterate through all model years loading and appending demand.
            demand = pd.read_csv("{path}vkm_by_speed_and_type_{self.index_year}_hgv.csv".format(**locals()))
            demand["Year"] = self.index_year
            for i in range(2020, 2055, 5):
                x = pd.read_csv("{path}vkm_by_speed_and_type_{i}_hgv.csv".format(**locals()))
                x["Year"] = i
                demand = demand._append(x)

        # Drop extra columns
        hgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return hgv_demand

    def __load_lgv_demand(self):
        """Load the NoHAM lgv demand for a specified scenario."""
        if self.time_period:
            path = DEMAND_PATH + "/{}/".format(self.scenario_code)

            # Iterate through all model years loading and appending demand.
            demand = pd.read_csv("{path}vkm_by_speed_and_type_{self.index_year}_lgv_{self.time}.csv".format(**locals()))
            demand["Year"] = self.index_year
            for i in range(2020, 2055, 5):
                x = pd.read_csv("{path}vkm_by_speed_and_type_{i}_lgv_{self.time}.csv".format(**locals()))
                x["Year"] = i
                demand = demand._append(x)
        else:
            path = DEMAND_PATH + "/{}/".format(self.scenario_code)

            # Iterate through all model years loading and appending demand.
            demand = pd.read_csv("{path}vkm_by_speed_and_type_{self.index_year}_lgv.csv".format(**locals()))
            demand["Year"] = self.index_year
            for i in range(2020, 2055, 5):
                x = pd.read_csv("{path}vkm_by_speed_and_type_{i}_lgv.csv".format(**locals()))
                x["Year"] = i
                demand = demand._append(x)
            # Drop extra columns
        lgv_demand = demand[demand.columns.drop(list(demand.filter(regex="perc_")))]
        return lgv_demand

    def __load_demand(self, vehicle_type):
        """Load in and preprocess demand data for a given vehicle type."""
        new_cols = ["zone", "road_type", "0_10", "10_30", "30_50", "50_70", "70_90", "90_110", "110_130", "total_vehicle_km", "year"]
        original_cols = ["origin_zone", "road_type", "vkm_0-10_kph", "vkm_10-30_kph", "vkm_30-50_kph", "vkm_50-70_kph", "vkm_70-90_kph", "vkm_90-110_kph", "vkm_110-130_kph", "total_vehicle_km", "Year"]
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
        # Combine first 2 speed bands as speed Emissions Curves don't account for 1-10 kmph
        demand["10_30"] = demand["10_30"] + demand["0_10"]
        demand = demand.drop(columns=["0_10"])
        # Reshape demand to long form with distance travelled called "chainage"
        demand = pd.melt(demand, id_vars=["zone", "road_type", "total_vehicle_km", "vehicle_type", "year", "km_growth"],
                         var_name="speed_band",
                         value_name="chainage")
        demand["chainage"] = demand["chainage"] * demand["km_growth"]
        demand = demand.drop(columns=["total_vehicle_km", "km_growth"])
        return demand
