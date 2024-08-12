from datetime import datetime

import pandas as pd
import numpy as np
import datetime

from caf.carbon import utility as ut
from caf.carbon import fleet_redistribution
from caf.carbon.load_data import OUT_PATH


class Model:
    """Predict emissions using emissions, demand and projected fleet data."""

    def __init__(
        self, region_filter, invariant_obj, scenario_obj, ev_redistribution, run_name, ev_redistribution_fresh
    ):
        """Initialise functions and set class variables.

        Parameters
        ----------
        invariant_obj : class obj
            Includes the baseline fleet and scenario invariant tables.
        scenario_obj : class obj
            Includes scenario tables.
        """
        self.outpath = OUT_PATH
        self.run_name = run_name
        self.invariant = invariant_obj
        self.scenario = scenario_obj
        self.se_curve = self.invariant.se_curve
        #self.date = datetime.today().strftime("%Y_%m_%d")
        self.ev_redistribution = ev_redistribution
        self.ev_redistribution_fresh = ev_redistribution_fresh
        self.region_filter = region_filter
        self.__predict_fleet_size()
        self.__create_future_fleet()
        self.__predict_sales()
        self.projected_fleet = self.__project_fleet()

    #########
    # FLEET #
    #########

    def __predict_fleet_size(self):
        """Derive the fleet size in future years.

        Multiply index year fleet size by index fleet growth to give yearly fleet forecasts.
        Returns a df by vehicle type and tally.
        """
        fleet_size = self.invariant.index_fleet.fleet.copy()
        fleet_size = fleet_size.groupby("vehicle_type")["tally"].sum().reset_index()
        fleet_size = fleet_size.merge(
            self.scenario.type_fleet_size_growth, how="left", on="vehicle_type"
        )
        fleet_size["tally"] = fleet_size["tally"] * fleet_size["index_fleet_growth"]
        fleet_size = ut.interpolate_timeline(
            fleet_size, grouping_vars=["vehicle_type"], value_var="tally", melt=False
        )
        self.scenario.fleet_size = fleet_size.rename(columns={"tally": "fleet_forecast"})

    def __create_future_fleet(self):
        """Create a template fleet which contains every vehicle that will occur.

        Match all fuel possible types to existing segments in the baseline year.
        Return a df without tally or characteristics.
        """
        fleet_df = self.invariant.index_fleet.fleet.copy()
        # Find all fuel-segments that we consider (i.e. exists in sales data)
        seg_fuel_list = self.scenario.fuel_share_of_year_seg_sales[
            ["segment", "fuel"]
        ].drop_duplicates()
        # Find the zones which purchase each segment (i.e. a cohort exists from the index year)
        future_fleet = fleet_df.loc[fleet_df["cohort"] == self.invariant.index_year]
        future_fleet = future_fleet[
            ["segment", "zone", "vehicle_type", "cohort"]
        ].drop_duplicates()
        # Map the fuel-segments to the zones known to buy them
        self.scenario.future_fleet = future_fleet.merge(
            seg_fuel_list, how="left", on="segment"
        )

    def __predict_sales(self):
        """Derive the fuel-segment share of sales in each zone and year."""
        fleet_df = self.invariant.index_fleet.fleet.copy()
        # Sales in index year = number of vehicles in the index year cohort broken down by segment and zone
        index_sales = fleet_df.loc[fleet_df["cohort"] == fleet_df["cohort"].max()]
        index_sales = index_sales.groupby(["segment", "zone"])["tally"].sum().reset_index()

        # Zonal distribution of sold segments.
        # e.g. 30% of all the minis sold in YEAR are sold to zone 7
        index_sales["seg_share"] = index_sales["tally"] / index_sales.groupby(["segment"])[
            "tally"
        ].transform("sum")
        index_sales = index_sales.drop(columns="tally")

        # Merge future fleet with the index fleet on segment and zone
        # Merge this df with both future sales share tables
        future_sales = self.scenario.future_fleet.copy()
        future_sales = future_sales.merge(index_sales, how="left", on=["segment", "zone"])
        future_sales = future_sales.merge(
            self.scenario.seg_share_of_year_type_sales, how="left", on=["segment"]
        )
        future_sales = future_sales.merge(
            self.scenario.fuel_share_of_year_seg_sales,
            how="left",
            on=["segment", "fuel", "year"],
        )

        # % of all vehicles sold which are fuel-segment sold to zone
        # e.g. 12% of all LGVs sold in YEAR are petrol n1 class ii sold to zone 7
        # zone fuel segment share of type sales = zone share of segment sales *
        # segment share of type sales * fuel share of segment sales
        eqn = "{seg_share}*{segment_sales_distribution}*{segment_fuel_sales_distribution}"
        try:
            future_sales["sales_share"] = future_sales.apply(eqn.format_map, axis=1).map(eval)
        except NameError:
            future_sales = future_sales.dropna()
            print(future_sales.shape[0])

        future_sales["cohort"] = future_sales["year"]
        self.scenario.fleet_sales = future_sales[
            ["zone", "segment", "cohort", "fuel", "vehicle_type", "sales_share"]
        ]

    @staticmethod
    def __jump_forward(fleet_df, scrappage_df, fleet_sales_df, tally_df):
        """Transform a fleet into the next years fleet."""

        def withdraw_cohort(fleet_to_withdraw, withdrawal_scrappage, withdrawal_year):
            """Increase cya by 1 and apply scrappage curve."""
            fleet_df["cya"] = withdrawal_year - fleet_to_withdraw["cohort"]
            fleet_to_withdraw = fleet_to_withdraw.merge(withdrawal_scrappage, how="left", on=["cya", "vehicle_type"])
            fleet_to_withdraw["tally"] = fleet_to_withdraw["tally"] * fleet_to_withdraw["survival"]
            fleet_to_withdraw = fleet_to_withdraw.dropna(subset=["tally"])
            fleet_to_withdraw = fleet_to_withdraw.drop(columns=["survival", "cya"])
            return fleet_to_withdraw

        def inject_cohort(fleet_to_inject, fleet_sales_injection, injection_tally, injection_year):
            """Add cya = 0 vehicles to match scenario defined fleet size."""
            required_fleet = injection_tally.loc[injection_tally["year"] == injection_year]
            current_fleet = fleet_df.groupby("vehicle_type")["tally"].sum().reset_index()
            required_sales = current_fleet.merge(required_fleet, how="left", on="vehicle_type")
            # sales = scenario defined fleet size - current fleet size
            required_sales["deficit"] = (
                required_sales["fleet_forecast"] - required_sales["tally"]
            )
            required_sales = required_sales[["vehicle_type", "deficit"]]
            new_cohort = fleet_sales_injection.merge(required_sales, how="left", on=["vehicle_type"])
            new_cohort = new_cohort[new_cohort["cohort"] == current_year]
            # zone fuel segment sales = zone fuel segment share of type sales * type sales
            new_cohort["tally"] = new_cohort["sales_share"] * new_cohort["deficit"]
            fleet_to_inject = pd.concat([fleet_to_inject, new_cohort[
                ["fuel", "segment", "cohort", "zone", "vehicle_type", "tally"]]]
            )
            return fleet_to_inject

        current_year = fleet_df["cohort"].max() + 1
        fleet_df = withdraw_cohort(fleet_df, scrappage_df, current_year)
        fleet_df = inject_cohort(fleet_df, fleet_sales_df, tally_df, current_year)
        return fleet_df, current_year

    def __project_fleet(self):
        """Predict the fleet for each key model year."""
        fleet_df = self.invariant.index_fleet.fleet[
            ["fuel", "segment", "zone", "tally", "vehicle_type", "cohort"]
        ]
        fleet_useful_years = fleet_df.copy()
        fleet_useful_years["year"] = fleet_df["cohort"].max()

        scrappage_df = self.invariant.scrappage_curve.copy()
        fleet_sales_df = self.scenario.fleet_sales.copy()
        tally_df = self.scenario.fleet_size.copy()
        # Predict the fleet for each year, storing fleets of key model years.
        print("\n\nProjecting fleet:")
        for i in range(self.invariant.index_year, 2050):
            fleet_df, current_year = self.__jump_forward(
                fleet_df, scrappage_df, fleet_sales_df, tally_df
            )
            print(f"\r{current_year}", end="\r")
            if current_year % 5 == 0:
                fleet_useful_years = fleet_useful_years.append(fleet_df).fillna(current_year)

        fleet_useful_years = fleet_useful_years[fleet_useful_years["tally"] > 0]

        if self.ev_redistribution:
            ev_projected_fleet = fleet_redistribution.Redistribution(
                self.invariant, self.scenario, fleet_useful_years, self.ev_redistribution_fresh
            ).projected_fleet
            self.projected_fleet = ev_projected_fleet
        else:
            self.projected_fleet = fleet_useful_years

        # if self.scenario.scenario_initials == "BAU":
        #     fleet_useful_years.to_csv(
        #         f"{self.outpath}/audit/projected_fleet_{self.date}.csv", index=False
        #     )
        print("\rProjection complete.\n")
        # Iterate through all model years loading and appending demand.
        self.projected_fleet = self.projected_fleet.loc[
            self.projected_fleet["zone"].isin(self.region_filter["MSOA11CD"])
        ].reset_index(drop=True)
        return self.projected_fleet

    ##########
    # DEMAND #
    ##########
    def fleet_transform(self):
        """Preparing projected fleet for coupling with demand."""

        self.projected_fleet["cya"] = (
                self.projected_fleet["year"] - self.projected_fleet["cohort"]
        )
        self.projected_fleet = ut.cya_column_to_group(self.projected_fleet)

        self.projected_fleet = ut.determine_body_type(self.projected_fleet)
        # Fuel-segment-cohort share of each zone vehicletype cyagroup
        self.projected_fleet = self.projected_fleet.rename(columns={"zone": "origin"})
        self.projected_fleet["prop_by_fuel_seg_cohort"] = self.projected_fleet[
                                                              "tally"
                                                          ] / self.projected_fleet.groupby(
            ["origin", "cya", "vehicle_type", "year"])[
                                                              "tally"
                                                          ].transform(
            "sum"
        )

        se_curve = self.se_curve.copy().drop_duplicates()
        se_curve = se_curve.merge(self.invariant.ghg_equivalent, how="left", on=["segment", "fuel"])
        se_curve = se_curve.merge(self.scenario.co2_reductions, how="left", on=["segment", "vehicle_type"])
        se_curve = se_curve.merge(self.invariant.yearly_co2_reduction, how="left", on=["segment", "e_cohort"])
        se_curve["index_carbon_reduction"] = se_curve["index_carbon_reduction"].fillna(1)
        se_curve["tailpipe_gco2"] = (se_curve["gco2/km"]
                                     * se_curve["ghg_factor"]
                                     * (1 + se_curve["co2_reduction"])
                                     * se_curve["index_carbon_reduction"])
        # se_curve = se_curve.set_index(["fuel", "segment", "e_cohort", "speed_band", "vehicle_type"])
        self.se_curve = se_curve[["fuel", "segment", "e_cohort", "speed_band", "vehicle_type", "tailpipe_gco2"]]
        grid_consumption = self.invariant.grid_consumption
        grid_intensity = self.invariant.grid_intensity
        grid_consumption["join"], grid_intensity["join"] = 1, 1
        grid_emissions = grid_consumption.merge(grid_intensity, how="outer", on="join")
        grid_emissions["grid_gco2"] = grid_emissions["kwh_km"] * grid_emissions["gco2_kwh"]
        grid_emissions = grid_emissions[["cohort", "vehicle_type", "segment", "fuel", "year", "grid_gco2"]]
        self.grid_emissions = grid_emissions

    def allocate_emissions(self, demand_data, year, time_period, first_enumeration):
        """Distribute the vehicle kms between fuel-segment-cohorts."""

        # introduce vkm chunking, break apart fleet, loop over last three functions, concat on outputs only
        # Divide chainage by ANPR data, each cya is distributed part of the bodytype-roadtype chainage
        # loop on ts and uc
        # merge on indices for memory efficiency, dask
        demand = demand_data.demand
        chainage = pd.merge(
            self.invariant.anpr,
            demand,
            how="left",
            on="vehicle_type",  # ["vehicle_type", "road_type"]
            copy=False
        )
        chainage["vkm"] *= chainage["cya_prop_of_bt_rt"]
        chainage["cya"] = chainage["cya"].astype(str)
        chainage = (
            chainage.groupby(["vehicle_type", "cya", "user_class", "origin", "destination",
                              "through", "speed_band", "trip_band"])["vkm"]
            .sum()
            .reset_index()
        )
        # component_no, component_size = 0, 20
        # unique_origins = self.unique_origins
        for user_class in [["uc1"], ["uc2"], ["uc3"], ["uc4", "uc5"]]:
            print(user_class)
            fleet_component = self.projected_fleet[self.projected_fleet["year"] == year]
            fleet_component = fleet_component[fleet_component["origin"].isin(chainage["origin"])]

            demand_component = chainage[chainage["user_class"].isin(user_class)]
            print("userclass sent to emissions assignment")
            emissions_data = self.assign_chunk_emissions(
                fleet_component, demand_component)
            """Save the final output (fleet + chainage + emissions)."""
            emissions_data["scenario"] = self.scenario.scenario_code
            # fleet = fleet.merge( TODO may need to add this later for WSP
            #     self.invariant.new_area_types[["zone", "msoa_area_type"]], how="left", on="zone", copy=False)
            print("writing to file")
            if first_enumeration:
                emissions_data.to_hdf(
                    f"{self.outpath}/{self.run_name}_{self.scenario.scenario_initials}"
                    f"_fleet_emissions_{self.date}.h5", f"{time_period}_{year}", mode='w', complevel=1, format="table",
                    index=False,
                )
                print("First enumerated")
                first_enumeration = False
            else:
                emissions_data.to_hdf(
                    f"{self.outpath}/{self.run_name}_{self.scenario.scenario_initials}"
                    f"_fleet_emissions_{self.date}.h5", f"{time_period}_{year}", mode='a', complevel=1, append=True,
                    format="table",
                    index=False,
                )
                print("Appending")

    def assign_chunk_emissions(self, fleet, demand):
        fleet = fleet.set_index(["vehicle_type", "cya", "origin"])
        demand = demand.set_index(["vehicle_type", "cya", "origin"])
        print("merging fleet with demand")
        fleet = fleet.merge(
            demand, how="inner", left_index=True, right_index=True, copy=False
        )  # 30 seconds
        fleet = fleet.reset_index()  # 6 seconds
        # Use tally share to distribute chainage
        print("multiplying vkm by prop seg")
        fleet["vkm"] *= fleet["prop_by_fuel_seg_cohort"]
        print("dropping prop by fuel seg cohort")
        fleet = fleet.drop(
            columns=["prop_by_fuel_seg_cohort"]
        )  # 7 seconds
        # All cohorts after the index year use the index year se curves with a scaling factor
        fleet["e_cohort"] = np.minimum(fleet["cohort"], self.invariant.index_year)
        print("merge se curve with fleet")
        print(datetime.datetime.now())
        fleet = fleet.merge(
            self.se_curve,
            how="left",
            on=["fuel", "segment", "e_cohort", "speed_band", "vehicle_type"],
            copy=False
        )  # TODO 50 seconds
        print(datetime.datetime.now())
        # CO2 = CO2/km * km
        fleet["tailpipe_gco2"] *= fleet["vkm"]
        print("grouping and dropping columns")
        print(datetime.datetime.now())
        fleet = (
            fleet.groupby(
                ["fuel", "segment", "origin", "destination", "through", "user_class", "vehicle_type", "cohort",
                 "year", "trip_band", "tally"]
            )[["tailpipe_gco2", "vkm"]]
            .sum()
        )  # TODO 60 SECONDS
        print(datetime.datetime.now())
        # Calculate indirect emissions through the electricity grid
        print("calculating grid emissions")
        fleet.to_csv("fleet1.csv")
        fleet = pd.merge(fleet,
                         self.grid_emissions,
                         on=["cohort", "vehicle_type", "segment", "fuel", "year"],
                         how="left",
                         #copy=False
                         ).fillna(0)  # TODO 30 seconds
        fleet.to_csv("fleet2.csv")
        fleet["grid_gco2"] *= fleet["vkm"]
        print(datetime.datetime.now())
        fleet = (
            fleet.groupby(
                ["origin", "destination", "through", "fuel", "user_class", "vehicle_type",
                 "year", "trip_band", "tally"]
            )[["tailpipe_gco2", "grid_gco2", "vkm"]]
            .sum()
            .reset_index()
        )  # 13 seconds
        return fleet

    # def allocate_chainage(self):
    #     """Distribute the vehicle kms between fuel-segment-cohorts."""
    #
    #     # introduce vkm chunking, break apart fleet, loop over last three functions, concat on outputs only
    #
    #     # Divide chainage by ANPR data, each cya is distributed part of the bodytype-roadtype chainage
    #     chainage = pd.merge(
    #         self.invariant.anpr,
    #         self.scenario.demand,
    #         how="left",
    #         on=["vehicle_type", "road_type"],
    #     )
    #     chainage["chainage"] = chainage["chainage"] * chainage["cya_prop_of_bt_rt"]
    #     chainage = (
    #         chainage.groupby(["vehicle_type", "cya", "zone", "speed_band", "year"])["chainage"]
    #         .sum()
    #         .reset_index()
    #     )
    #     self.projected_fleet["cya"] = (
    #         self.projected_fleet["year"] - self.projected_fleet["cohort"]
    #     )
    #     self.projected_fleet = ut.cya_column_to_group(self.projected_fleet)
    #
    #     self.projected_fleet = ut.determine_body_type(self.projected_fleet)
    #     # Fuel-segment-cohort share of each zone vehicletype cyagroup
    #     self.projected_fleet["prop_by_fuel_seg_cohort"] = self.projected_fleet[
    #         "tally"
    #     ] / self.projected_fleet.groupby(["zone", "cya", "vehicle_type", "year"])[
    #         "tally"
    #     ].transform(
    #         "sum"
    #     )
    #     chainage.to_csv("chainage.csv")
    #     self.projected_fleet.to_csv("projected fleet")
    #     self.projected_fleet = self.projected_fleet.merge(
    #         chainage, how="left", on=["vehicle_type", "cya", "zone", "year"]
    #     )
    #     self.projected_fleet = self.projected_fleet.merge(
    #         self.invariant.new_area_types[["zone", "msoa_area_type"]], how="left", on="zone"
    #     )
    #     self.projected_fleet = self.projected_fleet.merge(
    #         self.scenario.km_index_reductions,
    #         how="left",
    #         on=["year", "vehicle_type", "msoa_area_type"],
    #     )
    #     # Use tally share to distribute chainage
    #     self.projected_fleet["chainage"] = (
    #         self.projected_fleet["chainage"] * self.projected_fleet["prop_by_fuel_seg_cohort"]
    #     )
    #     # Apply scenario based nelum area type travel reductions
    #     self.projected_fleet["chainage"] = self.projected_fleet[
    #         "chainage"
    #     ] * self.projected_fleet["km_reduction"].fillna(0)
    #     self.projected_fleet = self.projected_fleet.drop(
    #         columns=["cya", "prop_by_fuel_seg_cohort", "km_reduction"]
    #     )
    #
    # def predict_emissions(self):
    #     """Convert chainage and emission intensity to emissions."""
    #     fleet_df = self.projected_fleet.copy()
    #     emission_change_df = self.invariant.yearly_co2_reduction.copy()
    #     se_curve = self.invariant.se_curve.copy().drop_duplicates()
    #
    #     # All cohorts after the index year use the index year se curves with a scaling factor
    #     fleet_df["e_cohort"] = np.minimum(fleet_df["cohort"], self.invariant.index_year)
    #     fleet_df = fleet_df.merge(
    #         se_curve,
    #         how="left",
    #         on=["fuel", "segment", "e_cohort", "speed_band", "vehicle_type"],
    #     )
    #     fleet_df = fleet_df.merge(emission_change_df, how="left", on=["cohort", "body_type"])
    #     fleet_df["index_carbon_reduction"] = fleet_df["index_carbon_reduction"].fillna(1)
    #
    #     # petrol and diesel biofuel component reductions
    #     bio_correction = self.invariant.biofuel_reduction
    #     bins = bio_correction["switch_year"]
    #     year_labels = []
    #     for i in range(len(bio_correction["switch_year"])):
    #         if i > 0:
    #             year_labels.append(bio_correction["switch_year"].loc[i])
    #
    #     fleet_df["binned"] = pd.cut(fleet_df["year"], bins, labels=year_labels)
    #     fleet_df["biocorrection"] = 1.0
    #     for i in range(len(bio_correction["switch_year"])):
    #         # petrol correction
    #         fleet_df.loc[
    #             (
    #                 (fleet_df["fuel"] == "petrol")
    #                 & (fleet_df["binned"] == bio_correction["switch_year"].loc[i])
    #             ),
    #             "biocorrection",
    #         ] = bio_correction.loc[i]["petrol"]
    #         # diesel correction
    #         fleet_df.loc[
    #             (
    #                 (fleet_df["fuel"] == "diesel")
    #                 & (fleet_df["binned"] == bio_correction["switch_year"].loc[i])
    #             ),
    #             "biocorrection",
    #         ] = bio_correction.loc[i]["diesel"]
    #
    #     # GHG equivalents
    #     ghg_factor = self.invariant.ghg_equivalent
    #     for i in range(len(ghg_factor)):
    #         fleet_df.loc[
    #             (
    #                 (fleet_df["fuel"] == ghg_factor.loc[i]["fuel"])
    #                 & (fleet_df["segment"] == ghg_factor.loc[i]["segment"])
    #             ),
    #             "ghg_factor",
    #         ] = ghg_factor.loc[i]["factor"]
    #
    #     # CO2 = CO2/km * km
    #     fleet_df["tailpipe_gco2"] = (
    #         fleet_df["gco2/km"]
    #         * fleet_df["index_carbon_reduction"]
    #         * fleet_df["chainage"]
    #         * fleet_df["biocorrection"]
    #         * fleet_df["ghg_factor"]
    #     )
    #
    #     # Apply reduction factors to relevant vehicles using evidence from the CCC 6th carbon budget
    #     # Only apply them from 2025
    #     fleet_df = fleet_df.merge(
    #         self.scenario.co2_reductions, on=["vehicle_type", "segment"], how="left"
    #     )
    #     fleet_df.loc[fleet_df.year < 2025, "co2_reduction"] = 0
    #     fleet_df["tailpipe_gco2"] = fleet_df["tailpipe_gco2"] * (1 + fleet_df["co2_reduction"])
    #
    #     # Calculate indirect emissions through the electricity grid
    #     fleet_df = (
    #         fleet_df.groupby(
    #             ["fuel", "segment", "zone", "vehicle_type", "cohort", "year", "tally"]
    #         )[["tailpipe_gco2", "chainage"]]
    #         .sum()
    #         .reset_index()
    #     )
    #     fleet_df = fleet_df.merge(
    #         self.invariant.grid_consumption,
    #         on=["cohort", "vehicle_type", "segment", "fuel"],
    #         how="left",
    #     ).fillna(0)
    #     fleet_df["kwh_consumption"] = fleet_df["kwh_km"] * fleet_df["chainage"]
    #     fleet_df = fleet_df.merge(self.invariant.grid_intensity, on="year", how="left")
    #     fleet_df["grid_gco2"] = fleet_df["kwh_consumption"] * fleet_df["gco2_kwh"]
    #     fleet_df = (
    #         fleet_df.groupby(
    #             ["fuel", "segment", "zone", "vehicle_type", "cohort", "year", "tally"]
    #         )[["tailpipe_gco2", "grid_gco2", "chainage"]]
    #         .sum()
    #         .reset_index()
    #     )
    #
    #     self.projected_fleet = fleet_df
    #     merged_emissions = (
    #         fleet_df.groupby(["vehicle_type", "year"])[
    #             ["tailpipe_gco2", "grid_gco2", "chainage"]
    #         ]
    #         .sum()
    #         .reset_index()
    #     )
    #
    # def save_output(self):
    #     """Save the final output (fleet + chainage + emissions)."""
    #     self.projected_fleet["scenario"] = self.scenario.scenario_code
    #     self.projected_fleet.to_csv(
    #         f"{self.outpath}/{self.run_name}_{self.scenario.scenario_initials}"
    #         f"_fleet_emissions_{self.date}.csv",
    #         index=False,
    #     )
