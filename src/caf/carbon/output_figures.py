# Built-Ins
import configparser as cf
import os
import re
from datetime import datetime

# Third Party
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Local Imports
from src.caf.carbon import utility as ut
from src.caf.carbon.load_data import (
    NOHAM_AREA_TYPE,
    NOHAM_TO_MSOA,
    OUT_PATH,
    TARGET_AREA_TYPE,
)


class SummaryOutputs:
    """Combine scenario outputs and transform into summary outputs."""

    def __init__(self, pathway, invariant_obj, scenario_obj):
        """
        
        Parameters
        ----------
        pathway : str
            Determines whether outputs relate to 'pathway' or standard 
            scenario inputs (used for filenames).
        invariant_obj : class obj
            Includes the baseline fleet and scenario invariant tables.
        scenario_obj : class obj
            Includes scenario tables for the most scenario run through NoCarb.
        """
        self.time_period_list = time_period_list
        self.outpath = OUT_PATH
        self.invariant = invariant_obj
        self.last_scenario = scenario_obj
        self.date = datetime.today().strftime('%Y_%m_%d')
        self.run_type = invariant_obj.run_type.lower()

        local_drive_date = os.listdir("CAFCarb/outputs")[-1][-14:-4]
        self.date = local_drive_date

        if pathway == "none":
            self.pathway = ""
        else:
            self.pathway = pathway

        self.__gis_attributes_fuel_share()
        self.__load_pt()

    def __gis_attributes_fuel_share(self):
        """Prepare two summary tables that summarise outputs across all scenarios.
        
        1. A GIS atttributes table : aggregated information by msoa zone.
        2. A 2050 fuel share table.
        """

        # Convert the noham area information into msoa zones
        area_info = pd.read_csv(NOHAM_AREA_TYPE).rename(columns={'zone': 'nohamZoneID'})
        msoa_zones = pd.read_csv(NOHAM_TO_MSOA)
        area_zones = pd.read_csv(TARGET_AREA_TYPE).rename(columns={'msoa_area_code': 'msoa11cd'})

        msoa_convert = msoa_zones[['nohamZoneID', 'msoa11cd', 'overlap_noham_split_factor']].merge(area_info,
                                                                                                   on='nohamZoneID',
                                                                                                   how='left')
        msoa_convert['population'] = msoa_convert['population'] * msoa_convert['overlap_noham_split_factor']
        msoa_convert = msoa_convert.sort_values(by=['msoa11cd', 'year'], ascending=True).reset_index(drop=True)
        msoa_convert = msoa_convert.groupby(['msoa11cd', 'area_type', 'north', 'year', 'scenario']).sum().reset_index()
        msoa_convert = msoa_convert[['scenario','msoa11cd', 'north', 'year', 'population']]
        area_info = msoa_convert.merge(area_zones[['msoa11cd', 'tfn_area_type']], on='msoa11cd', how='left').rename(
            columns={'msoa11cd': 'zone', 'tfn_area_type': 'area_type'})
        area_info = area_info[["scenario", "zone", "north", "area_type", "year", "population"]]

        self.area_info = area_info.copy()
        area_info = ut.interpolate_timeline(
            area_info,
            grouping_vars=["scenario", "zone", "north", "area_type"],
            value_var="population",
            melt=False)
        area_info.to_csv("interpolated_area_info.csv")

        self.scenario_rename = {"BAU": "SC01", "AE": "SC02", "BAUH": "SC03", "AEH": "SC04","BAUL": "SC05",
                                "AEL": "SC06", "Business As Usual Core": "SC01", "Accelerated EV Core": "SC02"}
        scenario_full_name = {"SC01": "Business As Usual Core",
                              "SC02": "Accelerated EV Core"}

        # Set empty DataFrames to populate with data from all scenarios
        all_scenarios = pd.DataFrame()
        attributes = pd.DataFrame()
        for i in ["BAU", "AE"]:
            if i == self.last_scenario.scenario.scenario_initials:
                scenario_df = self.last_scenario.projected_fleet.copy()
            else:
                scenario_df = pd.read_csv(
                    "{self.outpath}{i}_fleet_emissions_{self.run_type}_{self.date}_{time}.csv".format(**locals()))
            all_scenarios = all_scenarios.append(scenario_df, ignore_index=True)
            scenario_df.loc[scenario_df["vehicle_type"].isin(["lgv", "hgv"]), "vehicle_type"] = "lgv_hgv"
            scenario_df = scenario_df.groupby(["scenario", "zone", "vehicle_type", "year"])[
                ["tally", "chainage", "tailpipe_gco2"]].sum().reset_index()
            attributes = pd.concat([attributes, scenario_df], ignore_index=True)

        all_scenarios["scenario"] = all_scenarios["scenario"].map(scenario_full_name)
        all_scenarios = all_scenarios.rename(columns={"scenario": "scenario_full_name"})
        self.all_scenarios = all_scenarios.copy()

        # Transform columns to save summary information for each zone
        attributes["tally_sum"] = attributes.groupby(["scenario", "year", "zone"])["tally"].transform("sum")
        attributes["chainage_sum"] = attributes.groupby(["scenario", "year", "zone"])["chainage"].transform("sum")
        attributes["tailpipe_gco2"] = attributes["tailpipe_gco2"] * 1e-12  # MTCO2
        attributes["tailpipe_mt_co2_sum"] = attributes.groupby(["scenario", "year", "zone"])["tailpipe_gco2"].transform(
            "sum")
        attributes = attributes.pivot_table(index=["zone",
                                                   "scenario",
                                                   "year",
                                                   "tally_sum",
                                                   "chainage_sum",
                                                   "tailpipe_mt_co2_sum"],
                                            columns="vehicle_type",
                                            values="tailpipe_gco2").reset_index()
        attributes["co2_pc_change"] = attributes.groupby(["scenario", "zone"], sort=False)["tailpipe_mt_co2_sum"].apply(
            lambda x: x.div(x.iloc[0]).subtract(1)).fillna(0)
        attributes["kms_pc_change"] = attributes.groupby(["scenario", "zone"], sort=False)["chainage_sum"].apply(
            lambda x: x.div(x.iloc[0]).subtract(1)).fillna(0)

        attributes = attributes.merge(area_info, how="left", on=["zone", "year", "scenario"])
        attributes = attributes.dropna(subset=["population"])
        attributes["Emissions Intensity"] = 1e12 * (
                    attributes["tailpipe_mt_co2_sum"] / attributes["chainage_sum"]).fillna(0)  # g/km
        attributes["Emissions pp"] = 1e6 * attributes["tailpipe_mt_co2_sum"] / attributes[
            "population"]  # tonnes / capita / year
        attributes["Vehicle-kms pp"] = attributes["chainage_sum"] / attributes["population"]
        attributes = attributes.drop(columns=["population"])

        new_cols = ["Zone", "Scenario", "Year", "Num Cars", "Total km", "Total MTCO2",
                    "Car MTCO2", "LGV HGV MTCO2", "CO2_pc_change", "km_pc_change", "North",
                    "area type", "Emissions Intensity", "Emissions per head", "Vehiclekm per head"]

        rename_cols = dict(zip(attributes.columns.values, new_cols))
        attributes = attributes.rename(columns=rename_cols)
        attributes = attributes[["Scenario", "Zone", "area type", "North", "Year",
                                 "Total MTCO2", "CO2_pc_change", "Car MTCO2", "LGV HGV MTCO2",
                                 "Emissions per head", "Emissions Intensity", "Total km",
                                 "km_pc_change", "Vehiclekm per head", "Num Cars"]]

        attributes.to_csv("{self.outpath}carbon_scenario_attributes_{self.run_type}_{self.date}.csv".format(**locals()),
                          index=False)
        self.attributes = attributes.copy()

        # Produce the fuel share in 2050 for each scenario
        fuel_share = all_scenarios.groupby(["scenario_full_name", "year", "vehicle_type", "fuel"])[
            "tally"].sum().reset_index()
        fuel_share["scenario"] = fuel_share["scenario_full_name"].map(self.scenario_rename)
        fuel_share["fuel_share"] = fuel_share["tally"] / fuel_share.groupby(["scenario", "year", "vehicle_type"])[
            "tally"].transform("sum")
        fuel_share = fuel_share[["scenario", "year", "vehicle_type", "fuel", "fuel_share"]]

        fuel_share.to_csv("{self.outpath}fuel_share_{self.run_type}_{self.date}.csv".format(**locals()), index=False)
        self.fuel_share = fuel_share.copy()

    def __load_pt(self):
        """Calculate pan_Northern bus and rail emissions in future years.
        
        Apply scenario-based CO2e reduction factors to baseline figures. 
        Save as a summary table.
        """
        pt = pd.DataFrame()
        if self.pathway == "":
            file_name = "scenarioFile"
        else:
            file_name = "scenario" + self.pathway.capitalize()

        config = cf.ConfigParser(interpolation=cf.ExtendedInterpolation())
        config.read("config_local.txt")

        for i in ["Business As Usual Core", "Accelerated EV Core"]:
            pt_emission_index_reductions = pd.read_excel(
                io=config["filePaths"][file_name],
                sheet_name=i,
                header=1,
                usecols=config["fileStructure"]["ptEmissionReduction"]).dropna()
            pt_emission_index_reductions = pt_emission_index_reductions.rename(
                columns=lambda x: re.sub("\.[0-9]$", "", str(x)))
            pt_emission_index_reductions = ut.camel_columns_to_snake(pt_emission_index_reductions)
            pt_emission_index_reductions[str(self.invariant.index_year)] = 0
            pt_emission_index_reductions = pd.melt(pt_emission_index_reductions,
                                                   id_vars="vehicle_type",
                                                   var_name="year",
                                                   value_name="co2_reduction")
            pt_emission_index_reductions["scenario"] = self.scenario_rename[i]
            pt_emission_index_reductions["co2_reduction"] = 1 - pt_emission_index_reductions["co2_reduction"]
            pt_emission_index_reductions["scenario_full_name"] = i
            pt = pt.append(pt_emission_index_reductions, ignore_index=True)

        pt_emissions = pd.read_excel(io=config["filePaths"]["generalFile"],
                                     sheet_name="general",
                                     header=1,
                                     usecols=config["fileStructure"]["baselinePT"]).dropna()
        pt_emissions = pt_emissions.rename(columns=lambda x: re.sub("\.[0-9]$", "", str(x)))
        pt_emissions = ut.camel_columns_to_snake(pt_emissions)

        pt_emissions = pt.merge(pt_emissions, how="left", on="vehicle_type")
        pt_emissions["mt_co2"] = pt_emissions["mt_co2"] * pt_emissions["co2_reduction"]
        pt_emissions = pt_emissions.rename(columns={"mt_co2": "tailpipe_mtco2"})
        pt_emissions["vehicle_type"] = pt_emissions["vehicle_type"].str.lower()
        pt_emissions["year"] = pt_emissions["year"].astype(int)

        pt_ghg_factor = self.invariant.pt_ghg_factor

        for i in range(len(pt_ghg_factor)):
            pt_emissions.loc[
                (pt_emissions["vehicle_type"] == pt_ghg_factor.loc[i]["vehicle_type"]),
                "pt_ghg_factor"] = pt_ghg_factor.loc[i]["factor"]

        pt_emissions["tailpipe_mtco2"] = pt_emissions["tailpipe_mtco2"] * pt_emissions["pt_ghg_factor"]

        pt_emissions[["scenario", "year", "vehicle_type", "tailpipe_mtco2"]].to_csv(
            "{self.outpath}pt_emissions_{self.run_type}_{self.date}.csv".format(**locals()), index=False)
        self.pt_emissions = pt_emissions.copy()

    def plot_effects(self):
        """Plot CO2e and chainage using different aggregation levers."""

        def plot_total(value, pt=False, decarb_trajectory=False):
            """Plot total CO2e, chainage and grid CO2e."""
            if pt:
                table_df = self.co2_by_mode.copy()
            else:
                table_df = self.all_scenarios.copy()

            self.col_match = dict(
                zip(["Business As Usual Core", "Accelerated EV Core"],
                    ["indigo", "violet"]))
            val_name = {"tailpipe_mtco2": ["Total emissions", "Megatonnes of CO" + r"$_2$" + "e"],
                        "chainage": ["Total vehicle kms", "Vehicle kms (billions)"],
                        "grid_mtco2": ["Indirect emissions", "Megatonnes of CO" + r"$_2$" + "e"]}
            max_y_lim = {"tailpipe_mtco2": 28, "chainage": 200, "grid_mtco2": 0.5}

            plt.figure(figsize=(10, 5))
            for sc in table_df.scenario_full_name.unique():
                plotting_df = table_df.loc[table_df["scenario_full_name"] == sc].groupby("year")[
                    value].sum().reset_index()
                plt.plot(plotting_df.year, plotting_df[value], color=self.col_match[sc], label=sc)
            plt.title("{} in each scenario".format(val_name[value][0]))
            plt.ylabel(val_name[value][1])
            plt.xlabel("Year")
            plt.xlim(self.invariant.index_year, 2050)
            plt.ylim(0, max_y_lim[value])
            if decarb_trajectory:
                plt.plot(self.decarb_trajectory.year,
                         self.decarb_trajectory["decarb_tailpipe_mtco2"],
                         "--",
                         color="black",
                         label="Decarbonisation trajectory")
                plt.legend()
                plt.tight_layout()
                save_location = "{self.outpath}figures/total {value} + decarb traj {self.pathway}.png".format(
                    **locals())
                plt.savefig(save_location)
                plt.show()
            else:
                plt.legend()
                plt.tight_layout()
                save_location = "{self.outpath}figures/total {value} {self.pathway}.png".format(**locals())
                plt.savefig(save_location)
            plt.close("all")

        def plot_cumulative():
            """Plot cumulative emissions against the decarbonisation trajectory."""
            table_df = self.co2_by_mode.copy()

            decarb_trajectory = self.decarb_trajectory.copy()
            missing_years = [i for i in np.arange(2019, 2050) if i not in decarb_trajectory.year.unique()]
            decarb_trajectory = decarb_trajectory.append(pd.DataFrame({"year": missing_years,
                                                                       "decarb_tailpipe_mtco2": [np.nan] * len(
                                                                           missing_years)}),
                                                         ignore_index=True)
            decarb_trajectory = decarb_trajectory.sort_values(by="year")

            cum_em = table_df.pivot_table(index="year",
                                          columns="scenario_full_name",
                                          values="tailpipe_mtco2",
                                          aggfunc="sum").reset_index()
            cum_em = decarb_trajectory.merge(cum_em, on="year", how="left")
            cum_em = cum_em.rename(columns={"decarb_tailpipe_mtco2": "Decarbonisation Trajectory"})
            cum_em = cum_em.set_index("year")
            cum_em = cum_em.interpolate()
            cum_em = cum_em.cumsum()
            self.cum_em = cum_em.reset_index()

            plt.figure(figsize=(10, 5))
            for i in self.col_match.keys():
                plt.plot(cum_em.index.values,
                         cum_em[i].values,
                         color=self.col_match[i],
                         label=i)
            plt.plot(cum_em.index.values,
                     cum_em["Decarbonisation Trajectory"].values,
                     "--",
                     color="black",
                     label="Decarbonisation Trajectory")
            plt.title("Cumulative emissions by scenario compared to the decabonisation trajectory")
            plt.ylabel("Megatonnes of CO" + r"$_2$" + "e")
            plt.xlabel("Year")
            plt.xlim(self.invariant.index_year, 2050)
            plt.ylim(0)
            plt.legend()
            plt.tight_layout()

            save_location = "{self.outpath}figures/cumulative emissions {self.pathway}.png".format(
                **locals())
            plt.savefig(save_location)
            plt.close("all")

        def plot_scenario(scenario, grouping, value, pt=False):
            """Plot CO2e and chainage by mode and area type for a given scenario."""
            self.folder_name = {"Business As Usual Core": "SC01", "Accelerated EV Core": "SC02"}
            folder = self.folder_name[scenario]

            if pt:
                plotting_df = self.co2_by_mode.copy()
            else:
                plotting_df = self.all_scenarios.copy()

            plotting_df = plotting_df.loc[plotting_df["scenario_full_name"] == scenario]
            plotting_df = plotting_df.groupby(["year", grouping])[value].sum().reset_index()
            original_vals = plotting_df[grouping].unique()

            # Add additional rows to ensure the legend colours are the same across plots
            if grouping == "vehicle_type":
                plotting_df = plotting_df.append(
                    pd.DataFrame({"year": [self.invariant.index_year, 2020, 2025, 2030, 2035, 2040, 2045, 2050] * 3,
                                  "vehicle_type": (["Bus"] * 8) + (["Rail"] * 8) + (["Z"] * 8),
                                  value: [0] * 24}))
            else:
                plotting_df = plotting_df.append(
                    pd.DataFrame({"year": [self.invariant.index_year, 2020, 2025, 2030, 2035, 2040, 2045, 2050],
                                  "nelum_area_type": ["Z"] * 8,
                                  value: [0] * 8}))
            plotting_df = plotting_df.pivot_table(index="year",
                                                  columns=grouping,
                                                  values=value).reset_index()

            value_label = {"chainage": "Vehicle kms",
                           "tailpipe_mtco2": "Megatonnes of CO" + r"$_2$" + "e"}[value]

            grouping_label = {"area_type": "area type",
                              "vehicle_type": "vehicle type"}[grouping]

            ax = plotting_df.plot.area(x="year", linewidth=0, cmap="viridis", alpha=0.9)
            plt.title("{value_label} by {grouping_label}: {scenario}\n".format(**locals()))
            plt.ylabel(value_label if value == "tailpipe_mtco2" else "Vehicle kms (billions)")
            plt.xlabel("Year")
            plt.xlim(self.invariant.index_year, 2050)
            handles, labels = ax.get_legend_handles_labels()
            legend_vals = [(h, l) for i, (h, l) in enumerate(zip(handles, labels)) if l in original_vals]
            ax.legend(*zip(*legend_vals))
            plt.tight_layout()

            save_location = "{self.outpath}figures/{folder}/{value} by {grouping_label} for {folder}.png".format(
                **locals())
            plt.savefig(save_location, bbox_inches="tight")
            plt.close("all")

        # Prepare the summary emissions / chainage df for plotting
        # Import info related to Area type
        msoa_area_info = self.area_info.copy()
        msoa_area_info = msoa_area_info[["zone", "area_type"]].drop_duplicates(subset="zone")
        msoa_area_info["area_type"] = msoa_area_info["area_type"].replace(
            {1: "Inner Urban A",
            2: "Inner Urban B",
            3: "City Suburban A",
            4: "City Suburban B",
            5: "Urban Large (within urban area with pop 100-250k)",
            6: "Urban Medium (within urban area with pop 25-100k)",
            7: "Urban Small (within urban area with pop 25-100k)",
            8: "Rural"})

        all_scenarios = self.all_scenarios.copy()
        all_scenarios = all_scenarios.merge(msoa_area_info,
                                            how="left",
                                            on="zone",
                                            validate="many_to_one")

        all_scenarios["tailpipe_mtco2"] = all_scenarios["tailpipe_gco2"] * 1e-12
        all_scenarios["grid_mtco2"] = all_scenarios["grid_gco2"] * 1e-12
        all_scenarios["chainage"] = all_scenarios["chainage"] / (10 ** 9)
        vtype_replace = {"car": "Car", "hgv": "HGV", "lgv": "Van"}
        all_scenarios["vehicle_type"] = all_scenarios["vehicle_type"].replace(vtype_replace)
        self.all_scenarios = all_scenarios.copy()

        # Save the decarbonisation trajectory for plotting emissions
        decarb_reductions = np.array([1, 0.7, 0.45, 0.2, 0.05, 0.015, 0.0])
        baseline_mtco2 = all_scenarios.loc[(all_scenarios.year == self.invariant.index_year) & (
                    all_scenarios.scenario_full_name == "Business As Usual Core"), "tailpipe_mtco2"].sum() + \
                         self.pt_emissions.loc[(self.pt_emissions.year == self.invariant.index_year) & (
                                     self.pt_emissions.scenario_full_name == "Business As Usual Core"), "tailpipe_mtco2"].sum()
        decarb_trajectory = baseline_mtco2 * decarb_reductions
        self.decarb_trajectory = pd.DataFrame({"year": [self.invariant.index_year, 2025, 2030, 2035, 2040, 2045, 2050],
                                               "decarb_tailpipe_mtco2": decarb_trajectory})

        co2_by_mode = all_scenarios.groupby(["year", "scenario_full_name", "vehicle_type"])[
            "tailpipe_mtco2"].sum().reset_index()
        co2_by_mode = co2_by_mode.append(
            self.pt_emissions[["year", "scenario_full_name", "vehicle_type", "tailpipe_mtco2"]], ignore_index=True)
        co2_by_mode["vehicle_type"] = co2_by_mode["vehicle_type"].str.capitalize()
        self.co2_by_mode = co2_by_mode.copy()

        # Produce the plots
        for i in all_scenarios.scenario_full_name.unique():
            if self.run_type == "msoa":
                plot_scenario(i, "vehicle_type", "tailpipe_mtco2", pt=True)  # Add PT
                plot_scenario(i, "area_type", "tailpipe_mtco2")
                plot_scenario(i, "vehicle_type", "chainage")
                plot_scenario(i, "area_type", "chainage")

        plot_total("tailpipe_mtco2", pt=True, decarb_trajectory=False)
        plot_total("tailpipe_mtco2", pt=True, decarb_trajectory=True)
        plot_total("chainage", pt=False, decarb_trajectory=False)
        plot_total("grid_mtco2", pt=False, decarb_trajectory=False)
        plot_cumulative()

    def plot_fleet(self):
        """Preprocess fleet data and plot the fuel share of fleet and sales."""

        def plot_fleetorsales(scenario, vehicle_type, fleet_or_sales):
            """Plot the fuel share of the fuel or sales in future years."""
            if fleet_or_sales == "fleet":
                plotting_df = self.fleet_df
            else:
                plotting_df = self.sales_df

            folder = self.folder_name[scenario]
            plotting_df = plotting_df.loc[
                (plotting_df["scenario_full_name"] == scenario) & (plotting_df["vehicle_type"] == vehicle_type)]

            # Ensure all fuels are present so the colour scheme in the plot
            # is the same across plots
            original_fuels = plotting_df.fuel.unique()
            missing_fuels = [i for i in self.fuel_rename.values() if i not in original_fuels]
            for i in missing_fuels:
                plotting_df = plotting_df.append(
                    pd.DataFrame({"year": [self.invariant.index_year, 2020, 2025, 2030, 2035, 2040, 2045, 2050],
                                  "fuel": ([i] * 8),
                                  "share": [0] * 8}))

            plotting_df = plotting_df.pivot_table(index="year",
                                                  columns="fuel",
                                                  values="share").reset_index()
            ax = plotting_df.plot.area(x="year",
                                       linewidth=0,
                                       colormap="viridis",
                                       alpha=0.9)

            plt.title("Share of {vehicle_type} {fleet_or_sales} by fuel: {scenario}\n".format(**locals()))
            plt.ylabel("Percentage of {vehicle_type} {fleet_or_sales}".format(**locals()))
            plt.xlabel("Year")
            plt.xlim(self.invariant.index_year, 2050)
            plt.ylim(0, 100)

            handles, labels = ax.get_legend_handles_labels()
            legend_vals = [(h, l) for i, (h, l) in enumerate(zip(handles, labels)) if l in original_fuels]
            ax.legend(*zip(*legend_vals))

            plt.tight_layout()

            save_location = "{self.outpath}figures/{folder}/{vehicle_type} {fleet_or_sales} for {folder}.png".format(
                **locals())
            plt.savefig(save_location)
            plt.close("all")

        fleet_df = self.all_scenarios.copy()
        fleet_df = fleet_df.groupby(["scenario_full_name", "cohort", "vehicle_type", "year", "fuel"])[
            "tally"].sum().reset_index()
        fleet_df.loc[fleet_df["vehicle_type"].isin(["Car", "Van"]), "vehicle_type"] = "car and lgv"
        self.fuel_rename = {"bev": "BEV", "phev": "PHEV", "diesel": "Diesel", "petrol": "Petrol",
                            "petrol hybrid": "Petrol hybrid", "hydrogen": "Hydrogen"}
        fleet_df["fuel"] = fleet_df["fuel"].replace(self.fuel_rename)

        # Share of fuels in vehicle sales
        sales_df = fleet_df.loc[fleet_df["cohort"] == fleet_df["year"]].copy()
        sales_df["share"] = sales_df["tally"] / sales_df.groupby(["year", "scenario_full_name", "vehicle_type"])[
            "tally"].transform("sum") * 100
        sales_df = sales_df.groupby(["year", "fuel", "scenario_full_name", "vehicle_type"])["share"].sum().reset_index()
        self.sales_df = sales_df.copy()

        # Share of fuels in the fleet
        fleet_df["share"] = fleet_df["tally"] / fleet_df.groupby(["year", "scenario_full_name", "vehicle_type"])[
            "tally"].transform("sum") * 100
        fleet_df = fleet_df.groupby(["year", "fuel", "scenario_full_name", "vehicle_type"])["share"].sum().reset_index()
        self.fleet_df = fleet_df.copy()

        # Produce plots
        for i in self.fleet_df.scenario_full_name.unique():
            plot_fleetorsales(i, "HGV", "fleet")
            plot_fleetorsales(i, "HGV", "sales")
            plot_fleetorsales(i, "car and lgv", "fleet")
            plot_fleetorsales(i, "car and lgv", "sales")
