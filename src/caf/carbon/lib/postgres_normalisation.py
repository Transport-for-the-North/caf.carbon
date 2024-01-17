# Built-Ins
from datetime import datetime

# Third Party
import numpy as np
import pandas as pd


class NormaliseOutputs:

    def __init__(self, summary_outputs_obj, outpath):
        """
        Convert summary outputs into Third Normal Form.

        This makes the table formats more compatible with
        a relational database (PostreSQL).

        summary_outputs_obj: class obj
            Includes scenario_fleet_emissions and other
            summary outputs
        outpath: string
            Filepath to export tables.
        """

        self.summary_outputs = summary_outputs_obj
        self.outpath = outpath
        self.import_lookup_tables = False
        self.date = datetime.today().strftime('%Y_%m_%d')
        self.run_type = self.summary_outputs.run_type
        
        if self.import_lookup_tables:
            if self.run_type == "nelum":
                self.nelum_zone = pd.read_csv(outpath + "postgres_output_tables/data_zonal/nelum_zone_postgres.csv")
            elif self.run_type == "noham":
                self.nelum_zone = pd.read_csv(outpath + "postgres_output_tables/data_zonal/noham_zone_postgres.csv")
            self.dft_segment = pd.read_csv(outpath + "postgres_output_tables/data_common/dft_segment_postgres.csv")
            self.future_travel_scenario = pd.read_csv(outpath + "postgres_output_tables/data_common/future_travel_scenario_postgres.csv")
        else:
            self.__split_lookup_tables()

        self.__link_tables()
        self.__export_tables()
        
    @staticmethod
    def check_df_length(pre_count, post_count):
        if pre_count != post_count:
            print("!!DataFrame has changed in length!!")
        
    @staticmethod
    def add_id_column(table_df):
        """Add a unique id column and reorder columns so 'id' is first."""
        df_columns = list(table_df)
        table_df["id"] = np.arange(1, len(table_df) + 1)
        table_df = table_df[["id"] + df_columns]
        return table_df
    
    @staticmethod
    def replace_vehicle_types(table_df):
        table_df["vehicle_type"] = table_df["vehicle_type"].str.lower()
        table_df["vehicle_type"] = table_df["vehicle_type"].replace("van", "lgv")
        return table_df
        
    def __split_lookup_tables(self):
        """Create lookup tables and add an id column."""
               
        def split_table(parent_table, column_list, new_column_name=None, add_id=True):
            """Subset the dataframe using desired columns and preprocess."""
            table_df = parent_table[column_list].drop_duplicates()
            pre_count = table_df.shape[0]
            if "vehicle_type" in column_list:
                table_df = self.replace_vehicle_types(table_df)
            if new_column_name is not None: 
                table_df = table_df.rename(columns={column_list[0]: new_column_name})
            if add_id:
                table_df = self.add_id_column(table_df)
            table_df = table_df.sort_values(by="id")
            self.check_df_length(pre_count, table_df.shape[0])
            return table_df
        
        ### DATA ZONAL TABLES ###
        if self.run_type == "nelum":
            nelum_zone_info = self.summary_outputs.area_info[["zone",
                                                              "zone_name",
                                                              "area_type",
                                                              "north"]].drop_duplicates()
            nelum_zone_info.to_csv("nelum_zone_info.csv")

            nelum_zone = split_table(nelum_zone_info, ["zone", "zone_name", "area_type", "north"], "id", add_id=False)
            nelum_zone = nelum_zone.rename(columns={"zone_name": "nelum_zone_name",
                                                    "area_type": "nelum_area_type",
                                                    "north": "nelum_north"})
            nelum_zone["nelum_area_type"] = nelum_zone["nelum_area_type"].map({1: "urban",
                                                                               2: "sub-urban",
                                                                               3: "rural"})
            self.nelum_zone = nelum_zone
        
        ### DATA COMMON TABLES ###
        decarb_trajectory = self.add_id_column(self.summary_outputs.decarb_trajectory.copy())
        decarb_trajectory["scenario_full_name"] = "Decarbonisation Trajectory"
        decarb_trajectory = decarb_trajectory.merge(self.summary_outputs.cum_em[["year", "Decarbonisation Trajectory"]],
                                                    on="year",
                                                    how="left")
        self.decarb_trajectory = decarb_trajectory.rename(columns={"Decarbonisation Trajectory": "cmltv_mtco2"})
        
        dft_segment = split_table(self.summary_outputs.all_scenarios, ["segment", "vehicle_type"])
        self.dft_segment = dft_segment[["segment", "vehicle_type"]]
        self.scenario_code_name = {"Just About Managing": "SC01", "Prioritised Places": "SC02", "Digitally Distributed": "SC03", "Urban Zero Carbon": "SC04"}
        scenario_initials = {"Just About Managing": "JAM", "Prioritised Places": "PP", "Digitally Distributed": "DD", "Urban Zero Carbon": "UZC"}
        future_travel_scenario = split_table(self.summary_outputs.all_scenarios, 
                                             ["scenario_full_name"], 
                                             "future_travel_scenario")
        future_travel_scenario["scenario_full_name"] = future_travel_scenario["future_travel_scenario"]
        future_travel_scenario["future_travel_scenario"] = future_travel_scenario["future_travel_scenario"].map(self.scenario_code_name)
        future_travel_scenario["scenario_initial"] = future_travel_scenario["scenario_full_name"].map(scenario_initials)

        self.future_travel_scenario = future_travel_scenario.copy()
               
    def __link_tables(self):
        """Link the main outputs to the split tables."""
                
        def link_on_ids(lookup_table, main_output_table, lookup_column, main_output_column):
            """Replace values in the main input with the corresponding id in the lookup table."""
            lookup_id_dict = dict(zip(lookup_table[lookup_column], lookup_table.id))
            if main_output_column == "vehicle_type":
                main_output_table = self.replace_vehicle_types(main_output_table)
            main_output_table[main_output_column] = main_output_table[main_output_column].map(lookup_id_dict)
            main_output_table[main_output_column] = main_output_table[main_output_column].astype(int)
            main_output_table["year"] = main_output_table["year"].astype(int)
            main_output_table = main_output_table.rename(columns={main_output_column: lookup_column + "_id"})
            return main_output_table
        
        def remove_area_type_north(table_df):
            """Remove area type and north columns. 
            
            They can instead be derived from merging with nelum_zone 
            in Postgres."""
            non_area_cols = [i for i in list(table_df) if i not in ["nelum_area_type", "NELUM area type", "North"]]
            table_df = table_df[non_area_cols]
            return table_df
        
        if not self.import_lookup_tables:          
            ### NELUM population (data demographic)
            nelum_scenario_pop = self.summary_outputs.area_info[["scenario", 
                                                                 "year",
                                                                 "zone", 
                                                                 "population"]].drop_duplicates()
            nelum_scenario_pop = link_on_ids(self.future_travel_scenario, nelum_scenario_pop, "future_travel_scenario", "scenario")
            nelum_scenario_pop = nelum_scenario_pop.rename(columns={"zone": "nelum_zone_id"})        
            self.nelum_scenario_pop = self.add_id_column(nelum_scenario_pop)
        
        ### Scenario fleet emissions ###
        scenario_fleet_emissions = self.summary_outputs.all_scenarios.drop(columns=["tailpipe_mtco2", "grid_mtco2"])
        scenario_fleet_pre_count = scenario_fleet_emissions.shape[0]
        scenario_fleet_emissions = remove_area_type_north(scenario_fleet_emissions)
        scenario_fleet_emissions = self.replace_vehicle_types(scenario_fleet_emissions)
        scenario_fleet_emissions = link_on_ids(self.future_travel_scenario, 
                                               scenario_fleet_emissions, 
                                               "scenario_full_name", 
                                               "scenario_full_name")
        scenario_fleet_emissions = scenario_fleet_emissions.rename(columns={"zone": "nelum_zone_id", 
                                                                            "scenario_full_name_id": "future_travel_scenario_id"})
        scenario_fleet_emissions["chainage"] = scenario_fleet_emissions["chainage"] * 1e9
        num_zev = scenario_fleet_emissions.loc[scenario_fleet_emissions.fuel.isin(["bev", "hydrogen"])].groupby(["future_travel_scenario_id", "year", "nelum_zone_id"])["tally"].sum().reset_index()
        num_zev = num_zev.rename(columns={"tally": "num_zev"})
        scenario_fleet_emissions = scenario_fleet_emissions.merge(num_zev, on=["future_travel_scenario_id", "year", "nelum_zone_id"], how="left").fillna(0)
        scenario_fleet_emissions = self.add_id_column(scenario_fleet_emissions)
        self.scenario_fleet_emissions = scenario_fleet_emissions[["id", 
                                                                  "future_travel_scenario_id",
                                                                  "year", 
                                                                  "nelum_zone_id",
                                                                  "vehicle_type",
                                                                  "segment",
                                                                  "fuel",
                                                                  "cohort",
                                                                  "tailpipe_gco2",
                                                                  "grid_gco2",
                                                                  "chainage",
                                                                  "tally",
                                                                  "num_zev"]]
        self.check_df_length(scenario_fleet_pre_count, self.scenario_fleet_emissions.shape[0])

        ### Carbon scenario attributes ###
        carbon_scenario_attributes = self.summary_outputs.attributes.copy()
        scenario_attributes_pre_count = carbon_scenario_attributes.shape[0]
        carbon_scenario_attributes = remove_area_type_north(carbon_scenario_attributes)
        carbon_scenario_attributes = carbon_scenario_attributes.rename(columns={"Year": "year"})
        carbon_scenario_attributes = link_on_ids(self.future_travel_scenario, carbon_scenario_attributes, "future_travel_scenario", "Scenario")
        attribute_column_rename = {"Zone": "nelum_zone_id", 
                                   "NELUM area type": "nelum_area_type",
                                   "North": "nelum_north",
                                   "Total MTCO2": "tailpipe_mtco2",
                                   "CO2_pc_change": "tailpipe_mtco2_pc_change",
                                   "Car MTCO2": "car_tailpipe_mtco2",
                                   "LGV HGV MTCO2": "lgv_hgv_tailpipe_mtco2",
                                   "Emissions per head": "tonnes_tailpipe_co2_per_capita",
                                   "Emissions Intensity": "tailpipe_gco2_km_intensity",
                                   "Total km": "chainage",
                                   "Vehiclekm per head": "chainage_per_capita",
                                   "Num Cars": "tally"}
        carbon_scenario_attributes = carbon_scenario_attributes.rename(columns=attribute_column_rename)
        carbon_scenario_attributes = carbon_scenario_attributes.merge(num_zev, on=["future_travel_scenario_id", "year", "nelum_zone_id"], how="left").fillna(0)
        self.carbon_scenario_attributes = self.add_id_column(carbon_scenario_attributes)    
        self.check_df_length(scenario_attributes_pre_count, self.carbon_scenario_attributes.shape[0])

        ### Cumulative emissions ###
        scenario_names = list(self.scenario_code_name.keys())
        cum_em = self.summary_outputs.cum_em[["year"] + scenario_names].copy()
        cum_em = cum_em.melt(id_vars="year", 
                             value_vars=scenario_names, 
                             var_name="scenario",
                             value_name="cmltv_mtco2")
        cum_em_pre_count = cum_em.shape[0]
        cum_em["scenario"] = cum_em["scenario"].map(self.scenario_code_name)
        cum_em = link_on_ids(self.future_travel_scenario, 
                             cum_em,
                             "future_travel_scenario", 
                             "scenario")
        self.cum_em = self.add_id_column(cum_em)
        self.check_df_length(cum_em_pre_count, self.cum_em.shape[0])
        
        ### PT emissions ###
        pt_emissions = self.summary_outputs.pt_emissions.copy()
        pt_pre_count = pt_emissions.shape[0]
        pt_emissions = link_on_ids(self.future_travel_scenario, pt_emissions, "future_travel_scenario", "scenario")
        pt_emissions = self.add_id_column(pt_emissions)
        self.pt_emissions = pt_emissions[["id", "future_travel_scenario_id", "year", "vehicle_type", "tailpipe_mtco2"]]
        self.check_df_length(pt_pre_count, self.pt_emissions.shape[0])
        
        ### Fuel share ###
        fuel_share = self.summary_outputs.fuel_share.copy()
        fuel_share_pre_count = fuel_share.shape[0]
        fuel_share = link_on_ids(self.future_travel_scenario, fuel_share, "future_travel_scenario", "scenario")
        self.fuel_share = self.add_id_column(fuel_share)
        self.check_df_length(fuel_share_pre_count, self.fuel_share.shape[0])
        
    def __export_tables(self):
        """Export tables so they can be imported into Postgres."""
        outpath = self.outpath + "postgres_output_tables/"
                   
        if not self.import_lookup_tables:
            self.nelum_zone.to_csv(outpath + "data_zonal/nelum_zone_postgres.csv", 
                                   index=False)
            self.decarb_trajectory.to_csv(outpath + "data_common/decarb_trajectory_postgres.csv",
                                          index=False)
            self.dft_segment.to_csv(outpath + "data_common/dft_segment_postgres.csv", 
                                    index=False)
            self.future_travel_scenario.to_csv(outpath + "data_common/future_travel_scenario_postgres.csv", 
                                               index=False)
            self.nelum_scenario_pop.to_csv(outpath + "data_demographic/nelum_scenario_pop_postgres.csv", 
                                           index=False)

        if self.summary_outputs.pathway != "":
            outpath = outpath + "pathway_outputs/"
        
        self.scenario_fleet_emissions.to_csv(outpath + "data_zone_travel/scenario_fleet_emissions_postgres_{self.run_type}_{self.date}.csv".format(**locals()), 
                                             index=False)
        self.carbon_scenario_attributes.to_csv(outpath + "data_zone_travel/carbon_scenario_attributes_postgres_{self.run_type}_{self.date}.csv".format(**locals()), 
                                               index=False)
        
        self.cum_em.to_csv(outpath + "data_travel/cumulative_mtco2_postgres_{self.run_type}_{self.date}.csv".format(**locals()), 
                           index=False)
        self.pt_emissions.to_csv(outpath + "data_travel/pt_emissions_postgres_{self.run_type}_{self.date}.csv".format(**locals()), 
                                 index=False)
        self.fuel_share.to_csv(outpath + "data_travel/fuel_share_postgres_{self.run_type}_{self.date}.csv".format(**locals()), 
                               index=False)
