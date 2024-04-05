# Third Party
import numpy as np
import pandas as pd
from sklearn import impute
from sklearn.experimental import enable_iterative_imputer

# Local Imports
from src.caf.carbon import utility as ut
from src.caf.carbon.load_data import (
    ANPR_DATA,
    DEMAND_PATH,
    DVLA_BODY,
    MSOA_AREA_TYPE,
    MSOA_BODY,
    MSOA_LAD,
    NOHAM_TO_MSOA,
    OUT_PATH,
    POSTCODE_MSOA,
    TARGET_AREA_TYPE,
    VEHICLE_PATH,
)


# %% Helper Classes
class Imputation:
    """Impute missing values in the DfT fleet data."""

    @staticmethod
    def fill_with_mean(fleet_df, key_attribute):
        """Replace NAs with the mean of others with the same CYA, Fuel and Segment."""
        fleet_df = fleet_df.copy()
        key_attribute_imputed = "Impute_" + key_attribute
        impute_df = ut.weighted_mean(
            fleet_df, ["cya", "fuel", "segment"], "tally", key_attribute
        )

        # Unknown is not a real segment so should not undergo averaging
        impute_df.loc[impute_df["segment"] == "unknown", key_attribute] = np.nan
        impute_df = impute_df.rename(columns={key_attribute: key_attribute_imputed})
        impute_df = impute_df.drop("tally", axis=1)

        # Merge the imputation in, if its BEV then just set the value to 0
        fleet_df = pd.merge(fleet_df, impute_df, on=["cya", "fuel", "segment"], how="left")
        fleet_df[key_attribute] = fleet_df[key_attribute].fillna(
            fleet_df[key_attribute_imputed]
        )
        fleet_df.loc[
            (fleet_df[key_attribute].isnull()) & (fleet_df["fuel"] == "bev"), key_attribute
        ] = 0
        fleet_df = fleet_df.drop([key_attribute_imputed], axis=1)
        return fleet_df

    @staticmethod
    def fill_with_mice(fleet_df):
        """Use MICE to impute vehicle characteristics."""
        fleet_df = fleet_df.copy()
        impute_df = fleet_df[["avg_co2", "avg_mass", "avg_es", "segment", "cya", "fuel"]]
        impute_df = pd.get_dummies(impute_df)
        impute_matrix = impute.IterativeImputer(random_state=0).fit_transform(impute_df)
        impute_df[:] = impute_matrix
        # Integrate values back into the dataframe
        fleet_df[["avg_co2", "avg_mass", "avg_es"]] = impute_df[
            ["avg_co2", "avg_mass", "avg_es"]
        ].astype("float")
        return fleet_df

    @staticmethod
    def segment_by_mass(fleet_df, vehicle_type):
        """Assign segment to unknowns by comparing mass with the mass of known segments."""
        fleet_df = fleet_df.copy()
        reduced_fleet_df = fleet_df.loc[
            fleet_df["vehicle_type"] == vehicle_type, ["segment", "avg_mass"]
        ]

        if vehicle_type == "car":
            # Use the 25th percentile of mass for each car segment as the bin boundaries.
            mass_quantiles = reduced_fleet_df.groupby("segment")["avg_mass"].quantile(0.25)
            mass_quantiles = mass_quantiles.sort_values(  # .drop("unknown")
                ascending=False
            ).reset_index(level=0)
            mass_quantiles = mass_quantiles.iloc[::-1]
            mass_labels = mass_quantiles["segment"].tolist()
            mass_quantiles.iloc[0, mass_quantiles.columns.get_loc("avg_mass")] = 0
            mass_quantiles = mass_quantiles["avg_mass"].tolist()
            mass_quantiles.append(10e10)
        elif vehicle_type == "lgv":
            # Use the legal definitions of lgv classes as bin boundaries.
            # Missing the N2 classes
            mass_labels = ["n1 class i", "n1 class ii", "n1 class iii"]
            mass_quantiles = [0, 1305, 1760, 10e10]
        else:
            raise ValueError(f"invalid vehicle type:{vehicle_type}")
        # Convert to an interval object [start, end)
        mass_quantiles = pd.IntervalIndex.from_breaks(mass_quantiles, closed="left")
        mapping = dict(zip(mass_quantiles, mass_labels))
        fleet_df["label"] = pd.cut(fleet_df["avg_mass"], bins=mass_quantiles).map(mapping)
        fleet_df["label"] = (
            fleet_df["label"].values.add_categories("unknown").fillna("unknown")
        )
        fleet_df.loc[
            (fleet_df["segment"] == "unknown") & (fleet_df["vehicle_type"] == vehicle_type),
            "segment",
        ] = fleet_df["label"]
        fleet_df = fleet_df.drop(["label"], axis=1)
        return fleet_df


class CurveFitting:
    """Fit curves for vehicle scrappage and speed-band emissions."""

    @staticmethod
    def generate_scrappage(invariant_obj):
        """Generate vehicle_type specific scrappage curves using historic fleet data."""
        # Select the middle year within the cya group and give it an equal share of the tally.
        fleet_archive = invariant_obj.index_fleet.fleet_archive.copy()
        fleet_archive = (
            fleet_archive.groupby(["year", "cya", "vehicle_type"])["tally"].sum().reset_index()
        )
        fleet_archive["cya"] = (
            fleet_archive["cya"]
            .astype("str")
            .str.split(",")
            .apply(lambda x: [int(j) for j in x])
        )
        fleet_archive["cohort_tally"] = fleet_archive["tally"] / fleet_archive["cya"].map(len)
        fleet_archive["cya_group"] = fleet_archive["cya"]
        fleet_archive["cya"] = pd.DataFrame(fleet_archive["cya"].values.tolist()).median(
            1
        )  # !!!
        fleet_archive["cya"] = np.floor(fleet_archive["cya"])

        # Create cya_group to cya_column mapping
        groupings = fleet_archive.copy()
        groupings = groupings[["cya_group", "cya"]]
        groupings = groupings.drop_duplicates(subset="cya").explode("cya_group")

        # Calculate how many years into the group the selected year is
        # and the number of years until the end of the group
        # e.g. cya = 17 is in the 15-20 group so its 3rd in the group, 3 from the end
        fleet_archive["cohort"] = fleet_archive["year"] - fleet_archive["cya"]
        fleet_archive = fleet_archive.sort_values(["cohort", "cya"], ascending=[True, True])
        fleet_archive["position_in_group"] = fleet_archive.apply(
            lambda x: x["cya_group"].index(x["cya"]) + 1, axis=1
        )
        fleet_archive["years_to_end"] = fleet_archive.apply(
            lambda x: len(x["cya_group"]) - x["position_in_group"], axis=1
        )

        # If it is the final year in the group set the tally as the final year tally
        fleet_archive.loc[fleet_archive["cya_group"].map(len) == 1, "final_year_tally"] = (
            fleet_archive["cohort_tally"]
        )

        # Find the survival rate of each entry which follows an entry with a final year tally
        # Use that survival rate to calculate the entry's final year tally and iterate until all calculated
        for i in range(5):
            fleet_archive["survival"] = fleet_archive["cohort_tally"] / fleet_archive.groupby(
                ["cohort", "vehicle_type"]
            )["final_year_tally"].shift(1)
            fleet_archive["survival"] = np.power(
                fleet_archive["survival"], 1 / fleet_archive["position_in_group"]
            )
            fleet_archive["survival"] = fleet_archive.groupby(["cya", "vehicle_type"])[
                "survival"
            ].transform("mean")
            fleet_archive["final_year_tally"] = fleet_archive["cohort_tally"] * np.power(
                fleet_archive["survival"], fleet_archive["years_to_end"]
            )

        # Take the average of the survival rates across all cohorts
        scrappage_curve = (
            fleet_archive.groupby(["cya", "vehicle_type"])["survival"].mean().reset_index()
        )
        scrappage_curve = scrappage_curve.merge(groupings, how="left", on="cya").fillna(1)
        scrappage_curve["cya"] = scrappage_curve["cya_group"]
        scrappage_curve = scrappage_curve.drop(columns=["cya_group"])
        print("\n\nScrappage Curve Generated")
        return scrappage_curve

    @staticmethod
    def characteristics_to_rwc(invariant_obj):
        """Calculate the real world correction factor of a vehicle using its emission characteristics."""
        characteristics = invariant_obj.index_fleet.characteristics.copy()
        table_df = characteristics.merge(
            invariant_obj.real_world_coefficients, how="inner", on=["fuel", "vehicle_type"]
        )
        table_df["fc_approval"] = table_df.apply(
            "{avg_co2}*12/44 /1000/{density}*100/{carbon_ef}*1000".format_map, axis=1
        ).map(eval)
        table_df["fc_in_use"] = table_df.apply(
            "{params1} + {params2}*{avg_es} + {params3}*{avg_mass} + {params4}*{fc_approval}".format_map,
            axis=1,
        ).map(eval)

        table_df["rw_multiplier"] = table_df["fc_in_use"] / table_df["fc"]
        table_df = table_df[["cohort", "segment", "fuel", "vehicle_type", "rw_multiplier"]]

        characteristics = characteristics[["cohort", "segment", "fuel", "vehicle_type"]]
        characteristics = ut.determine_fuel_type(characteristics)
        # If no real word correction can be calculated, set it as 1.
        characteristics = characteristics.merge(table_df, how="left").fillna(1)
        rw_multiplier = characteristics.merge(
            invariant_obj.fuel_characteristics, how="left", on="fuel"
        )
        # PHEVs operate in electric mode 50% of the time.
        rw_multiplier.loc[rw_multiplier["fuel_type"] == "phev", "rw_multiplier"] = (
            rw_multiplier["rw_multiplier"] * 0.5
        )
        return rw_multiplier

    @staticmethod
    def fit_se_curves(invariant_obj):
        """Fit speed emission curves using the rwc factor and known coefficients."""
        rw_multiplier = invariant_obj.rw_multiplier.copy()
        # If params are missing then they are filled with the
        # closest more recent year of matching fuel and segment
        curve_parameters = invariant_obj.naei_coefficients.merge(
            rw_multiplier[["fuel", "cohort", "segment"]], how="outer"
        )
        curve_parameters = curve_parameters.loc[
            ~(curve_parameters["fuel"] == "clean")
        ].sort_values(by=["cohort"], ascending=False)
        curve_parameters = curve_parameters.groupby(["fuel", "segment"], as_index=False).apply(
            lambda group: group.ffill()
        )
        # Substitute coefficients into the se curve formula
        se_curve = rw_multiplier.merge(
            curve_parameters, how="left", on=["cohort", "segment", "fuel", "vehicle_type"]
        )
        se_curve["formula"] = se_curve.apply(
            "({alpha}*speed**2 + {beta}*speed + {gamma} + {delta}/speed) / ({epsilon}*speed**2 +"
            " {zita}*speed + {hta}) *{rw_multiplier}*{mj_co2}".format_map,
            axis=1,
        )
        # Set clean fuel as 0 emissions at all speeds
        se_curve.loc[se_curve["fuel"] == "clean", "formula"] = "0"
        se_curve["fuel"] = se_curve["fuel_type"]
        se_curve = se_curve[
            ["cohort", "fuel", "vehicle_type", "segment", "formula", "min_speed", "max_speed"]
        ]
        se_curve = se_curve.drop_duplicates()
        return se_curve

    @staticmethod
    def calculate_speed_band_emissions(invariant_obj):
        """Use the se curves to calculate the emission intensity for each speed band."""
        se_calcs = invariant_obj.se_curve.copy()
        grouping_vars = ["segment", "fuel", "cohort", "vehicle_type"]
        speed_bands = ["10_30", "30_50", "50_70", "70_90", "90_110", "110_130"]
        se_calcs[speed_bands] = pd.DataFrame(
            [[20, 40, 60, 80, 100, 120]], index=se_calcs.index
        )
        se_calcs = pd.melt(
            se_calcs,
            id_vars=grouping_vars + ["formula", "min_speed", "max_speed"],
            var_name="speed_band",
            value_vars=speed_bands,
            value_name="speed",
        )
        se_calcs["formula"] = se_calcs.apply(
            lambda x: x["formula"].replace("speed", str(x["speed"])), axis=1
        )
        se_calcs["gco2/km"] = se_calcs["formula"].map(eval)
        # The NAEI curves are only fitted for a certain range of speeds - outside of this range they aren't accurate
        # (e.g. high-speed HGVs)
        # By setting gco2/km to a very high number we can catch any inaccuracies in the results
        se_calcs.loc[se_calcs["min_speed"] > se_calcs["speed"], "gco2/km"] = 10e20
        se_calcs.loc[se_calcs["max_speed"] < se_calcs["speed"], "gco2/km"] = 10e20
        se_calcs = se_calcs[grouping_vars + ["speed_band", "gco2/km"]]
        se_calcs = se_calcs.rename(columns={"cohort": "e_cohort"})
        return se_calcs


# %% Model Classes


class IndexFleet:
    """Load in and preprocess DfT Fleet data."""

    def __init__(self, run_fresh, fleet_year):
        """Initialise functions and set filepath to export tables.

        Parameters
        ----------
        run_fresh : boolean
            Determines whether raw DfT fleet data is called in and preprocessed
            or whether preprocessed tables are called in, thereby skipping
            preprocessing.

        fleet_year : int
            The year of the DVLA fleet data.
        """
        if run_fresh:
            self.fleet_index_year = fleet_year
            self.__load_fleet()
            self.__basic_clean()
            self.__advanced_clean()
            self.__split_tables()
            # self.__map_zones() # TODO: likely unneccesary, check
        else:
            self.fleet_archive = pd.read_csv(f"{OUT_PATH}/audit/fleet_archive.csv")
            self.fleet = pd.read_csv(f"{OUT_PATH}/audit/index_fleet.csv")
            self.characteristics = pd.read_csv(f"{OUT_PATH}/audit/characteristics.csv")

    def __load_fleet(self):
        """Read in the DfT fleet data for cars, vans and HGVs and concetenate."""
        # TODO: add fuel and fleet conversions
        fleet_archive = pd.read_csv(VEHICLE_PATH)
        fleet_archive = fleet_archive.rename(
            columns={
                "Post_Code_Current": "Postcode",
                "Records": "Tally",
                "Fuel Type": "Fuel",
                "Body Type Text": "BodyTypeText",
                "Vehicle_Type": "VehicleType",
                "AvgCC": "AvgES",
            }
        )

        postcode_to_msoa = pd.read_csv(
            POSTCODE_MSOA, usecols=["Postcode", "MSOA Code"]
        ).rename(columns={"MSOA Code": "Zone"})
        postcode_to_msoa["Postcode"] = postcode_to_msoa["Postcode"].str.replace(" ", "")
        fleet_archive = fleet_archive.merge(postcode_to_msoa, on="Postcode", how="left")
        fleet_archive = fleet_archive.drop(columns=["Postcode", "LSOA11NM", "Gross_Weight"])
        fleet_archive["Fuel"] = fleet_archive["Fuel"].replace(
            {
                "Diesel/ Heavy oil": "diesel",
                "Electric": "bev",
                "Electric/ Diesel": "diesel",
                "Electric/ Petrol": "phev",
                "Gas": "diesel",
                "Gas/Diesel": "diesel",
                "Gas/Petrol": "diesel",
                "Petrol": "petrol",
            }
        )
        fleet_archive = fleet_archive[
            fleet_archive["Fuel"].isin(
                ["diesel", "petrol", "phev", "bev", "hybrid", "hyrdogen", "petrol hybrid"]
            )
        ]
        # make sure the other random fuel types are filtered/out replaced
        fleet_archive = fleet_archive[fleet_archive["VehicleType"].isin(["Car", "Goods"])]
        fleet_archive = fleet_archive.drop(columns=["Keeper", "VehicleType"])
        self.fleet_archive = ut.camel_columns_to_snake(fleet_archive)

    def __basic_clean(self):
        """Carry out basic preprocessing on the DfT fleet data.

        - Change case and recode attributes.
        - Remove unused records (tally = 0 or na, other fuel, no zone).
        - Impute cya, fuel.
        """
        fleet_archive = self.fleet_archive.copy()
        fleet_archive = fleet_archive[fleet_archive["tally"] > 0]
        fleet_archive["fuel"] = fleet_archive["fuel"].str.lower().fillna("diesel")
        fleet_archive = fleet_archive[
            fleet_archive["fuel"].isin(
                ["diesel", "petrol", "phev", "bev", "hybrid", "hydrogen", "petrol hybrid"]
            )
        ]
        fleet_segmentation = pd.read_csv(DVLA_BODY)
        # fleet_archive = fleet_archive.loc[(
        #                                       fleet_archive["body_type_text"].isin(fleet_segmentation["body_type_text"]))
        #                                   & (fleet_archive["wheelplan_text"].isin(fleet_segmentation["wheelplan_text"]))
        #                                   ].reset_index(drop=True)
        fleet_archive = fleet_archive.merge(
            fleet_segmentation, how="left", on=["body_type_text", "wheelplan_text"]
        )
        fleet_archive = fleet_archive.drop(columns=["wheelplan_text", "body_type_text"])
        fleet_archive = fleet_archive[~fleet_archive["zone"].isin(["zzDisposal", "zzUnknown"])]
        fleet_archive = fleet_archive[~fleet_archive["fuel"].isin(["other"])]
        fleet_archive["segment"] = fleet_archive["segment"].fillna("Unknown")
        # Convert hev cars to petrol hybrids
        # fleet_archive.loc[
        #     (fleet_archive["vehicle_type"] == "car") & (fleet_archive["fuel"] == "hev"), "fuel"
        # ] = "petrol hybrid"
        # Convert HEV/PHEV non-cars to BEVs
        fleet_archive.loc[
            (fleet_archive["vehicle_type"].isin(["hgv", "lgv"]))
            & (fleet_archive["fuel"].isin(["hev", "phev"])),
            "fuel",
        ] = "bev"
        fleet_archive.loc[
            (fleet_archive["vehicle_type"].isin(["hgv"]))
            & (fleet_archive["fuel"].isin(["petrol"])),
            "fuel",
        ] = "diesel"
        # Determine missing CYA from eurostandard and vehicle type
        # fleet_archive = ut.determine_from_similar(
        #     fleet_archive,
        #     shared_qualities=["year", "vehicle_type", "euro_standard"],
        #     missing_quality="cya",
        #     value_to_distribute="tally",
        # )
        fleet_archive["cya"] = self.fleet_index_year
        fleet_archive["cya"] = fleet_archive["cya"] - fleet_archive["year"]
        fleet_archive["year"] = self.fleet_index_year

        fleet_archive = fleet_archive.groupby(
            [
                "zone",
                "fuel",
                "segment",
                "vehicle_type",
                "cya",
                "year",
                "avg_mass",
                "avg_es",
                "avg_co2",
            ],
            as_index=False,
        ).sum()

        fleet_archive = ut.cya_group_to_list(fleet_archive)
        # Merge vehicles with identical attributes
        self.fleet_archive = (
            fleet_archive.groupby(ut.all_but(fleet_archive, "tally"), dropna=False)
            .sum()
            .reset_index()
        )

    def __advanced_clean(self):
        """Carry out more advance preprocessing on the baseline fleet data.

        - Reduce to index year.
        - Impute segments and emission characteristics.
        """
        print("Index year:", self.fleet_index_year)
        fleet_df = self.fleet_archive.copy()
        # fleet_df = self.fleet_archive.loc[self.fleet_archive["year"] == int(self.fleet_index_year)]
        # Segmentation
        print(
            "Commencing imputation and determining from similar, this is a slow operation for large dataframes..."
        )
        fleet_df = Imputation.segment_by_mass(fleet_df, "car")
        fleet_df = Imputation.segment_by_mass(fleet_df, "lgv")
        # Determine missing segment from cya, fuel and vehicle type
        fleet_df = ut.determine_from_similar(
            fleet_df,
            shared_qualities=["cya", "fuel", "vehicle_type"],
            missing_quality="segment",
            value_to_distribute="tally",
        )
        # Imputation
        fleet_df = Imputation.fill_with_mean(fleet_df, "avg_mass")
        fleet_df = Imputation.fill_with_mean(fleet_df, "avg_co2")
        fleet_df = Imputation.fill_with_mean(fleet_df, "avg_es")
        self.fleet = Imputation.fill_with_mice(fleet_df)
        print("Imputation and determining from similar complete.")

    def __split_tables(self):
        """Split table into fleet only and emission characteristics only."""
        fleet_df = self.fleet.copy()
        # fleet_df = ut.cya_list_to_column(fleet_df, shared_value="tally")
        fleet_df["cohort"] = fleet_df["year"] - fleet_df["cya"]
        fleet_df = fleet_df.drop(columns="cya")

        # Take a weighted average of emission characteristics
        vehicle_characteristics = fleet_df.drop(columns="zone")
        vehicle_characteristics = ut.weighted_mean(
            vehicle_characteristics,
            grouping_var_list=["cohort", "vehicle_type", "segment", "fuel"],
            weight_var="tally",
            mean_var_list=["avg_co2", "avg_mass", "avg_es"],
        )
        # Reduce and store emission characteristics data
        vehicle_characteristics = vehicle_characteristics.drop(["tally"], axis=1)
        self.characteristics = vehicle_characteristics.drop_duplicates()

        # Reduce and store index fleet data
        fleet_df = fleet_df.drop(columns=["avg_co2", "avg_es", "avg_mass"])
        self.fleet = (
            fleet_df.groupby(ut.all_but(fleet_df, "tally"))["tally"].sum().reset_index()
        )

    def __map_zones(self):
        """Translate fleet data from LAD to MSOA zones.

        Uses the MSOA tally and the LAD vehicle shares to proportionately
        assign vehicles to MSOA zones.
        """
        # Aggregate the fleet data to the MSOA Zone level
        fleet_df = self.fleet.copy()

        # Calculate the share each vehicle makes up of its vehicle type in its LAD
        group_by_segment = (
            fleet_df.groupby(ut.all_but(fleet_df, "tally"))["tally"].sum().reset_index()
        )
        group_by_segment = group_by_segment.drop(columns="year")
        group_by_segment["shr_of_lad_type"] = group_by_segment["tally"] / fleet_df.groupby(
            ["zone", "vehicle_type"]
        )["tally"].transform("sum")
        # Vehicle type tally by MSOA
        msoa_bodytype = pd.read_csv(MSOA_BODY).fillna(0)
        msoa_bodytype = pd.melt(
            msoa_bodytype,
            id_vars="MSOA11CD",
            var_name="vehicle_type",
            value_vars=["Cars", "LGVs", "Goods"],
            value_name="msoa_tally",
        )
        msoa_bodytype["vehicle_type"] = msoa_bodytype["vehicle_type"].replace(
            {"Cars": "car", "LGVs": "lgv", "Goods": "hgv"}
        )
        # Connects LAD to MSOA
        msoa_lad_lookup = pd.read_csv(MSOA_LAD)
        # join on msoa, group by msoa zones & any categories, sum by msoa zones & any categories
        msoa_zones = pd.read_csv(NOHAM_TO_MSOA).rename(columns={"msoa11cd": "MSOA11CD"})
        msoa_zones = msoa_zones.merge(msoa_lad_lookup, on="MSOA11CD")
        vehicle_msoa_zones = pd.merge(msoa_zones, msoa_bodytype, on="MSOA11CD", how="inner")
        vehicle_msoa_zones = vehicle_msoa_zones.groupby(
            ["MSOA11CD", "vehicle_type", "TAG_LAD"], as_index=False
        ).sum()
        merged_data = vehicle_msoa_zones.merge(
            group_by_segment,
            left_on=["TAG_LAD", "vehicle_type"],
            right_on=["zone", "vehicle_type"],
        )
        merged_data = merged_data.loc[merged_data["msoa_tally"] > 0]

        # MSOA Tally for vehicle equals Vehicle share of LAD * MSOA total tally
        merged_data["tally"] = merged_data["shr_of_lad_type"] * merged_data["msoa_tally"]
        merged_data = (
            merged_data.groupby(
                ["MSOA11CD", "segment", "cohort", "fuel", "vehicle_type", "msoa_tally"]
            )["tally"]
            .sum()
            .reset_index()
        )
        merged_data["tally"] = merged_data["tally"].round().astype(int)

        # Write out
        vehicle_types = fleet_df[["year", "segment", "vehicle_type"]].drop_duplicates()
        merged_data = merged_data.rename(columns={"MSOA11CD": "zone"})
        merged_data = merged_data[["cohort", "fuel", "segment", "zone", "tally"]]
        self.fleet = merged_data.merge(vehicle_types, on="segment", how="left")


class Invariant:
    """Import and preprocess scenario invariant tables (baseline inputs)."""

    def __init__(self, index_fleet_obj, fleet_year):
        """Initialise functions and set class parameters.

        Parameters
        ----------
        index_fleet_obj : class obj
            Includes the fully preprocessed index fleet, partially
            preprocessed historic fleet and emission characteristics.
        fleet_year : int
            the year of the fleet
        """
        self.index_fleet = index_fleet_obj
        self.type = "general"
        self.scenario_name = "general"
        self.index_year = fleet_year
        self.save_invariant = True  # TODO(JC) Changed this from True to False
        # Choose a scenario for grid carbon intensity
        self.grid_intensity_scenario = "CCC Balanced"  # Options include CCC Balanced or TAG

        self.__import_shared_tables()
        self.__warp_tables()
        self.__generate_curves()
        self.__load_anpr()
        if self.save_invariant:
            self.__export_invariants()
        del self.index_fleet.fleet_archive

    def __generate_curves(self):
        """Call on CurveFitting functions to generate scrappage and SE curves."""
        self.scrappage_curve = CurveFitting.generate_scrappage(self)
        self.rw_multiplier = CurveFitting.characteristics_to_rwc(self)
        self.se_curve = CurveFitting.fit_se_curves(self)
        self.se_curve = CurveFitting.calculate_speed_band_emissions(self)

    def __export_invariants(self):
        """Export preprocessed tables.

        These tables would be called in if run_fresh is False.
        """
        self.index_fleet.fleet_archive.to_csv(
            f"{OUT_PATH}/audit/fleet_archive.csv", index=False
        )
        self.index_fleet.fleet.to_csv(f"{OUT_PATH}/audit/index_fleet.csv", index=False)
        self.index_fleet.characteristics.to_csv(
            f"{OUT_PATH}/audit/characteristics.csv", index=False
        )
        self.scrappage_curve.to_csv(f"{OUT_PATH}/audit/scrappage_curve.csv", index=False)

    def __import_shared_tables(self):
        """Import scenario invariant/baseline inputs."""
        # TODO(JC): change name of function in utility
        self.real_world_coefficients = ut.new_load_general_table("realWorldAttributes")
        self.fuel_characteristics = ut.new_load_general_table("fuelCharacteristics")
        self.yearly_co2_reduction = ut.new_load_general_table("newVehicleCarbonReduction")
        self.biofuel_reduction = ut.new_load_general_table("fuelComposition")
        self.ghg_equivalent = ut.new_load_general_table("GHGEquivalent")
        self.pt_ghg_factor = ut.new_load_general_table("PTGHGEquivalent")
        self.naei_coefficients = ut.new_load_general_table("naei")
        self.grid_consumption = ut.new_load_general_table("gridConsumption")
        self.grid_intensity = ut.new_load_general_table("gridCarbonIntensity")

        # self.real_world_coefficients = ut.load_table(self, "realWorldAttributes")
        # self.fuel_characteristics = ut.load_table(self, "fuelCharacteristics")
        # self.yearly_co2_reduction = ut.load_table(self, "newVehicleCarbonReduction")
        # self.biofuel_reduction = ut.load_table(self, "fuelComposition")
        # self.ghg_equivalent = ut.load_table(self, "GHGEquivalent")
        # self.pt_ghg_factor = ut.load_table(self, "PTGHGEquivalent")
        # self.naei_coefficients = ut.load_csv(self, "naei")
        # self.grid_consumption = ut.load_table(self, "gridConsumption", table_type="gridCo2")
        # self.grid_intensity = ut.load_table(self, "gridCarbonIntensity", table_type="gridCo2")

        self.msoa_area_info = pd.read_csv(MSOA_AREA_TYPE)
        self.msoa_area_info = self.msoa_area_info.rename(
            columns={"msoaZoneID": "zone", "R": "msoa_area_type"}
        )
        self.new_area_types = pd.read_csv(TARGET_AREA_TYPE)
        self.new_area_types = self.new_area_types.rename(
            columns={"msoa_area_code": "zone", "tfn_area_type": "msoa_area_type"}
        )

        # Check that baseline demand is the same across scenarios
        car_sc01_baseline_demand = pd.read_csv(
            str(DEMAND_PATH) + f"/SC01/vkm_by_speed_and_type_{self.index_year}_car.csv"
        )
        car_sc01_baseline_demand = car_sc01_baseline_demand.loc[
            car_sc01_baseline_demand.road_type == "Motorway", "total_vehicle_km"
        ].sum()
        for scenario_code in ["SC02"]:
            path = str(DEMAND_PATH) + f"/{scenario_code}/"
            car_scenario_baseline_demand = pd.read_csv(
                path + "vkm_by_speed_and_type_2023_car.csv"
            )
            car_scenario_baseline_demand = car_scenario_baseline_demand.loc[
                car_scenario_baseline_demand.road_type == "Motorway", "total_vehicle_km"
            ].sum()
            if np.round(car_sc01_baseline_demand) != np.round(car_scenario_baseline_demand):
                print("\n****!! Car Baseline demand varies across scenarios!!***\n")
        # gv demand equivalents
        hgv_sc01_baseline_demand = pd.read_csv(
            str(DEMAND_PATH) + f"/SC01/vkm_by_speed_and_type_{self.index_year}_hgv.csv"
        )
        hgv_sc01_baseline_demand = hgv_sc01_baseline_demand.loc[
            hgv_sc01_baseline_demand.road_type == "Motorway", "total_vehicle_km"
        ].sum()
        lgv_sc01_baseline_demand = pd.read_csv(
            str(DEMAND_PATH) + f"/SC01/vkm_by_speed_and_type_{self.index_year}_lgv.csv"
        )
        lgv_sc01_baseline_demand = lgv_sc01_baseline_demand.loc[
            lgv_sc01_baseline_demand.road_type == "Motorway", "total_vehicle_km"
        ].sum()
        for scenario_code in ["SC02"]:
            # hgv placeholder
            path = str(DEMAND_PATH) + f"/{scenario_code}/"
            hgv_scenario_baseline_demand = pd.read_csv(
                path + f"vkm_by_speed_and_type_{self.index_year}_hgv.csv"
            )
            hgv_scenario_baseline_demand = hgv_scenario_baseline_demand.loc[
                hgv_scenario_baseline_demand.road_type == "Motorway", "total_vehicle_km"
            ].sum()
            if np.round(hgv_sc01_baseline_demand) != np.round(hgv_scenario_baseline_demand):
                print("\n****!! HGV Baseline demand varies across scenarios!!***\n")
            # lgv placeholder
            path = str(DEMAND_PATH) + f"/{scenario_code}/"
            lgv_scenario_baseline_demand = pd.read_csv(
                path + f"vkm_by_speed_and_type_{self.index_year}_lgv.csv"
            )
            lgv_scenario_baseline_demand = lgv_scenario_baseline_demand.loc[
                lgv_scenario_baseline_demand.road_type == "Motorway", "total_vehicle_km"
            ].sum()
            if np.round(lgv_sc01_baseline_demand) != np.round(lgv_scenario_baseline_demand):
                print("\n****!! LGV Baseline demand varies across scenarios!!***\n")

    def __warp_tables(self):
        """Preprocess and transform scenario invariant/basleine inputs."""
        yearly_co2_reduction = pd.melt(
            self.yearly_co2_reduction,
            id_vars=["year"],
            var_name="body_type",
            value_name="year_reduction_in_co2",
        )
        # Convert year-on-year change to proportion of previous year
        # Cumulative product gives proportion of index year
        yearly_co2_reduction["year_reduction_in_co2"] = (
            1 + yearly_co2_reduction["year_reduction_in_co2"]
        )
        yearly_co2_reduction["index_carbon_reduction"] = yearly_co2_reduction.groupby(
            "body_type"
        )["year_reduction_in_co2"].cumprod()
        yearly_co2_reduction["cohort"] = yearly_co2_reduction["year"]
        self.yearly_co2_reduction = yearly_co2_reduction[
            ["cohort", "body_type", "index_carbon_reduction"]
        ]
        naei_coefficients = self.naei_coefficients.copy()
        vehicle_classes = {
            "Passenger Cars": "car",
            "Light Commercial Vehicles": "lgv",
            "Heavy Duty Trucks": "hgv",
        }
        naei_coefficients["vehicle_type"] = naei_coefficients["category"].replace(
            vehicle_classes
        )
        naei_coefficients["fuel"] = naei_coefficients["fuel"].str.lower()
        naei_coefficients["segment"] = naei_coefficients["segment"].str.lower()
        naei_coefficients = ut.cya_group_to_list(naei_coefficients)
        naei_coefficients = ut.cya_list_to_column(naei_coefficients)
        naei_coefficients["cohort"] = self.index_year - naei_coefficients["cya"]
        naei_coefficients = naei_coefficients.drop(
            columns=["cya", "category", "euro_standard"]
        )
        self.naei_coefficients = naei_coefficients

        # Preprocess grid consumption data and assign to the correct segments
        # by merging with baseline vehicle_types and segments
        fleet_segments = (
            self.index_fleet.fleet[["vehicle_type", "segment"]].drop_duplicates().copy()
        )
        fleet_segments["bev_v_type"] = fleet_segments["vehicle_type"].replace(
            {"car": "electric_car", "lgv": "electric_lgv", "hgv": "bev_hgv"}
        )
        fleet_segments.loc[
            fleet_segments.segment.isin(["rigid_a_upto7.5t", "rigid_b_7.5t-14t"]), "bev_v_type"
        ] = "bev_small_rigid"
        fleet_segments.loc[
            fleet_segments.segment.isin(
                ["rigid_c_14t-20t", "rigid_d_20t-26t", "rigid_e_over26t"]
            ),
            "bev_v_type",
        ] = "bev_large_rigid"
        fleet_segments.loc[
            fleet_segments.segment.isin(["artic_a_upto40t", "artic_b_over40t"]), "bev_v_type"
        ] = "bev_artic"

        grid_consumption = pd.melt(
            self.grid_consumption, id_vars="year", var_name="bev_v_type", value_name="kwh_km"
        )
        grid_consumption = grid_consumption.rename(columns={"year": "cohort"})
        grid_consumption["fuel"] = "bev"
        grid_consumption = grid_consumption.merge(fleet_segments, on="bev_v_type", how="left")
        grid_consumption = grid_consumption.drop(columns="bev_v_type")
        hydrogen_grid_consumption = grid_consumption.copy()
        hydrogen_grid_consumption["fuel"] = "hydrogen"
        phev_grid_consumption = grid_consumption.copy()
        phev_grid_consumption["fuel"] = "phev"
        # Apply PHEV factor (same as for RWC)
        phev_grid_consumption["kwh_km"] = phev_grid_consumption["kwh_km"] * 0.5
        grid_consumption = pd.concat(
            [grid_consumption, hydrogen_grid_consumption, phev_grid_consumption],
            ignore_index=True,
        )
        self.grid_consumption = grid_consumption.copy()

        # Index grid CO2 intensity figures from a specified scenario
        grid_scenario = {"CCC Balanced": "ccc_6_cb", "TAG": "tag"}[
            self.grid_intensity_scenario
        ]
        grid_intensity = self.grid_intensity[["year", grid_scenario]].copy()
        grid_intensity.columns = ["year", "gco2_kwh"]
        self.grid_intensity = grid_intensity.copy()

    def __load_anpr(self):
        """Load in an preprocess ANPR data."""
        # Set empty dataframe to save processed df
        anpr = pd.DataFrame()

        # Import anpr files
        for i in ["Car", "Van", "HGV"]:
            sheet = i + " output"
            # Import relevant tab
            anpr_info = pd.read_excel(ANPR_DATA, sheet_name=sheet)
            anpr_info = ut.camel_columns_to_snake(anpr_info)

            # Make ANPR df consistent with fleet data
            anpr_info = anpr_info.rename(columns={"fuel_type": "fuel"})
            anpr_info["fuel"] = anpr_info["fuel"].str.lower()
            anpr_info["cya"] = anpr_info["cya"].astype(str)
            anpr_info["body_type"] = anpr_info["body_type"].replace(
                {"HGV - Rigid": "Rigid", "HGV - Artic": "Artic"}
            )
            anpr_info["vehicle_type"] = i
            # Normalise the data so that the groups add up to 100 percent
            anpr_info["percentage"] = anpr_info["percentage"] / anpr_info.groupby(
                ["fuel", "vehicle_type", "road_type"]
            )["percentage"].transform("sum")
            # Aggregate the percentages by removing fuel
            # As cars have anpr data for both petrol and diesel, derive a weighted average across these fuels
            # Retrieve the share of petrol and diesel in the baseline fleet under each CYA and body type
            if i == "Car":
                petrol_diesel_share = self.index_fleet.fleet.copy()
                petrol_diesel_share["cya"] = (
                    petrol_diesel_share["year"] - petrol_diesel_share["cohort"]
                )
                petrol_diesel_share = ut.cya_column_to_group(petrol_diesel_share)
                petrol_diesel_share = ut.determine_body_type(petrol_diesel_share)
                petrol_diesel_share = (
                    petrol_diesel_share.loc[
                        petrol_diesel_share.fuel.isin(["petrol", "diesel"])
                    ]
                    .groupby(["body_type", "cya", "fuel"])["tally"]
                    .sum()
                    .reset_index()
                )
                petrol_diesel_share["petrol_diesel_prop"] = petrol_diesel_share[
                    "tally"
                ] / petrol_diesel_share.groupby(["body_type", "cya"])["tally"].transform("sum")
                petrol_diesel_share = petrol_diesel_share.drop(columns="tally")

                # Merge with the anpr data and derive the weighted average
                anpr_info = anpr_info.merge(
                    petrol_diesel_share, on=["body_type", "cya", "fuel"], how="left"
                )
                anpr_info["percentage"] = (
                    anpr_info["percentage"] * anpr_info["petrol_diesel_prop"]
                )
                anpr_info = (
                    anpr_info.groupby(["road_type", "body_type", "cya"])["percentage"]
                    .sum()
                    .reset_index()
                )

            # For HGVs, simply find the mean percentage for vehicle type (Artic and Rigid combined)
            elif i == "HGV":
                anpr_info["percentage"] = anpr_info.groupby(
                    ["road_type", "vehicle_type", "cya"]
                )["percentage"].transform("mean")
                anpr_info = anpr_info[["road_type", "body_type", "cya", "percentage"]]

            # For vans and HGVs, simply find the mean percentage
            else:
                anpr_info = (
                    anpr_info.groupby(["road_type", "body_type", "cya"])["percentage"]
                    .mean()
                    .reset_index()
                )

            # Re-normalise the percentages
            anpr_info["vehicle_type"] = i
            anpr_info["percentage"] = anpr_info["percentage"] / anpr_info.groupby(
                ["vehicle_type", "road_type"]
            )["percentage"].transform("sum")
            anpr = pd.concat([anpr, anpr_info], ignore_index=True)
        # Carry out final pre-processing
        anpr["vehicle_type"] = anpr["body_type"].replace(
            {"Car": "car", "Van": "lgv", "Rigid": "hgv", "Artic": "hgv"}
        )
        anpr = anpr.rename(
            columns={"percentage": "cya_prop_of_bt_rt"}
        )  # Fuel Body Type and Road Type

        self.anpr = anpr[
            ["body_type", "road_type", "vehicle_type", "cya", "cya_prop_of_bt_rt"]
        ]
