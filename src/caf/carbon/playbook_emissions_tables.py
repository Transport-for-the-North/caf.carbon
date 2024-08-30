# -*- coding: utf-8 -*-
"""Produces Carbon Playbook emissions tables from CAF.carbon outputs."""

##### IMPORTS #####

# Built-Ins
import pathlib

# Third Party
import caf.toolkit as ctk
import pandas as pd
import pydantic

##### CONSTANTS #####

TIME_PERIODS = ["TS1", "TS2", "TS3"]
REQUIRED_YEARS = [2018, 2028, 2038, 2043, 2048]
REQUIRED_SCENARIOS = ["bau", "cas"]
CONFIG_PATH = pathlib.Path("playbook_emissions_config.yml")

##### FUNCTIONS & CLASSES #####


class PlaybookConfig(ctk.BaseConfig):
    """Parameters for playbook process."""

    emissions_files: dict[str, dict[int, pydantic.FilePath]]
    area_type_lookup: pydantic.FilePath
    code_lookup: pydantic.FilePath

    years: list[int] = pydantic.Field(default_factory=lambda: REQUIRED_YEARS)

    @pydantic.field_validator("emissions_files")
    @classmethod
    def _check_scenarios(
        cls, value: dict[str, dict[int, pathlib.Path]]
    ) -> dict[str, dict[int, pathlib.Path]]:
        required = set(REQUIRED_SCENARIOS)
        scenarios = set(value.keys())

        missing = required - scenarios
        if len(missing) > 0:
            raise ValueError(f"missing emissions files for scenarios: {missing}")

        return value

    @pydantic.model_validator(mode="after")
    def _check_years(self):
        if set(self.years) != set(REQUIRED_YEARS):
            raise ValueError(f"required years are {REQUIRED_YEARS}")

        required = set(self.years)

        for scenario, paths in self.emissions_files.items():
            missing = required - set(paths.keys())
            if len(missing) > 0:
                raise ValueError(
                    f"missing emissions files for '{scenario}' for years: {missing}"
                )

        return self


class PlaybookProcess:

    def __init__(self, parameters: PlaybookConfig):
        """Initialise functions and set class variables.

        Parameters
        ----------
        """
        self.years_to_iter = parameters.years
        self.time_periods = TIME_PERIODS
        self.emissions_files = parameters.emissions_files
        self.area_type_lookup = pd.read_csv(parameters.area_type_lookup)
        self.area_type_lookup = self.area_type_lookup[["MSOA11CD", "RUC_Class"]].rename(
            columns={"MSOA11CD": "origin", "RUC_Class": "Origin Place Type"}
        )
        self.code_lookup = pd.read_csv(parameters.code_lookup)

        self.run_analysis()

    def read_cafcarb_output(self, year, scenario):
        year_emissions_data = pd.DataFrame()
        for time_period in self.time_periods:
            emissions_data = pd.read_hdf(
                self.emissions_files[scenario][year], f"{time_period}_{year}", mode="r"
            )
            emissions_data["time_period"] = time_period
            year_emissions_data = pd.concat([year_emissions_data, emissions_data])
        through_lta = self.code_lookup[["lad", "through_name"]].rename(
            columns={"lad": "through"}
        )
        year_emissions_data = year_emissions_data.merge(through_lta, how="left", on="through")
        year_emissions_data.drop(columns=["through"])
        year_emissions_data = year_emissions_data.rename(columns={"through_name": "through"})
        return year_emissions_data

    def apply_annualisation(self, day_data):

        for demand in ["tailpipe_gco2", "grid_gco2", "vkm"]:
            day_data.loc[day_data["time_period"] == "TS1", demand] = day_data[demand] * 3
            day_data.loc[day_data["time_period"] == "TS2", demand] = day_data[demand] * 6
            day_data.loc[day_data["time_period"] == "TS3", demand] = day_data[demand] * 3

            day_data.loc[day_data["vehicle_type"] == "car", demand] = day_data[demand] * 348
            day_data.loc[day_data["vehicle_type"] == "lgv", demand] = day_data[demand] * 329
            day_data.loc[day_data["vehicle_type"] == "hgv", demand] = (
                day_data[demand] * 297 / 2.5
            )  # PCU accounting

        unsimulated_data = day_data.copy()
        for demand in ["tailpipe_gco2", "grid_gco2", "vkm"]:
            unsimulated_data.loc[unsimulated_data["vehicle_type"] == "car", demand] = (
                unsimulated_data[demand] * 3 * 0.238
            )
            unsimulated_data.loc[unsimulated_data["vehicle_type"] == "lgv", demand] = (
                unsimulated_data[demand] * 6 * 0.216
            )
            unsimulated_data.loc[unsimulated_data["vehicle_type"] == "hgv", demand] = (
                unsimulated_data[demand] * 3 * 331
            )
        unsimulated_data["time_period"] = "TS4"
        year_data = pd.concat([day_data, unsimulated_data])
        year_data = (
            year_data.groupby(
                [
                    "origin",
                    "destination",
                    "through",
                    "fuel",
                    "user_class",
                    "vehicle_type",
                    "year",
                    "trip_band",
                    "time_period",
                    "tally",
                ]
            )[["tailpipe_gco2", "grid_gco2", "vkm"]]
            .sum()
            .reset_index()
        )
        return year_data

    def create_2019_purpose(self, annual_data):
        purpose_data = annual_data[["through", "user_class", "tailpipe_gco2", "grid_gco2"]]
        purpose_data["Emissions (tCO2e)"] = (
            purpose_data["tailpipe_gco2"] + purpose_data["grid_gco2"]
        )
        purpose_data["Emissions (tCO2e)"] = purpose_data["Emissions (tCO2e)"] / (1000 * 1000)
        purpose_data = purpose_data.rename(columns={"through": "LTA"})
        purpose_data.loc[purpose_data["user_class"] == "uc1", "Purpose"] = "Business"
        purpose_data.loc[purpose_data["user_class"] == "uc2", "Purpose"] = "Commute"
        purpose_data.loc[purpose_data["user_class"] == "uc3", "Purpose"] = "Other"
        purpose_data.loc[purpose_data["user_class"] == "uc4", "Purpose"] = "LGV"
        purpose_data.loc[purpose_data["user_class"] == "uc5", "Purpose"] = "HGV"
        purpose_data = (
            purpose_data[["LTA", "Purpose", "Emissions (tCO2e)"]]
            .groupby(["LTA", "Purpose"], as_index=False)
            .sum()
        )
        # interpolation
        return purpose_data

    def create_vehicle_type(self, annual_data):
        vehicle_data = annual_data[["through", "vehicle_type", "tailpipe_gco2", "grid_gco2"]]
        vehicle_data["Emissions (tCO2e)"] = (
            vehicle_data["tailpipe_gco2"] + vehicle_data["grid_gco2"]
        )
        vehicle_data["Emissions (tCO2e)"] = vehicle_data["Emissions (tCO2e)"] / (1000 * 1000)
        vehicle_data = vehicle_data.rename(
            columns={"through": "LTA", "vehicle_type": "Vehicle"}
        )
        vehicle_data.loc[vehicle_data["vehicle_type"] == "car", "vehicle_type"] = "Car"
        vehicle_data.loc[vehicle_data["vehicle_type"] == "lgv", "vehicle_type"] = "LGV"
        vehicle_data.loc[vehicle_data["vehicle_type"] == "hgv", "vehicle_type"] = "HGV"
        vehicle_data = (
            vehicle_data[["LTA", "Vehicle", "Emissions (tCO2e)"]]
            .groupby(["LTA", "Vehicle"], as_index=False)
            .sum()
        )
        # interpolation
        return vehicle_data

    def create_type(self, annual_data):
        type_data = annual_data[["through", "origin", "tailpipe_gco2", "grid_gco2"]]
        type_data = type_data.merge(self.area_type_lookup, how="left", on="origin")
        type_data["Emissions (tCO2e)"] = type_data["tailpipe_gco2"] + type_data["grid_gco2"]
        type_data["Emissions (tCO2e)"] = type_data["Emissions (tCO2e)"] / (1000 * 1000)
        type_data = type_data.rename(columns={"through": "LTA"})
        type_data = (
            type_data[["LTA", "Origin Place Type", "Emissions (tCO2e)"]]
            .groupby(["LTA", "Origin Place Type"], as_index=False)
            .sum()
        )
        # interpolation
        return type_data

    def create_trip_length(self, annual_data):
        trip_data = annual_data[
            ["origin", "destination", "through", "trip_band", "tailpipe_gco2", "grid_gco2"]
        ]
        trip_data["Emissions"] = trip_data["tailpipe_gco2"] + trip_data["grid_gco2"]
        trip_data["Emissions"] = trip_data["Emissions"] / (1000 * 1000)
        trip_data = trip_data.rename(columns={"through": "LTA", "trip_band": "Trip Length"})
        trip_data = (
            trip_data[["LTA", "Trip Length", "Emissions"]]
            .groupby(["LTA", "Trip Length"], as_index=False)
            .sum()
        )
        # interpolation
        return trip_data

    def create_genesis(self, annual_data):
        genesis_data = annual_data[
            ["origin", "destination", "through", "trip_band", "tailpipe_gco2", "grid_gco2"]
        ]
        origin_lta = self.code_lookup[["zone_cd", "through_name"]].rename(
            columns={"zone_cd": "origin", "through_name": "o_lta"}
        )
        genesis_data = genesis_data.merge(origin_lta, how="left", on="origin")
        destination_lta = self.code_lookup[["zone", "through_name"]].rename(
            columns={"zone": "destination", "through_name": "d_lta"}
        )
        genesis_data = genesis_data.merge(destination_lta, how="left", on="destination")
        genesis_data["Emissions (tCO2e)"] = (
            genesis_data["tailpipe_gco2"] + genesis_data["grid_gco2"]
        )
        genesis_data["Emissions (tCO2e)"] = genesis_data["Emissions (tCO2e)"] / (1000 * 1000)
        genesis_data = genesis_data[["o_lta", "d_lta", "t_lta", "Emissions"]]
        genesis_data["Genesis"] = "Through"
        genesis_data.loc[
            (genesis_data["o_lta"] == genesis_data["d_lta"])
            & (genesis_data["o_lta"] == genesis_data["through"]),
            "Genesis",
        ] = "Internal"
        genesis_data.loc[
            (genesis_data["o_lta"] == genesis_data["through"])
            & (genesis_data["d_lta"] != genesis_data["through"]),
            "Genesis",
        ] = "Outbound"
        genesis_data.loc[
            (genesis_data["d_lta"] == genesis_data["through"])
            & (genesis_data["o_lta"] != genesis_data["through"]),
            "Genesis",
        ] = "Inbound"

        genesis_data = genesis_data.rename(columns={"through": "LTA"})
        genesis_data = (
            genesis_data[["LTA", "Genesis", "Emissions (tCO2e)"]]
            .groupby(["LTA", "Trip Length"], as_index=False)
            .sum()
        )
        return genesis_data

    def vkm_info(self, data):
        data = (
            data[["through", "year", "tailpipe_gco2", "grid_gco2"]]
            .groupby(["through", "year"], as_index=False)
            .sum()
        )
        data_totals = (
            data[["through", "year", "vehicle_type", "vkm"]]
            .groupby(["through", "year", "vehicle_type"], as_index=False)
            .sum()
        )
        data_totals = data_totals.rename(columns={"vkm": "vkm_total"})
        data.loc[data["fuel"] == "hydrogen", "fuel"] = "ZEV"
        data.loc[data["fuel"] == "bev", "fuel"] = "ZEV"
        data = data[data["fuel"] == "ZEV"]
        data = data.merge(data_totals, how="left", on=["through", "year", "vehicle_type"])
        data["zev mileage"] = data["vkm"] / data["vkm_total"]
        data = data.rename(columns={"through": "LTA"})
        data = (
            data[["LTA", "year", "zev mileage", "vehicle_type"]]
            .groupby(["LTA", "year", "vehicle_type"], as_index=False)
            .sum()
        )
        transformed_data = data.pivot(
            index=["LTA", "year"], columns="vehicle_type", values="zev_mileage"
        )
        return transformed_data

    def co2_info(self, data):
        data = (
            data[["through", "year", "tailpipe_gco2", "grid_gco2"]]
            .groupby(["through", "year", "vehicle_type", "fuel_type"], as_index=False)
            .sum()
        )
        data["Emissions (MtCO2e)"] = data["tailpipe_gco2"] + data["grid_gco2"]
        data["Emissions (MtCO2e)"] = data["Emissions (MtCO2e)"] / (1000 * 1000 * 1000 * 1000)
        data = data.rename(columns={"through": "LTA"})
        data = (
            data[["LTA", "year", "Emissions (MtCO2e)"]]
            .groupby(["LTA", "year"], as_index=False)
            .sum()
        )
        return data

    def create_vkm_splits(self, data_2018, data_2028, data_2038, data_2043, data_2048):
        vkm_split_data = pd.DataFrame()
        data_2018 = self.vkm_info(data_2018)
        data_2028 = self.vkm_info(data_2028)
        data_2038 = self.vkm_info(data_2038)
        data_2043 = self.vkm_info(data_2043)
        data_2048 = self.vkm_info(data_2048)
        columns = ["car", "lgv", "hgv"]
        for year in range(2019, 2028):
            interpol_data = self.lininterpol(data_2018, data_2028, 2018, 2028, year, columns)
            vkm_split_data = pd.concat([vkm_split_data, interpol_data])
        vkm_split_data = pd.concat([vkm_split_data, data_2028])
        for year in range(2029, 2038):
            interpol_data = self.lininterpol(data_2028, data_2038, 2028, 2038, year, columns)
            vkm_split_data = pd.concat([vkm_split_data, interpol_data])
        vkm_split_data = pd.concat([vkm_split_data, data_2038])
        for year in range(2039, 2043):
            interpol_data = self.lininterpol(data_2038, data_2043, 2038, 2043, year, columns)
            vkm_split_data = pd.concat([vkm_split_data, interpol_data])
        vkm_split_data = pd.concat([vkm_split_data, data_2043])
        for year in range(2044, 2049):
            interpol_data = self.lininterpol(data_2043, data_2048, 2043, 2048, year, columns)
            vkm_split_data = pd.concat([vkm_split_data, interpol_data])
        vkm_split_data = pd.concat([vkm_split_data, data_2048])
        for year in range(2049, 2051):
            interpol_data = self.lininterpol(data_2043, data_2048, 2043, 2048, year, columns)
            vkm_split_data = pd.concat([vkm_split_data, interpol_data])

        return vkm_split_data

    def create_co2_emissions(self, data_2018, data_2028, data_2038, data_2043, data_2048):
        co2_emission_data = pd.DataFrame()
        data_2018 = self.co2_info(data_2018)
        data_2028 = self.co2_info(data_2028)
        data_2038 = self.co2_info(data_2038)
        data_2043 = self.co2_info(data_2043)
        data_2048 = self.co2_info(data_2048)
        columns = ["Emissions (MtCO2e)"]
        for year in range(2019, 2028):
            interpol_data = self.lininterpol(data_2018, data_2028, 2018, 2028, year, columns)
            co2_emission_data = pd.concat([co2_emission_data, interpol_data])
        co2_emission_data = pd.concat([co2_emission_data, data_2028])
        for year in range(2029, 2038):
            interpol_data = self.lininterpol(data_2028, data_2038, 2028, 2038, year, columns)
            co2_emission_data = pd.concat([co2_emission_data, interpol_data])
        co2_emission_data = pd.concat([co2_emission_data, data_2038])
        for year in range(2039, 2043):
            interpol_data = self.lininterpol(data_2038, data_2043, 2038, 2043, year, columns)
            co2_emission_data = pd.concat([co2_emission_data, interpol_data])
        co2_emission_data = pd.concat([co2_emission_data, data_2043])
        for year in range(2044, 2049):
            interpol_data = self.lininterpol(data_2043, data_2048, 2043, 2048, year, columns)
            co2_emission_data = pd.concat([co2_emission_data, interpol_data])
        co2_emission_data = pd.concat([co2_emission_data, data_2048])
        for year in range(2049, 2051):
            interpol_data = self.lininterpol(data_2043, data_2048, 2043, 2048, year, columns)
            co2_emission_data = pd.concat([co2_emission_data, interpol_data])

        return co2_emission_data

    def lininterpol(self, df1, df2, year1, year2, targetyear, columns):
        """Conduct the Interpolation"""
        df1year = year1
        df2year = year2
        year = targetyear
        target = df1.copy()
        for i in columns:
            target[i] = (
                -1
                * ((df2year - year) * df1[i] + (year - df1year) * df2[i])
                / (df1year - df2year)
            )
        target["year"] = targetyear
        return target

    def run_analysis(self):
        emissions_2018 = self.read_cafcarb_output(2018, "bau")
        emissions_2018 = self.apply_annualisation(emissions_2018)

        emissions_2028 = self.read_cafcarb_output(2028, "bau")
        emissions_2028 = self.apply_annualisation(emissions_2028)

        emissions_2019 = self.lininterpol(
            emissions_2018,
            emissions_2028,
            2018,
            2028,
            2019,
            ["tailpipe_gco2", "grid_gco2", "vkm"],
        )

        purpose_2019 = self.create_2019_purpose(emissions_2019)
        purpose_2019.to_csv("2019 Purpose.csv")

        vehicle_2019 = self.create_vehicle_type(emissions_2019)
        vehicle_2019.to_csv("2019 Vehicle.csv")

        trip_length_2019 = self.create_trip_length(emissions_2019)
        trip_length_2019.to_csv("2019 Trip Length.csv")

        type_2019 = self.create_type(emissions_2019)
        type_2019.to_csv("2019 Origin Place Type.csv")

        genesis_2019 = self.create_genesis(emissions_2019)
        genesis_2019.to_csv("2019 Genesis.csv")

        emissions_2038 = self.read_cafcarb_output(2038, "bau")
        emissions_2038 = self.apply_annualisation(emissions_2038)

        emissions_2043 = self.read_cafcarb_output(2043, "bau")
        emissions_2043 = self.apply_annualisation(emissions_2043)

        emissions_2048 = self.read_cafcarb_output(2048, "bau")
        emissions_2048 = self.apply_annualisation(emissions_2048)

        emissions_2050 = self.lininterpol(
            emissions_2043,
            emissions_2048,
            2043,
            2048,
            2050,
            ["tailpipe_gco2", "grid_gco2", "vkm"],
        )

        vehicle_2050 = self.create_vehicle_type(emissions_2050)
        vehicle_2050.to_csv("2050 Vehicle.csv")

        vkm_splits_bau = self.create_vkm_splits(
            emissions_2018, emissions_2028, emissions_2038, emissions_2043, emissions_2048
        )
        vkm_splits_bau.to_csv("vkm_splits_bau.csv")

        ue_emissions_2018 = self.read_cafcarb_output(2018, "cas")
        ue_emissions_2018 = self.apply_annualisation(ue_emissions_2018)
        ue_emissions_2028 = self.read_cafcarb_output(2028, "cas")
        ue_emissions_2028 = self.apply_annualisation(ue_emissions_2028)
        ue_emissions_2038 = self.read_cafcarb_output(2038, "cas")
        ue_emissions_2038 = self.apply_annualisation(ue_emissions_2038)
        ue_emissions_2043 = self.read_cafcarb_output(2043, "cas")
        ue_emissions_2043 = self.apply_annualisation(ue_emissions_2043)
        ue_emissions_2048 = self.read_cafcarb_output(2048, "cas")
        ue_emissions_2048 = self.apply_annualisation(ue_emissions_2048)

        vkm_splits_ue = self.create_vkm_splits(
            ue_emissions_2018,
            ue_emissions_2028,
            ue_emissions_2038,
            ue_emissions_2043,
            ue_emissions_2048,
        )
        vkm_splits_ue.to_csv("vkm_splits_ue.csv")

        co2_emissions = self.create_co2_emissions(
            emissions_2018, emissions_2028, emissions_2038, emissions_2043, emissions_2048
        )
        co2_emissions.to_csv("co2_emissions_bau.csv")

        co2_emissions_ue = self.create_co2_emissions(
            ue_emissions_2018,
            ue_emissions_2028,
            ue_emissions_2038,
            ue_emissions_2043,
            ue_emissions_2048,
        )
        co2_emissions_ue.to_csv("co2_emissions_ue.csv")


def _run():
    try:
        config = PlaybookConfig.load_yaml(CONFIG_PATH)
    except pydantic.ValidationError as exc:
        # SystemExit shows the error without the traceback
        # which is clearer for validation errors
        raise SystemExit(exc) from exc

    PlaybookProcess(config)


if __name__ == "__main__":
    _run()
