import pandas as pd


class PlaybookProcess:

    def __init__(self):
        """Initialise functions and set class variables.

        Parameters
        ----------
        """
        self.time_periods = ["TS1", "TS2", "TS3"]
        self.out_date = "DD_MM_YYYY"
        self.working_directory = r"A:/QCR- assignments/Data for WSP/"
        self.car_factor, self.lgv_factor, self.hgv_factor = 0.006331, 0.0069166, 0.005161
        self.area_type_lookup = pd.read_csv(
            r"G:\raw_data\WSP Carbon Playbook\WSP_final\msoa_reclassifications_england_v6.csv")
        self.area_type_lookup = self.area_type_lookup[["MSOA11CD", "RUC_Class"]].rename(columns={
            "MSOA11CD": "origin",
            "RUC_Class": "Origin Place Type"})
        self.code_lookup = pd.read_csv(
            r"A:\QCR- assignments\03.Assignments\h5files\Other Inputs\North_through_lookup-MSOA11_lta.csv")

        self.run_analysis()

    def read_cafcarb_output(self, year, scenario, file_name):
        year_emissions_data = pd.DataFrame()
        for time_period in self.time_periods:
            emissions_data = pd.read_csv(f"{self.working_directory}/{scenario}_{year}_{time_period}__{file_name}.csv")
            # emissions_data = emissions_data[emissions_data["LTA"] != 22]
            emissions_data["time_period"] = time_period
            year_emissions_data = pd.concat([year_emissions_data, emissions_data])
        through_lta = self.code_lookup[["lad", "through_name"]].rename(columns={"lad": "LTA"})
        year_emissions_data = year_emissions_data.merge(through_lta, how="left", on="LTA")
        year_emissions_data = year_emissions_data.drop(columns="LTA")
        year_emissions_data = year_emissions_data.rename(columns={"through_name": "LTA"})
        return year_emissions_data

    def apply_annualisation(self, day_data, emissions_column):
        target = emissions_column[0]
        annualisation_needed = False  # Annualisation now done earlier in vkm emission model, not needed
        if annualisation_needed:
            day_data.loc[day_data["time_period"] == "TS1", target] = day_data[target] * 3
            day_data.loc[day_data["time_period"] == "TS2", target] = day_data[target] * 6
            day_data.loc[day_data["time_period"] == "TS3", target] = day_data[target] * 3
            day_data.loc[day_data["vehicle_type"] == "car", target] = day_data[target] * 348 * self.car_factor
            day_data.loc[day_data["vehicle_type"] == "lgv", target] = day_data[target] * 329 * self.lgv_factor
            day_data.loc[day_data["vehicle_type"] == "hgv", target] = day_data[target] * 297 * self.hgv_factor / 2.5  # PCU accounting
            unsimulated_data = day_data.copy()
            unsimulated_data.loc[unsimulated_data["vehicle_type"] == "car", target] = (unsimulated_data[target] * 0.238)
            unsimulated_data.loc[unsimulated_data["vehicle_type"] == "lgv", target] = (unsimulated_data[target] * 0.216)
            unsimulated_data.loc[unsimulated_data["vehicle_type"] == "hgv", target] = (unsimulated_data[target] * 0.331)
            unsimulated_data["time_period"] = "TS4"
            year_data = pd.concat([day_data, unsimulated_data])
        else:
            year_data = day_data
        return year_data

    def create_co2_emissions(self, data_2018, data_2028, data_2038, data_2043, data_2048):
        co2_emission_data = pd.DataFrame()
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
        for year in range(2044, 2048):
            interpol_data = self.lininterpol(data_2043, data_2048, 2043, 2048, year, columns)
            co2_emission_data = pd.concat([co2_emission_data, interpol_data])
        co2_emission_data = pd.concat([co2_emission_data, data_2048])
        for year in range(2049, 2051):
            interpol_data = self.lininterpol(data_2043, data_2048, 2043, 2048, year, columns)
            co2_emission_data = pd.concat([co2_emission_data, interpol_data])
        return co2_emission_data

    def lininterpol(self, df1, df2, year1, year2, targetyear, columns):
        """ Conduct the Interpolation """
        df1year = year1
        df2year = year2
        year = targetyear
        target = df1.copy()
        df1["year"] = year1
        df2["year"] = year2
        for i in columns:
            target[i] = -1 * ((df2year - year) * df1[i] + (year - df1year) * df2[i]) / (df1year - df2year)
        target["year"] = targetyear
        return target

    def run_analysis(self):
        purpose_2018 = self.read_cafcarb_output(2018, "bau", "Purpose")
        purpose_2018 = self.apply_annualisation(purpose_2018, ["Emissions (tCO2e)"])
        purpose_2018 = purpose_2018[["LTA", "Purpose", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Purpose"], as_index=False).sum()

        purpose_2028 = self.read_cafcarb_output(2028, "bau", "Purpose")
        purpose_2028 = self.apply_annualisation(purpose_2028, ["Emissions (tCO2e)"])
        purpose_2028 = purpose_2028[["LTA", "Purpose", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Purpose"], as_index=False).sum()
        purpose_2019 = self.lininterpol(purpose_2018, purpose_2028, 2018, 2028, 2019,
                                        ["Emissions (tCO2e)"])
        purpose_2019.to_csv(r"A:\QCR- assignments\Data for WSP\2019 Purpose.csv")

        vehicle_2018 = self.read_cafcarb_output(2018, "bau", "Vehicle_Type")
        vehicle_2018 = self.apply_annualisation(vehicle_2018, ["Emissions (tCO2e)"])
        vehicle_2018 = vehicle_2018.rename(columns={"vehicle_type": "Vehicle"})
        vehicle_2018 = vehicle_2018[["LTA", "Vehicle", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Vehicle"], as_index=False).sum()
        vehicle_2028 = self.read_cafcarb_output(2028, "bau", "Vehicle_Type")
        vehicle_2028 = self.apply_annualisation(vehicle_2028, ["Emissions (tCO2e)"])
        vehicle_2028 = vehicle_2028.rename(columns={"vehicle_type": "Vehicle"})
        vehicle_2028 = vehicle_2028[["LTA", "Vehicle", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Vehicle"], as_index=False).sum()
        vehicle_2019 = self.lininterpol(vehicle_2018, vehicle_2028, 2018, 2028, 2019,
                                        ["Emissions (tCO2e)"])
        vehicle_2019.to_csv(r"A:\QCR- assignments\Data for WSP\2019 Vehicle.csv")

        trip_length_2018 = self.read_cafcarb_output(2018, "bau", "Trip_Band")
        trip_length_2018 = self.apply_annualisation(trip_length_2018, ["Emissions (tCO2e)"])
        trip_length_2018 = trip_length_2018[["LTA", "Trip Length", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Trip Length"], as_index=False).sum()
        trip_length_2028 = self.read_cafcarb_output(2028, "bau", "Trip_Band")
        trip_length_2028 = self.apply_annualisation(trip_length_2028, ["Emissions (tCO2e)"])
        trip_length_2028 = trip_length_2028[["LTA", "Trip Length", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Trip Length"], as_index=False).sum()
        trip_length_2019 = self.lininterpol(trip_length_2018, trip_length_2028, 2018, 2028, 2019,
                                            ["Emissions (tCO2e)"])
        trip_length_2019.to_csv(r"A:\QCR- assignments\Data for WSP\2019 Trip Length.csv")

        genesis_2018 = self.read_cafcarb_output(2018, "bau", "Genesis")
        genesis_2018 = self.apply_annualisation(genesis_2018, ["Emissions (tCO2e)"])
        genesis_2018 = genesis_2018[["LTA", "Genesis", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Genesis"], as_index=False).sum()
        genesis_2028 = self.read_cafcarb_output(2028, "bau", "Genesis")
        genesis_2028 = self.apply_annualisation(genesis_2028, ["Emissions (tCO2e)"])
        genesis_2028 = genesis_2028[["LTA", "Genesis", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Genesis"], as_index=False).sum()
        genesis_2019 = self.lininterpol(genesis_2018, genesis_2028, 2018, 2028, 2019,
                                        ["Emissions (tCO2e)"])
        genesis_2019.to_csv(r"A:\QCR- assignments\Data for WSP\2019 Genesis.csv")

        type_2018 = self.read_cafcarb_output(2018, "bau", "Place_Type")
        type_2018 = self.apply_annualisation(type_2018, ["Emissions (tCO2e)"])
        type_2018 = type_2018[["LTA", "Origin Place Type", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Origin Place Type"], as_index=False).sum()
        type_2028 = self.read_cafcarb_output(2028, "bau", "Place_Type")
        type_2028 = self.apply_annualisation(type_2028, ["Emissions (tCO2e)"])
        type_2028 = type_2028[["LTA", "Origin Place Type", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Origin Place Type"], as_index=False).sum()
        type_2019 = self.lininterpol(type_2018, type_2028, 2018, 2028, 2019,
                                        ["Emissions (tCO2e)"])
        type_2019.to_csv(r"A:\QCR- assignments\Data for WSP\2019 Origin Place Type.csv")

        vehicle_2043 = self.read_cafcarb_output(2043, "bau", "Vehicle_Type")
        vehicle_2043 = self.apply_annualisation(vehicle_2043, ["Emissions (tCO2e)"])
        vehicle_2043 = vehicle_2043.rename(columns={"vehicle_type": "Vehicle"})
        vehicle_2043 = vehicle_2043[["LTA", "Vehicle", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Vehicle"], as_index=False).sum()
        vehicle_2048 = self.read_cafcarb_output(2048, "bau", "Vehicle_Type")
        vehicle_2048 = self.apply_annualisation(vehicle_2048, ["Emissions (tCO2e)"])
        vehicle_2048 = vehicle_2048.rename(columns={"vehicle_type": "Vehicle"})
        vehicle_2048 = vehicle_2048[["LTA", "Vehicle", "Emissions (tCO2e)"]].groupby(
            ["LTA", "Vehicle"], as_index=False).sum()
        vehicle_2050 = self.lininterpol(vehicle_2043, vehicle_2048, 2043, 2048, 2050,
                                        ["Emissions (tCO2e)"])
        vehicle_2050.to_csv(r"A:\QCR- assignments\Data for WSP\2050 Vehicle.csv")

        emissions_2018 = self.read_cafcarb_output(2018, "bau", "CO2_Info")
        emissions_2018 = self.apply_annualisation(emissions_2018, ["Emissions (MtCO2e)"])
        emissions_2018 = emissions_2018[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_2028 = self.read_cafcarb_output(2028, "bau", "CO2_Info")
        emissions_2028 = self.apply_annualisation(emissions_2028, ["Emissions (MtCO2e)"])
        emissions_2028 = emissions_2028[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_2038 = self.read_cafcarb_output(2038, "bau", "CO2_Info")
        emissions_2038 = self.apply_annualisation(emissions_2038, ["Emissions (MtCO2e)"])
        emissions_2038 = emissions_2038[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_2043 = self.read_cafcarb_output(2043, "bau", "CO2_Info")
        emissions_2043 = self.apply_annualisation(emissions_2043, ["Emissions (MtCO2e)"])
        emissions_2043 = emissions_2043[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_2048 = self.read_cafcarb_output(2048, "bau", "CO2_Info")
        emissions_2048 = self.apply_annualisation(emissions_2048, ["Emissions (MtCO2e)"])
        emissions_2048 = emissions_2048[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()

        ue_emissions_2018 = self.read_cafcarb_output(2018, "ue", "CO2_Info")
        ue_emissions_2018 = self.apply_annualisation(ue_emissions_2018, ["Emissions (MtCO2e)"])
        ue_emissions_2018 = ue_emissions_2018[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_2028 = self.read_cafcarb_output(2028, "ue", "CO2_Info")
        ue_emissions_2028 = self.apply_annualisation(ue_emissions_2028, ["Emissions (MtCO2e)"])
        ue_emissions_2028 = ue_emissions_2028[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_2038 = self.read_cafcarb_output(2038, "ue", "CO2_Info")
        ue_emissions_2038 = self.apply_annualisation(ue_emissions_2038, ["Emissions (MtCO2e)"])
        ue_emissions_2038 = ue_emissions_2038[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_2043 = self.read_cafcarb_output(2043, "ue", "CO2_Info")
        ue_emissions_2043 = self.apply_annualisation(ue_emissions_2043, ["Emissions (MtCO2e)"])
        ue_emissions_2043 = ue_emissions_2043[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_2048 = self.read_cafcarb_output(2048, "ue", "CO2_Info")
        ue_emissions_2048 = self.apply_annualisation(ue_emissions_2048, ["Emissions (MtCO2e)"])
        ue_emissions_2048 = ue_emissions_2048[["LTA", "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()

        co2_emissions = self.create_co2_emissions(emissions_2018,
                                                 emissions_2028,
                                                 emissions_2038,
                                                 emissions_2043,
                                                 emissions_2048)
        co2_emissions = pd.pivot(co2_emissions, index="LTA", columns="year", values="Emissions (MtCO2e)")
        co2_emissions.to_csv(r"A:\QCR- assignments\Data for WSP\co2_emissions_bau.csv")

        co2_emissions_ue = self.create_co2_emissions(ue_emissions_2018,
                                                    ue_emissions_2028,
                                                    ue_emissions_2038,
                                                    ue_emissions_2043,
                                                    ue_emissions_2048)
        co2_emissions_ue = pd.pivot(co2_emissions_ue, index="LTA", columns="year", values="Emissions (MtCO2e)")
        co2_emissions_ue.to_csv(r"A:\QCR- assignments\Data for WSP\co2_emissions_ue.csv")

        emissions_internal_2018 = self.read_cafcarb_output(2018, "bau", "Genesis")
        emissions_internal_2018 = emissions_internal_2018[emissions_internal_2018["Genesis"] == "Internal"].rename(columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        emissions_internal_2018["Emissions (MtCO2e)"] = emissions_internal_2018["Emissions (MtCO2e)"] / 1000000
        emissions_internal_2018 = self.apply_annualisation(emissions_internal_2018, ["Emissions (MtCO2e)"])
        emissions_internal_2018 = emissions_internal_2018[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_internal_2028 = self.read_cafcarb_output(2028, "bau", "Genesis")
        emissions_internal_2028 = emissions_internal_2028[emissions_internal_2028["Genesis"] == "Internal"].rename(columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        emissions_internal_2028["Emissions (MtCO2e)"] = emissions_internal_2028["Emissions (MtCO2e)"] / 1000000
        emissions_internal_2028 = self.apply_annualisation(emissions_internal_2028, ["Emissions (MtCO2e)"])
        emissions_internal_2028 = emissions_internal_2028[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_internal_2038 = self.read_cafcarb_output(2038, "bau", "Genesis")
        emissions_internal_2038 = emissions_internal_2038[emissions_internal_2038["Genesis"] == "Internal"].rename(columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        emissions_internal_2038["Emissions (MtCO2e)"] = emissions_internal_2038["Emissions (MtCO2e)"] / 1000000
        emissions_internal_2038 = self.apply_annualisation(emissions_internal_2038, ["Emissions (MtCO2e)"])
        emissions_internal_2038 = emissions_internal_2038[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_internal_2043 = self.read_cafcarb_output(2043, "bau", "Genesis")
        emissions_internal_2043 = emissions_internal_2043[emissions_internal_2043["Genesis"] == "Internal"].rename(columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        emissions_internal_2043["Emissions (MtCO2e)"] = emissions_internal_2043["Emissions (MtCO2e)"] / 1000000
        emissions_internal_2043 = self.apply_annualisation(emissions_internal_2043, ["Emissions (MtCO2e)"])
        emissions_internal_2043 = emissions_internal_2043[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        emissions_internal_2048 = self.read_cafcarb_output(2048, "bau", "Genesis")
        emissions_internal_2048 = emissions_internal_2048[emissions_internal_2048["Genesis"] == "Internal"].rename(columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        emissions_internal_2048["Emissions (MtCO2e)"] = emissions_internal_2048["Emissions (MtCO2e)"] / 1000000
        emissions_internal_2048 = self.apply_annualisation(emissions_internal_2048, ["Emissions (MtCO2e)"])
        emissions_internal_2048 = emissions_internal_2048[["LTA",  "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()

        ue_emissions_internal_2018 = self.read_cafcarb_output(2018, "ue", "Genesis")
        ue_emissions_internal_2018 = ue_emissions_internal_2018[ue_emissions_internal_2018["Genesis"] == "Internal"].rename(
            columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        ue_emissions_internal_2018["Emissions (MtCO2e)"] = ue_emissions_internal_2018["Emissions (MtCO2e)"] / 1000000
        ue_emissions_internal_2018 = self.apply_annualisation(ue_emissions_internal_2018, ["Emissions (MtCO2e)"])
        ue_emissions_internal_2018 = ue_emissions_internal_2018[["LTA", "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_internal_2028 = self.read_cafcarb_output(2028, "ue", "Genesis")
        ue_emissions_internal_2028 = ue_emissions_internal_2028[ue_emissions_internal_2028["Genesis"] == "Internal"].rename(
            columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        ue_emissions_internal_2028["Emissions (MtCO2e)"] = ue_emissions_internal_2028["Emissions (MtCO2e)"] / 1000000
        ue_emissions_internal_2028 = self.apply_annualisation(ue_emissions_internal_2028, ["Emissions (MtCO2e)"])
        ue_emissions_internal_2028 = ue_emissions_internal_2028[["LTA", "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_internal_2038 = self.read_cafcarb_output(2038, "ue", "Genesis")
        ue_emissions_internal_2038 = ue_emissions_internal_2038[ue_emissions_internal_2038["Genesis"] == "Internal"].rename(
            columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        ue_emissions_internal_2038["Emissions (MtCO2e)"] = ue_emissions_internal_2038["Emissions (MtCO2e)"] / 1000000
        ue_emissions_internal_2038 = self.apply_annualisation(ue_emissions_internal_2038, ["Emissions (MtCO2e)"])
        ue_emissions_internal_2038 = ue_emissions_internal_2038[["LTA", "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_internal_2043 = self.read_cafcarb_output(2043, "ue", "Genesis")
        ue_emissions_internal_2043 = ue_emissions_internal_2043[ue_emissions_internal_2043["Genesis"] == "Internal"].rename(
            columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        ue_emissions_internal_2043["Emissions (MtCO2e)"] = ue_emissions_internal_2043["Emissions (MtCO2e)"] / 1000000
        ue_emissions_internal_2043 = self.apply_annualisation(ue_emissions_internal_2043, ["Emissions (MtCO2e)"])
        ue_emissions_internal_2043 = ue_emissions_internal_2043[["LTA", "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()
        ue_emissions_internal_2048 = self.read_cafcarb_output(2048, "ue", "Genesis")
        ue_emissions_internal_2048 = ue_emissions_internal_2048[ue_emissions_internal_2048["Genesis"] == "Internal"].rename(
            columns={"Emissions (tCO2e)": "Emissions (MtCO2e)"})
        ue_emissions_internal_2048["Emissions (MtCO2e)"] = ue_emissions_internal_2048["Emissions (MtCO2e)"] / 1000000
        ue_emissions_internal_2048 = self.apply_annualisation(ue_emissions_internal_2048, ["Emissions (MtCO2e)"])
        ue_emissions_internal_2048 = ue_emissions_internal_2048[["LTA", "Emissions (MtCO2e)"]].groupby(
            ["LTA", ], as_index=False).sum()

        co2_emissions = self.create_co2_emissions(emissions_internal_2018,
                                                     emissions_internal_2028,
                                                     emissions_internal_2038,
                                                     emissions_internal_2043,
                                                     emissions_internal_2048)
        co2_emissions = pd.pivot(co2_emissions, index="LTA", columns="year", values="Emissions (MtCO2e)")
        co2_emissions.to_csv(r"A:\QCR- assignments\Data for WSP\co2_emissions_internal_bau.csv")

        co2_emissions_internal_ue = self.create_co2_emissions(ue_emissions_internal_2018,
                                                    ue_emissions_internal_2028,
                                                    ue_emissions_internal_2038,
                                                    ue_emissions_internal_2043,
                                                    ue_emissions_internal_2048)
        co2_emissions_internal_ue = pd.pivot(co2_emissions_internal_ue, index="LTA", columns="year", values="Emissions (MtCO2e)")
        co2_emissions_internal_ue.to_csv(r"A:\QCR- assignments\Data for WSP\co2_emissions_internal_ue.csv")


PlaybookProcess()
