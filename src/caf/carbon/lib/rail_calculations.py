import pandas as pd
import math

from lib import utility as ut


class Rail:
    """Calculate and predict rail base and current emissions using emissions, demand and service data."""

    def __init__(self, config, scenario_obj, outpath):
        """Initialise functions and set class variables.

                Parameters
                ----------
                config : configparser
                    Filepath lookup to import tables
                scenario_obj : class obj
                    Scenario, if applicable
                outpath : str
                    Filepath to export preprocessed tables.
                """
        print("Initialising Rail Calculations")
        self.outpath = outpath
        self.configuration = config
        self.scenario = scenario_obj
        # to be adjusted
        self.run_name = "partner_preferred"
        self.scheme_years = [2018, 2042, 2052]
        self.base_year, self.year2042_year, self.year2052_year = self.scheme_years[0], self.scheme_years[1],\
            self.scheme_years[2]
        self.base_code, self.year2042_code, self.year2052_code = "IGX", "K1P", "K1Q"

        self.projected_years = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032,
                                2033, 2034, 2035, 2036, 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046,
                                2047, 2048, 2049, 2050, 2051]
        # [2020, 2025, 2030, 2035, 2040, 2045, 2050]

        self.__load_data()
        self.__calculate_base_emissions()
        self.__interpolate_future_emissions()
        self.__create_zonal_assignment()
        self.__create_regional_metrics()
        print("Finalised rail emissions")

    def __load_data(self):

        path = self.configuration["filePaths"]["NoRMSFile"]
        self.bus_emission = pd.read_csv(self.configuration["filePaths"]["busEmissionFile"])
        self.station_zones = pd.read_csv(self.configuration["filePaths"]["railStationNodes"])
        self.rail_emission = pd.read_csv(self.configuration["filePaths"]["railEmissionFile"])
        rail_vehicles = pd.read_csv(self.configuration["filePaths"]["vehicleDictionaryFile"])
        rail_consumption = pd.read_csv(self.configuration["filePaths"]["railConsumptionFile"])
        # unit conversion from miles to kilometres
        rail_consumption["consumption rate"] = rail_consumption["consumption rate"] / 1.609344
        self.vehicle_consumption = rail_vehicles.merge(rail_consumption, how="left", on="vehicle type")

        base = pd.read_csv("%s/" % path+self.base_code+"_2018/"+self.base_code+"_2018_AM_On_Offs_NoCarb.csv")
        # replace with national
        national_rail_movement = pd.read_csv("%s/" % path+self.base_code+"_2018/"+self.base_code +
                                             "_2018_AM_On_Offs_NoCarb.csv")
        year2042 = pd.read_csv("%s/" % path+self.year2042_code+"_2042/"+self.year2042_code +
                               "_2042_AM_On_Offs_NoCarb.csv")
        year2052 = pd.read_csv("%s/" % path+self.year2052_code+"_2052/"+self.year2052_code +
                               "_2052_AM_On_Offs_NoCarb.csv")

        base["period"], year2052["period"], year2042["period"] = "AM", "AM", "AM"
        for period in ["IP", "PM", "OP"]:
            time_segment_2018 = pd.read_csv("%s/" % path+self.base_code+"_2018/"+self.base_code +
                                            "_2018_%s_On_Offs_NoCarb.csv" % period)
            # replace with national
            time_segment_2042 = pd.read_csv("%s/" % path+self.year2042_code+"_2042/"+self.year2042_code +
                                            "_2042_%s_On_Offs_NoCarb.csv" % period)
            time_segment_2052 = pd.read_csv("%s/" % path+self.year2052_code+"_2052/"+self.year2052_code +
                                            "_2052_%s_On_Offs_NoCarb.csv" % period)
            time_segment_2018["period"], time_segment_2042["period"], time_segment_2052["period"]\
                = period, period, period
            base, year2052, year2042 = pd.concat([base, time_segment_2018]), pd.concat([year2052, time_segment_2042]),\
                pd.concat([year2042, time_segment_2052])

        base["year"], year2042["year"], year2052["year"] = self.base_year, self.year2042_year, self.year2052_year

        self.scheme_combined = pd.concat([base, year2052])
        self.scheme_combined = pd.concat([self.scheme_combined, year2042])

        pt_emission_index_reductions = pd.read_excel(
            io=self.configuration["filePaths"]["scenarioFile"],
            sheet_name=self.scenario,
            header=1,
            usecols=self.configuration["fileStructure"]["ptEmissionReduction"]).dropna()

    def __speed_curve_emissions(self, vehicle_type, speed, dist):
        if vehicle_type == "Bus":
            a, b, c, d, e, f, g = self.bus_emission.loc[0]["a"], self.bus_emission.loc[0]["b"], \
                                  self.bus_emission.loc[0]["c"], self.bus_emission.loc[0]["d"], \
                                  self.bus_emission.loc[0]["e"], self.bus_emission.loc[0]["f"], \
                                  self.bus_emission.loc[0]["g"]
            g_kilometre = (a + b * speed + c * speed ** 2 + d * speed ** 3 + e * speed ** 4
                           + f * speed ** 5 + g * speed ** 6) / speed
            kg = (g_kilometre / 1000) * dist
            return kg

    def __calculate_base_emissions(self):

        scheme_emissions = self.scheme_combined.copy()
        # scheme_emissions["g CO2 per passenger kilometre"] = 36.66  # to replace
        # scheme_emissions["g CO2"] = scheme_emissions["g CO2 per passenger kilometre"]\
        #     * scheme_emissions["VOL"] * scheme_emissions["DIST_KM"]



        scheme_emissions = scheme_emissions.merge(self.rail_emission, how="left", on="year")
        scheme_emissions["VCLASS"] = scheme_emissions["VCLASS"].astype(str)
        scheme_emissions = scheme_emissions.merge(self.vehicle_consumption, how="left", on="VCLASS")

        # carriage * distance * consumption * emission rate - diesel or electric branch
        scheme_emissions.loc[(scheme_emissions["VTYPE"].isin(["DMU", "Loco"])), "kg CO2e"] = \
            scheme_emissions["VCARRIAGES"] * scheme_emissions["DIST_KM"] \
            * scheme_emissions["consumption rate"] * scheme_emissions["Kg CO2e/l"]
        scheme_emissions.loc[(scheme_emissions["VTYPE"].isin(["EMU", "HS"])), "kg CO2e"] = \
            scheme_emissions["VCARRIAGES"] * scheme_emissions["DIST_KM"] \
            * scheme_emissions["consumption rate"] * scheme_emissions["Kg CO2e/kWh"]

        scheme_emissions.loc[(scheme_emissions["VTYPE"] == "Bus"), "kg CO2e"] =\
            self.__speed_curve_emissions("Bus", scheme_emissions["SPEED_KPH"], scheme_emissions["DIST_KM"])

        norms_zoning = self.station_zones[["N", "ZONEID"]].copy()
        scheme_emissions = scheme_emissions.merge(norms_zoning, how="left", left_on="A", right_on="N")

        scheme_emissions.to_csv("{self.outpath}scheme_emissions_%s.csv".format(**locals()) % self.run_name)
        scheme_emissions["kg CO2e"], scheme_emissions["DIST_KM"] = scheme_emissions["kg CO2e"] * 363, scheme_emissions["DIST_KM"] * 363
        scheme_total = scheme_emissions[["year", "VTYPE", "DIST_KM", "kg CO2e"]].groupby(["year", "VTYPE"]).sum()
        scheme_total.to_csv("{self.outpath}annual_summary_emissions_%s.csv".format(**locals()) % self.run_name)
        scheme_zone_total = scheme_emissions[["year", "ZONEID", "kg CO2e"]].groupby(["year", "ZONEID"]).sum()
        scheme_zone_total.to_csv("{self.outpath}annual_zonal_emissions_%s.csv".format(**locals()) % self.run_name)
        self.scheme_emissions = scheme_emissions

    def __interpolate_future_emissions(self):

        scheme_emissions = self.scheme_combined.copy()

        for year in self.projected_years:
            if year < self.scheme_years[1]:
                new_scheme_year = self.scheme_combined.copy()
                new_scheme_year = new_scheme_year[new_scheme_year["year"] == self.scheme_years[0]]
                new_scheme_year["year"] = year
            elif year > self.scheme_years[2]:
                new_scheme_year = self.scheme_combined.copy()
                new_scheme_year = new_scheme_year[new_scheme_year["year"] == self.scheme_years[2]]
                new_scheme_year["year"] = year
            else:
                new_scheme_year = self.scheme_combined.copy()
                new_scheme_year = new_scheme_year[new_scheme_year["year"] == self.scheme_years[1]]
                new_scheme_year["year"] = year
            scheme_emissions = pd.concat([scheme_emissions, new_scheme_year])

        scheme_emissions = scheme_emissions.merge(self.rail_emission, how="left", on="year")
        scheme_emissions = scheme_emissions.merge(self.vehicle_consumption, how="left", on="VCLASS")

        # carriage * distance * consumption * emission rate - diesel or electric branch
        scheme_emissions.loc[(scheme_emissions["VTYPE"].isin(["DMU", "Loco"])), "kg CO2e"] = \
            scheme_emissions["VCARRIAGES"] * scheme_emissions["DIST_KM"] \
            * scheme_emissions["consumption rate"] * scheme_emissions["Kg CO2e/l"]
        scheme_emissions.loc[(scheme_emissions["VTYPE"].isin(["EMU", "HS"])), "kg CO2e"] = \
            scheme_emissions["VCARRIAGES"] * scheme_emissions["DIST_KM"] \
            * scheme_emissions["consumption rate"] * scheme_emissions["Kg CO2e/kWh"]

        scheme_emissions.loc[(scheme_emissions["VTYPE"] == "Bus"), "kg CO2e"] = \
            self.__speed_curve_emissions("Bus", scheme_emissions["SPEED_KPH"], scheme_emissions["DIST_KM"])

        norms_zoning = self.station_zones[["N", "ZONEID"]].copy()
        scheme_emissions = scheme_emissions.merge(norms_zoning, how="left", left_on="A", right_on="N")

        scheme_emissions.to_csv("{self.outpath}scheme_emissions_%s".format(**locals()) % "_interpolated_" +
                                self.run_name + ".csv")
        scheme_emissions["kg CO2e"], scheme_emissions["DIST_KM"] = scheme_emissions["kg CO2e"] * 363, scheme_emissions[
            "DIST_KM"] * 363
        scheme_total = scheme_emissions[["year", "VTYPE", "DIST_KM", "kg CO2e"]].groupby(["year", "VTYPE"]).sum()
        scheme_total.to_csv("{self.outpath}annual_summary_emissions_%s".format(**locals()) % "_interpolated_" +
                            self.run_name + ".csv")
        scheme_zone_total = scheme_emissions[["year", "ZONEID", "kg CO2e"]].groupby(["year", "ZONEID"]).sum()
        scheme_zone_total.to_csv("{self.outpath}annual_zonal_emissions_%s".format(**locals()) % "_interpolated_" +
                                 self.run_name + ".csv")
        self.scheme_emissions = scheme_emissions

    def __create_zonal_assignment(self):
        station_zones = pd.read_csv(self.configuration["filePaths"]["railStationNodes"])
        # od matrix on hold for now
        temp = True

    def __create_regional_metrics(self):
        temp = True
