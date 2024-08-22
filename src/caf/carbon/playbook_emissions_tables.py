import pandas as pd


class PlaybookProcess:

    def __init__(self, emissions_file):
        """Initialise functions and set class variables.

        Parameters
        ----------
        """
        self.years_to_iter = [2018]
        self.time_periods = ["TS1", "TS2", "TS3"]
        self.out_date = "DD_MM_YYYY"
        self.emissions_file = emissions_file
        self.working_directory = "D:/GitHub/caf.carbon/CAFCarb/outputs/"
        self.run_analysis()

    def read_cafcarb_output(self, year):
        year_emissions_data = pd.DataFrame()
        for time_period in self.time_periods:
            emissions_data = pd.read_hdf(
                f"{self.working_directory}/TfN_carbon_playbook_BAU"
                f"_fleet_emissions_{self.out_date}.h5", f"{time_period}_{year}", mode='r')
            emissions_data["time_period"] = time_period
            year_emissions_data = pd.concat([year_emissions_data, emissions_data])
        return year_emissions_data

    def apply_annualisation(self, day_data):

        return year_data

    def create_sheet_table(self, day_data):
        # interpolation
        return year_data

    def run_analysis(self):
        emissions_2018 = self.read_cafcarb_output(2018)