import pandas as pd

# Local Imports
from caf.carbon import (
    projection,
    scenario_dependent,
    scenario_invariant,
    scenario_invariant_2018
)
from caf.carbon.load_data import REGION_FILTER, DEMAND_PATH


class FleetEmissionsModel:
    """Calculate fleet emissions from fleet and demand data."""
    def __init__(
        self, regions, ev_redistribution,
            scenario_list, run_fresh,
            run_name, fleet_year,
            pathway, ev_redistribution_fresh,
            years_to_include
    ):

        # %% Load config file
        region_filter = pd.read_csv(REGION_FILTER)
        region_filter = region_filter[region_filter["stb_name"].isin(regions)]

        # %% Scenario Agnostic
        if fleet_year == 2018:
            index_fleet = scenario_invariant_2018.IndexFleet(run_fresh, fleet_year)
            invariant_data = scenario_invariant_2018.Invariant(index_fleet, fleet_year)
        else:
            index_fleet = scenario_invariant.IndexFleet(run_fresh, fleet_year)
            invariant_data = scenario_invariant.Invariant(index_fleet, fleet_year)

        # %% Scenario Dependent

        for i in scenario_list:
            print("\n\n\n###################")
            print(i)
            print("###################")
            scenario = scenario_dependent.Scenario(i, invariant_data, pathway)
            model = projection.Model(
                region_filter, invariant_data, scenario, ev_redistribution, run_name, ev_redistribution_fresh
            )
            ev_redistribution_fresh = False
            self.first_enumeration = True
            self.years_to_include = years_to_include
            model.fleet_transform()
            for year in self.years_to_include:
                for time_period in ["TS1", "TS2", "TS3"]:
                    print(f"Running {time_period} {year}")
                    keystoenum = pd.HDFStore(str(DEMAND_PATH) + f"/{scenario.scenario_code}/"
                                             + f"vkm_by_speed_and_type_{year}_{time_period}_car.h5", mode="r").keys()
                    for demand_key in keystoenum:
                        print(f"Processing demand for key {demand_key}")
                        demand_data = scenario_dependent.Demand(scenario, year, time_period, demand_key)
                        print(f"Allocating emissions for key {demand_key}")
                        model.allocate_emissions(demand_data, year, time_period, self.first_enumeration)
                        if self.first_enumeration:
                            print("enumeration changed")
                            self.first_enumeration = False
