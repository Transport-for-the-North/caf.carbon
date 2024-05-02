# Built-Ins
import configparser as cf

# Third Party
import pandas as pd

# Local Imports
from caf.carbon import (
    output_figures,
    projection,
    scenario_dependent,
    scenario_invariant,
)
from caf.carbon.load_data import OUT_PATH, REGION_FILTER


class FleetEmissionsModel:
    """Calculate fleet emissions from fleet and demand data."""

    def __init__(
        self, regions, ev_redistribution, scenario_list, run_fresh, run_name, fleet_year, pathway
    ):

        # %% Load config file
        region_filter = pd.read_csv(REGION_FILTER)
        region_filter = region_filter[region_filter["region"].isin(regions)]

        # %% Scenario Agnostic
        index_fleet = scenario_invariant.IndexFleet(run_fresh, fleet_year)
        invariant_data = scenario_invariant.Invariant(index_fleet, fleet_year)

        # %% Scenario Dependent
        for i in scenario_list:
            print("\n\n\n###################")
            print(i)
            print("###################")
            scenario = scenario_dependent.Scenario(region_filter, i, invariant_data, pathway)
            model = projection.Model(
                region_filter, invariant_data, scenario, ev_redistribution, run_name
            )
            model.allocate_chainage()
            model.predict_emissions()
            model.save_output()

        # %% Save summary outputs and plots for all scenarios if demand not time period specific
        generate_outputs = False
        if generate_outputs:
            summary_outputs = output_figures.SummaryOutputs(pathway, invariant_data, model)
            summary_outputs.plot_effects()
            summary_outputs.plot_fleet()
