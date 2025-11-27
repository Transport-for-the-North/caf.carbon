import configparser as cf
import pandas as pd
from caf.carbon import projection, scenario_dependent, scenario_invariant, output_figures
from caf.carbon.load_data import REGION_FILTER, OUT_PATH


class FleetEmissionsModel:
    """Calculate fleet emissions from fleet and demand data."""

    def __init__(self, regions, ev_redistribution, time_period, scenario_list, run_fresh, run_name):
        # %% Load config file
        region_filter = pd.read_csv(REGION_FILTER)
        region_filter = region_filter.drop_duplicates()
        region_filter = region_filter[region_filter["region"].isin(regions)]

        # Specify if NoHAM demand inputs are separated into AM, IP, PM time periods
        if time_period:
            time_period_list = ["AM", "IP", "PM"]
        else:
            time_period_list = ["aggregated"]

        # %% Scenario Agnostic
        index_fleet = scenario_invariant.IndexFleet(run_fresh)
        invariant_data = scenario_invariant.Invariant(index_fleet, time_period)

        # State whether you want to generate baseline projections or decarbonization
        # pathway projections
        pathway = "none"  # Options include "none", "element"
        # %% Scenario Dependent
        for time in time_period_list:
            for i in scenario_list:
                print("\n\n\n###################")
                print(i, time)
                print("###################")
                scenario = scenario_dependent.Scenario(
                    region_filter, time_period, time, i, invariant_data, regions, pathway,
                )
                model = projection.Model(
                    time,
                    time_period,
                    region_filter,
                    invariant_data,
                    scenario,
                    ev_redistribution,
                    run_name
                )
                model.allocate_chainage()
                model.predict_emissions()
                model.save_output()

        # %% Save summary outputs and plots for all scenarios if demand not time period specific
        if not time_period:
            generate_outputs = False
            if generate_outputs:
                summary_outputs = output_figures.SummaryOutputs(
                    time_period_list, pathway, invariant_data, model
                )
                summary_outputs.plot_effects()
                summary_outputs.plot_fleet()
