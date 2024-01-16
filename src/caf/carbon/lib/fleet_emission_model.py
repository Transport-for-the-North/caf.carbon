import configparser as cf
import pandas as pd
import logging
from lib import scenario_invariant, scenario_dependent, projection, output_figures, postgres_normalisation
from lib.load_data import REGION_FILTER


class FleetEmissionsModel:
    def __init__(self, regions, ev_redistribution, time_period, scenario_list):
        # %% Load config file
        LOG = logging.getLogger(__name__)
        LOG.info("Log messages using module logger")
        region_filter = pd.read_csv(REGION_FILTER)
        region_filter = region_filter[region_filter["region"].isin(regions)]
        config = cf.ConfigParser(interpolation=cf.ExtendedInterpolation())

        # Indicate input/output zoning system
        run_type = "MSOA"  # Options currently limited only to "MSOA"
        # Specify if NoHAM demand inputs are separated into AM, IP, PM time periods
        if time_period:
            time_period_list = ["AM", "IP", "PM"]
        else:
            time_period_list = ["aggregated"]
        config.read("config_local.txt")
        outpath = "CAFCarb/outputs/"
        # %% Scenario Agnostic
        index_fleet = scenario_invariant.IndexFleet(config, run_type, outpath, run_fresh=True)
        invariant_data = scenario_invariant.Invariant(index_fleet, config, run_type, time_period)
        if run_type == "MSOA":
            # State whether you want to generate baseline projections or decarbonisation
            # pathway projections
            pathway = "none"  # Options include "none", "element"
            # %% Scenario Dependent
            for time in time_period_list:
                for i in scenario_list:
                    print("\n\n\n###################")
                    print(i, time)
                    print("###################")
                    scenario = scenario_dependent.Scenario(config, run_type, region_filter, time_period, time, i, invariant_data,
                                                           pathway)
                    model = projection.Model(config, time, time_period, region_filter, invariant_data, scenario, ev_redistribution,
                                             outpath)
                    model.allocate_chainage()
                    model.predict_emissions()
                    model.save_output()

                # %% Save summary outputs and plots for all scenarios if demand not time period specific
            if not time_period:
                generate_outputs = False
                if generate_outputs:
                    summary_outputs = output_figures.SummaryOutputs(config,
                                                                    time_period_list,
                                                                    outpath,
                                                                    pathway,
                                                                    invariant_data,
                                                                    model)
                    summary_outputs.plot_effects()
                    summary_outputs.plot_fleet()

                normalise_outputs = False
                if normalise_outputs:
                    postgres_outputs = postgres_normalisation.NormaliseOutputs(summary_outputs, outpath)
