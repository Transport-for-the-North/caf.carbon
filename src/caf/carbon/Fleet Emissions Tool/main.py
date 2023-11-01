import configparser as cf

from lib import scenario_invariant, scenario_dependent, projection, output_figures, postgres_normalisation

# %% Load config file
config = cf.ConfigParser(interpolation=cf.ExtendedInterpolation())

# Indicate input/output zoning system
run_type = "MSOA"  # Options currently limited only to "MSOA"

# Specify if NoHAM demand inputs are separated into AM, IP, PM time periods
time_period = False
ev_redistribution = True

if time_period:
    time_period_list = ["AM", "IP", "PM"]
else:
    time_period_list = ["aggregated"]

config.read("config_local.txt")
outpath = "CAFCarb/outputs/"
# %% Scenario Agnostic
index_fleet = scenario_invariant.IndexFleet(config, run_type, outpath, run_fresh=False)
invariant_data = scenario_invariant.Invariant(index_fleet, config, run_type, time_period)

if run_type == "MSOA":
    # State whether you want to generate baseline projections or decarbonisation
    # pathway projections
    pathway = "none"  # Options include "none", "element"

    # %% Scenario Dependent
    for time in time_period_list:
        for i in ["Business As Usual Core",
                  "Accelerated EV Core"]:
            print("\n\n\n###################")
            print(i, time)
            print("###################")
            scenario = scenario_dependent.Scenario(config, run_type, time_period, time, i, invariant_data, pathway)
            model = projection.Model(config, time, time_period, invariant_data, scenario, ev_redistribution, outpath)
            model.allocate_chainage()
            model.predict_emissions()
            model.save_output()

        # %% Save summary outputs and plots for all scenarios if demand not time period specific
    if not time_period:
        generate_outputs = True
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
