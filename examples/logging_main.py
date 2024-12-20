import logging
from caf.carbon.load_data import LOG_PATH
from caf.carbon import fleet_emission_model, vkm_emissions_model, carbon_vkms_to_cafcarb
from caf.toolkit.log_helpers import LogHelper, ToolDetails


def main():
    """Run CAF.Carbon"""
    # GENERAL PARAMETERS #
    # What is the name of the overarching model run? This will appear in output file names.
    run_name = "TfN_Decarb_Refresh"
    # Which STB regions is the model being run for? "Transport for the North", "Midlands Connect", "Transport East"...
    regions = ["Transport for the North"]
    # Which travel scenarios to run with? ["Business As Usual Core", "Accelerated EV Core"]
    # or ["Just About Managing", "Prioritised Places", "Digitally Distributed", "Urban Zero Carbon"]
    scenarios = ["Business As Usual Core"]
    # Is the model framework being run for the first time? This will create mediating files like the base year fleet
    # which do not need to be run every time
    run_fresh = False
    # Is the model following a strategy trajectory? Default "none" will simply follow scenarios.
    pathway = "none"  # Options include "none", "decarb", "old"

    # PREPROCESSING PARAMETERS #
    run_preprocessing = True


    # FLEET MODEL PARAMETERS #
    # Is the Fleet Emissions model being run? This is the default mode to run CAFCarb in.
    run_fleet = False
    # What is the model base year? ONLY 2018 or 2023 can be selected.
    # Selecting 2018 will run an older version of CAFCarb
    fleet_year = 2023
    # What demand years are being used? These should be years with VKM data,
    # the fleet is always projected for every year until 2050.
    years_to_include = [fleet_year, 2025, 2030, 2035, 2040, 2045, 2050]
    # Should EVs appear universally, or based on local income? Default = False
    ev_redistribution = False
    # Is the EV redistribution being run for the first time? Default = same as run_fresh
    ev_redistribution_fresh = run_fresh

    # VKM MODEL PARAMETERS
    # Is the VKM Emissions model being run? This is a lower intensity/nuance carbon tool.
    run_vkm = True
    # The years to be run, should match available base and forecast years
    run_list = [2023]
    # This should correspond to the input files pathway naming conventions.
    vkm_scenarios = ["BAU_ev_mandate"]

    # POSTPROCESSING PARAMETERS #

    # run the fleet emissions model
    if run_fleet:
        fleet_emission_model.FleetEmissionsModel(
            regions, ev_redistribution, scenarios, run_fresh, run_name, fleet_year, pathway, ev_redistribution_fresh,
            years_to_include
        )
    if run_vkm:
        if run_preprocessing:
            for year in run_list:
                carbon_vkms_to_cafcarb.VKMPreprocessing(year)
        vkm_emissions_model.VKMEmissionsModel(run_list, run_fresh, regions, vkm_scenarios)


if __name__ == '__main__':
    log = logging.getLogger('__main__')
    log.setLevel(logging.DEBUG)
    details = ToolDetails("caf.carbon", "1.0.0")
    main()
