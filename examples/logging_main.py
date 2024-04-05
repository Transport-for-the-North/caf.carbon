# Built-Ins
import logging

# Third Party
from caf.toolkit.log_helpers import LogHelper, ToolDetails

# Local Imports
from src.caf.carbon import fleet_emission_model, vkm_emissions_model
from src.caf.carbon.load_data import LOG_PATH


def main():
    """Run CAF.Carbon"""
    # Which regions is the model being run for?
    # ["North West", "North East", "Yorkshire and The Humber", "East of England"
    #  "East Midlands", "West Midlands", "South East", "South West"]
    regions = ["North West", "North East", "Yorkshire and The Humber"]
    # Which travel scenarios to run with? ["Business As Usual Core", "Accelerated EV Core"]
    scenarios = ["Business As Usual Core", "Accelerated EV Core"]
    run_fresh = False
    run_name = "TfN"
    # Distribute EVs without income assumptions (False) or with income factors (True)?
    ev_redistribution = False

    # run the fleet and vkm emissions models?
    run_vkm = False
    run_fleet = True

    fleet_year = 2023

    # run the fleet emissions model
    if run_fleet:
        run_fleet_emissions = fleet_emission_model.FleetEmissionsModel(
            regions, ev_redistribution, scenarios, run_fresh, run_name, fleet_year
        )
    if run_vkm:
        run_vkm_emissions = vkm_emissions_model.VkmEmissionsModel(regions, scenarios)


if __name__ == "__main__":
    log = logging.getLogger("__main__")
    log.setLevel(logging.DEBUG)
    details = ToolDetails("caf.carbon", "1.0.0")
    main()
    # with LogHelper(__package__, details, log_file=LOG_PATH):
    #     main()
