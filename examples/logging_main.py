import logging
from caf.carbon.load_data import LOG_PATH
from caf.carbon import fleet_emission_model, vkm_emissions_model
from caf.toolkit.log_helpers import LogHelper, ToolDetails


def main():
    """Run CAF.Carbon"""
    # Which regions is the model being run for?
    # ["Transport for the North", "Midlands Connect"]
    regions = ["Transport for the North"]
    # Which travel scenarios to run with? ["Business As Usual Core", "Accelerated EV Core"]

    # or ["Just About Managing", "Prioritised Places", "Digitally Distributed", "Urban Zero Carbon"]
    scenarios = ["Business As Usual Core"]
    run_fresh = False
    run_name = "TfN_carbon_playbook"

    # Distribute EVs without income assumptions (False) or with income factors (True)?
    ev_redistribution = False
    ev_redistribution_fresh = False
    # run the fleet and vkm emissions models?
    run_vkm = False
    run_fleet = True

    fleet_year = 2018
    years_to_include = [2028, 2038, 2043]  # years that appear in projected fleet and demand data
    # decarb pathway
    pathway = "none"  # Options include "none", "decarb", "old"

    # run the fleet emissions model
    if run_fleet:
        fleet_emission_model.FleetEmissionsModel(
            regions, ev_redistribution, scenarios, run_fresh, run_name, fleet_year, pathway, ev_redistribution_fresh,
            years_to_include
        )
    if run_vkm:
        vkm_emissions_model.VkmEmissionsModel(regions, scenarios)


if __name__ == '__main__':
    log = logging.getLogger('__main__')
    log.setLevel(logging.DEBUG)
    details = ToolDetails("caf.carbon", "1.0.0")
    main()
