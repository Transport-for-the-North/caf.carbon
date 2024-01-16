import logging
from src.caf.carbon.lib import fleet_emission_model, vkm_emissions_model
from caf.toolkit.log_helpers import LogHelper, ToolDetails


def main():

    # Which regions is the model being run for?
    # ["North West", "North East", "Yorkshire and The Humber", "East of England"
    #  "East Midlands", "West Midlands", "South East", "South West"]
    regions = ["North West", "North East", "Yorkshire and The Humber"]
    LOG = logging.getLogger('__main__')
    LOG.setLevel(logging.DEBUG)
    LOG.info("Starting weighted translation")

    # Which travel scenarios to run with? ["Business As Usual Core", "Accelerated EV Core"]
    scenarios = ["Business As Usual Core"]

    # Distribute EVs without income assumptions (False) or with income factors (True)?
    ev_redistribution = False

    # Is the demand data aggregated by time period (time_period = False)
    # or broken down across AM, IP, PM (time_period = True)?
    time_period = False

    # run the fleet and vkm emissions models?
    run_vkm = False
    run_fleet = True

    # run the fleet emissions model
    if run_fleet:
        run_fleet_emissions = fleet_emission_model.FleetEmissionsModel(regions, ev_redistribution, time_period, scenarios)
    if run_vkm:
        run_vkm_emissions = vkm_emissions_model.VkmEmissionsModel(regions, scenarios)

LOG = logging.getLogger(__name__)


if __name__ == '__main__':

    path = "CAFCarb/outputs/cafcarbon.log"
    LOG = logging.getLogger('__main__')
    LOG.setLevel(logging.DEBUG)
    details = ToolDetails("caf.carbon", "1.0.0")
    LOG.info("test 1")
    with LogHelper(__package__, details, log_file=path):
        LOG.info("test 2")
        main()

