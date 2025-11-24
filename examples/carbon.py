import itertools
import logging
from caf.carbon import fleet_emission_model, vkm_emissions_model, carbon_vkms_to_cafcarb, _version
import caf.toolkit as ctk
import pathlib
import pydantic

import caf.toolkit as ctk
import pydantic

_NAME = "carbon"
LOG = logging.getLogger(_NAME)


class CarbonConfig(ctk.BaseConfig):
    """ NoCarb Parameters """

    run_fleet: bool
    run_vkm: bool
    scenarios: list[str]
    pathway: str
    vkm_scenarios: list[str]
    run_fresh: bool
    run_preprocessing: bool
    run_name: str
    fleet_year: int
    ev_redistribution: bool
    ev_redistribution_fresh: bool
    regions: list[str]
    run_list: list[int]
    years_to_include: list[int]
    demand_path: pydantic.DirectoryPath
    region_filter: pydantic.FilePath
    out_path: pydantic.DirectoryPath
    fleet_dir: pydantic.DirectoryPath
    msoa_body: pydantic.FilePath
    noham_to_msoa: pydantic.FilePath
    msoa_lad: pydantic.FilePath
    msoa_area_type: pydantic.FilePath
    target_area_type: pydantic.FilePath
    anpr: pydantic.FilePath
    vehicle_path: pydantic.FilePath
    postcode_msoa: pydantic.FilePath
    dvla_body: pydantic.FilePath
    seg_share: pydantic.FilePath
    fuel_share: pydantic.FilePath
    demand_path: pydantic.DirectoryPath
    vkm_demand: pydantic.DirectoryPath
    grid_profiles: pydantic.FilePath
    tail_pipe_profiles: pydantic.FilePath
    index_fleet_path: pydantic.FilePath
    vkm_out_path: pydantic.DirectoryPath
    general_table_path: pydantic.FilePath
    scenario_table_path: str
    vkm_input_folder: pydantic.DirectoryPath
    code_lookup: pydantic.FilePath
    log_path: str
    region_filter: pydantic.FilePath
    demographic_data: pydantic.FilePath
    traveller_data: pydantic.FilePath


def main():
    """Run CAF.Carbon"""

    config_path = pathlib.Path(__file__).with_suffix(".yml")
    parameters = CarbonConfig.load_yaml(config_path)
    details = ctk.ToolDetails(_NAME, _version.__version__)

    with ctk.LogHelper(_NAME, details, log_file=parameters.log_path):
        if parameters.run_fleet:
            fleet_emission_model.FleetEmissionsModel(parameters)
        if parameters.run_vkm:
            if parameters.run_preprocessing:
                for year in parameters.run_list:
                    carbon_vkms_to_cafcarb.VKMPreprocessing(year, parameters)
            vkm_emissions_model.VKMEmissionsModel(parameters)


if __name__ == '__main__':
    main()
