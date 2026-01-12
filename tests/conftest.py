import pandas as pd
import pytest
import os.path


# # # FIXTURES # # #
# fleet_archive data for basic_clean function in scenario_invariant.py
@pytest.fixture(scope="module")
def mock_basic_clean():
    ba_cl_data = pd.read_csv(os.path.join(os.getcwd(), "data_basic_clean/audit/fleet_archive.csv"))
    return ba_cl_data


# fleet_archive data for advanced_clean function in scenario_invariant.py
@pytest.fixture(scope="module")
def mock_advance_clean():
    ad_cl_data = pd.read_csv(os.path.join(os.getcwd(), "data_advance_clean/audit/fleet_archive.csv"))
    return ad_cl_data


# index_fleet data for split_tables function in scenario_invariant.py
@pytest.fixture(scope="module")
def mock_split_tables():
    split_data = pd.read_csv(os.path.join(os.getcwd(), "data_split_tables/audit/index_fleet.csv"))
    return split_data


# a class which holds the index_year data to replicate the invariant_obj input of the Scenario class
# used in warp_tables function in scenario_dependent.py
@pytest.fixture(scope="module")
def mock_warp_invariant_obj():
    class Test:
        index_year = 2023
    return Test


# a class which holds the index_year and scenario code data to replicate the scenario_obj input of the Demand class
# used in load_demand function in scenario_dependent.py
@pytest.fixture(scope="module")
def mock_load_scenario_obj():
    class Test:
        index_year = 2023
        scenario_code = 'JAM'
    return Test


# a class which holds the se_curve, index_year, scrappage and index_fleet data to replicate the invariant_obj input of
# the Model class (fleet section) used in predict_fleet_size, create_future_fleet, predict_sales and project_fleet
# functions in projection.py
@pytest.fixture(scope="module")
def mock_model_class_invariant_obj():
    class Test:
        se_curve = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_demand/se_curve.csv"))
        index_year = 2023
        scrappage_curve = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_fleet/scrappage_curve.csv"))
        ghg_equivalent = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_demand/ghg_equivalent.csv"))
        yearly_co2_reduction = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_demand/yearly_co2_reductions.csv"))
        grid_intensity = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_demand/grid_intensity.csv"))
        grid_consumption = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_demand/grid_consumption.csv"))
        anpr = pd.read_csv(os.path.join(os.getcwd(), "data_assign_chunk/anpr.csv"))

        class index_fleet:
            fleet = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_fleet/index_fleet.csv"))
    return Test


# a class which holds the fleet_growth, fuel_share_of_year_seg_sales, seg_share_of_year_type_sales, fleet_sales,
# fleet_size and scenario_initials data to replicate the scenario_obj input of the Model class (fleet section) used in
# predict_fleet_size, create_future_fleet, predict_sales and project_fleet functions in projection.py
@pytest.fixture(scope="module")
def mock_model_class_scenario_obj():
    class Test:
        type_fleet_size_growth = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_fleet/fleet_growth.csv"))
        fuel_share_of_year_seg_sales = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_fleet/fuel_share_of_year_seg_sales.csv"))
        seg_share_of_year_type_sales = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_fleet/seg_share_of_year_type_sales.csv"))
        fleet_sales = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_fleet/fleet_sales.csv"))
        fleet_size = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_fleet/fleet_size.csv"))
        co2_reductions = pd.read_csv(os.path.join(os.getcwd(), "data_Model(proj)_demand/co2_reductions.csv"))
        scenario_initials = 'JAM'
    return Test


# list of MSOAs included in testing, used in predict_fleet_size, create_future_fleet, predict_sales and project_fleet
# functions in projection.py as an input of Model class
@pytest.fixture(scope="module")
def mock_region_filter():
    data = {'MSOA11CD': ['E02000001', 'E02000002', 'E02000003', 'E02000004']}
    filters = pd.DataFrame(data=data)
    return filters


# the resulting output for __predict_sales function in projection.py. Given as a csv rather than hardcoded due to
# dataframe size
@pytest.fixture(scope="module")
def mock_predict_sales_result():
    data = pd.read_csv(os.path.join(os.getcwd(), "data_assign_chunk/fleet_sales_result.csv"))
    result = data.round(4)
    return result


# projected_fleet data for fleet_transform in projection.py
@pytest.fixture(scope="module")
def mock_fleet_transform():
    data = pd.read_csv(os.path.join(os.getcwd(), "data_assign_chunk/projected_fleet.csv"))
    return data


# fleet, demand, se_curve and grid_emissions data for assign_chunk_emissions in projection.py
@pytest.fixture(scope="module")
def mock_assign_chunk_emissions():
    class Test:
        fleet = pd.read_csv(os.path.join(os.getcwd(), "data_assign_chunk/fleet.csv"))
        demand = pd.read_csv(os.path.join(os.getcwd(), "data_assign_chunk/demand.csv"))
        se_curve = pd.read_csv(os.path.join(os.getcwd(), "data_assign_chunk/se_curve.csv"))
        emissions = pd.read_csv(os.path.join(os.getcwd(), "data_assign_chunk/emissions.csv"))
    return Test


# class that holds fleet_archive data to mock invariant_obj for functions inside CurveFitting class in
# scenario_invariant.py
@pytest.fixture(scope="module")
def mock_curvefitting_invariant_obj():
    class Test:
        real_world_coefficients = pd.read_csv(os.path.join(os.getcwd(), "data_curve_fitting/real_world_coeff.csv"))
        fuel_characteristics = pd.read_csv(os.path.join(os.getcwd(), "data_curve_fitting/fuel_characteristics.csv"))
        rw_multiplier = pd.read_csv(os.path.join(os.getcwd(), "data_curve_fitting/rw_multiplier.csv"))
        naei_coefficients = pd.read_csv(os.path.join(os.getcwd(), "data_curve_fitting/naei_coefficients.csv"))
        se_curve = pd.read_csv(os.path.join(os.getcwd(), "data_curve_fitting/se_curve.csv"))
        class index_fleet:
            fleet_archive = pd.read_csv(os.path.join(os.getcwd(), "data_curve_fitting/fleet_archive.csv"))
            characteristics = pd.read_csv(os.path.join(os.getcwd(), "data_curve_fitting/characteristics.csv"))
    return Test
