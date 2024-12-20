# -*- coding: utf-8 -*-
"""Tests for the scenario invariant processes"""
# Built-Ins
import os.path
from typing import Any

# Third Party
import pandas as pd
import pytest


# Local Imports
from caf.carbon.scenario_dependent import Scenario, Demand

# # # CONSTANTS # # #


# # # TESTS # # #
class TestScenario:
    def test__warp_tables(self, mock_warp_invariant_obj):
        exp_seg_sales = {'segment': ['medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium',
                                     'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium',
                                     'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium',
                                     'medium', 'medium', 'medium', 'medium'],
                         'year': [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035, 2036,
                                  2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047, 2048, 2049, 2050],
                         'segment_sales_distribution': [0.3, 0.325, 0.35, 0.352, 0.354, 0.356, 0.358, 0.36, 0.362,
                                                        0.364, 0.366, 0.368, 0.37, 0.372, 0.374, 0.376, 0.378, 0.38,
                                                        0.382, 0.384, 0.386, 0.388, 0.39, 0.392, 0.394, 0.396, 0.398,
                                                        0.4]}
        exp_seg_result = pd.DataFrame(data=exp_seg_sales)
        exp_fuel_seg = {'segment': ['medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium',
                                     'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium',
                                     'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium', 'medium',
                                     'medium', 'medium', 'medium', 'medium'],
                        'fuel': ['bev', 'bev', 'bev', 'bev', 'bev', 'bev', 'bev', 'bev',
                                 'bev', 'bev', 'bev', 'bev', 'bev', 'bev', 'bev', 'bev',
                                 'bev', 'bev', 'bev', 'bev', 'bev', 'bev', 'bev', 'bev',
                                 'bev', 'bev', 'bev', 'bev'],
                        'year': [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035, 2036,
                                 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047, 2048, 2049, 2050],
                        'segment_fuel_sales_distribution': [0.1, 0.125, 0.15, 0.18, 0.21, 0.24, 0.27, 0.3, 0.3, 0.3,
                                                            0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3,
                                                            0.3, 0.3, 0.3, 0.3, 0.3, 0.3]}
        exp_fuel_result = pd.DataFrame(data=exp_fuel_seg)
        exp_fleet = {'vehicle_type': ['car', 'car', 'car', 'car', 'car', 'car', 'car'],
                     'year': [2025, 2030, 2035, 2040, 2045, 2050, 2023],
                     'index_fleet_growth': [1.05, 1.113, 1.17978, 1.2505668, 1.325600808, 1.405136856, 1]}
        exp_fleet_result = pd.DataFrame(data=exp_fleet)
        exp_km = {'vehicle_type': ['car', 'hgv', 'car', 'hgv', 'car', 'hgv', 'car', 'hgv', 'car', 'hgv', 'car', 'hgv',
                                   'car', 'hgv'],
                  'msoa_area_type': [1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6],
                  'year': [2025, 2025, 2030, 2030, 2035, 2035, 2040, 2040, 2045, 2045, 2050, 2050, 2023, 2023],
                  'km_reduction': [1, 1, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 1]}
        exp_km_result = pd.DataFrame(data=exp_km)

        class_scenario = Scenario('Just About Managing', invariant_obj=mock_warp_invariant_obj, pathway='none')
        # class_scenario._Scenario__warp_tables()

        actual_seg_sales = class_scenario.seg_share_of_year_type_sales
        actual_fuel_sales = class_scenario.fuel_share_of_year_seg_sales
        actual_fleet = class_scenario.type_fleet_size_growth
        actual_km = class_scenario.km_index_reductions

        pd.testing.assert_frame_equal(actual_seg_sales, exp_seg_result, check_dtype=False, check_like=True)
        pd.testing.assert_frame_equal(actual_fuel_sales, exp_fuel_result, check_dtype=False, check_like=True)
        pd.testing.assert_frame_equal(actual_fleet, exp_fleet_result, check_dtype=False, check_like=True)
        pd.testing.assert_frame_equal(actual_km, exp_km_result, check_dtype=False, check_like=True)


class TestDemand:
    def test__load_demand(self, mock_load_scenario_obj):
        exp_data = {'origin': ['E02000001', 'E02000001', 'E02000001', 'E02000001', 'E02000001',
                               'E02000001', 'E02000001', 'E02000001', 'E02000001', 'E02000001',
                               'E02000001', 'E02000001', 'E02000001', 'E02000001', 'E02000001'],
                    'destination': ['E02000002', 'E02000002', 'E02000002', 'E02000002', 'E02000002',
                                    'E02000002', 'E02000002', 'E02000002', 'E02000002', 'E02000002',
                                    'E02000002', 'E02000002', 'E02000002', 'E02000002', 'E02000002'],
                    'through': ['E02000003', 'E02000003', 'E02000003', 'E02000003', 'E02000003',
                                'E02000003', 'E02000003', 'E02000003', 'E02000003', 'E02000003',
                                'E02000003', 'E02000003', 'E02000003', 'E02000003', 'E02000003'],
                    'vehicle_type': ['car', 'car', 'car', 'car', 'car',
                                     'lgv', 'lgv', 'lgv', 'lgv', 'lgv',
                                     'hgv', 'hgv', 'hgv', 'hgv', 'hgv'],
                    'user_class': [1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 1],
                    'trip_band': [1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1,
                                  1, 1, 1, 1, 1],
                    'speed_band': ['10_30', '30_50', '50_70', '70_90', '90_110',
                                   '10_30', '30_50', '50_70', '70_90', '90_110',
                                   '10_30', '30_50', '50_70', '70_90', '90_110'],
                    'vkm': [100, 200, 300, 400, 500,
                            100, 200, 300, 400, 500,
                            100, 200, 300, 400, 500]}
        exp_df = pd.DataFrame(data= exp_data)

        class_demand = Demand(scenario_obj=mock_load_scenario_obj, demand_year='2023', time_period= 'TS1',
                              demand_key='df')
        class_demand._Demand__load_demand(vehicle_type='car')

        actual_demand = class_demand.demand

        pd.testing.assert_frame_equal(actual_demand, exp_df, check_dtype=False, check_like=True)
