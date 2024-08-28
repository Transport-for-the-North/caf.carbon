# -*- coding: utf-8 -*-
"""Tests for the scenario invariant processes"""
# Built-Ins
import os.path
from typing import Any

# Third Party
import pandas as pd
import pytest


# Local Imports
from caf.carbon.scenario_invariant import IndexFleet

# # # CONSTANTS # # #


# # # TESTS # # #
class TestIndexFleet:
    def test__basic_clean(self, mock_basic_clean):
        exp_data = {'zone': ['E02002052', 'E02002325', 'E02002325', 'E02002623', 'E02002623',
                             'E02002623', 'E02002627', 'E02002627', 'S02001261', 'S02001261'],
                    'tally': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                    'year': [2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023],
                    'fuel': ['bev', 'diesel', 'diesel', 'bev', 'petrol', 'phev', 'hydrogen', 'hydrogen', 'diesel',
                             'diesel'],
                    'avg_mass': [1500.0, 2000.0, 2000.0, 0.0, 0.0, 1200.0, 1000.0, 1000.0, 1000.0, 1000.0],
                    'avg_cc': [50, 1400, 1400, 3000, 3000, 1000, 2000, 2000, 1400, 1400],
                    'avg_co2': [100.0, 140.0, 140.0, 200.0, 200.0, 220.0, 150.0, 150.0, 110.0, 110.0],
                    'vehicle_type': ['car', 'car', 'car', 'lgv', 'lgv', 'car', 'car', 'car', 'car', 'car'],
                    'cya': [19, 19, 19, 19, 19, 19, 19, 19, 19, 19],
                    'segment': ['large-suv-executive', 'large-suv-executive', 'medium', 'n1 class i', 'n1 class i',
                                'large-suv-executive', 'large-suv-executive', 'medium', 'large-suv-executive',
                                'medium'],
                    'split': [1, 0.375, 0.625, 1, 1, 1, 0.375, 0.625, 0.375, 0.625]}
        expected_result = pd.DataFrame(data=exp_data)

        class_index = IndexFleet(False, os.path.join(os.getcwd(), "data_basic_clean"))
        class_index.fleet_archive = mock_basic_clean
        class_index.fleet_index_year = 2023
        class_index._IndexFleet__basic_clean()
        actual_result = class_index.fleet_archive

        pd.testing.assert_frame_equal(actual_result, expected_result, check_dtype=False, check_like=True)

    def test__advanced_clean(self, mock_advance_clean):
        exp_data = {'zone': ['E02002052', 'E02002052', 'E02002627', 'E02002623', 'E02002623',
                             'E02002325', 'S02001261', 'E02002623',],
                    'tally': [1, 1, 1, 1, 1, 1, 1, 1],
                    'year': [2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023],
                    'fuel': ['bev', 'bev', 'hydrogen', 'phev', 'petrol', 'diesel', 'diesel', 'petrol'],
                    'avg_mass': [1500.0, 750.0, 1000.0, 1200.0, 100.0, 1000.0, 1500.0, 100.0],
                    'avg_cc': [25, 50, 2000, 1000, 3000, 1400, 1400, 3000],
                    'avg_co2': [100.0, 50.0, 150.0, 220.0, 200.0, 140.0, 110.0, 200.0],
                    'vehicle_type': ['car', 'car', 'car', 'car', 'lgv', 'car', 'car', 'lgv'],
                    'cya': [19, 19, 19, 19, 19, 19, 19, 19],
                    'segment': ['large-suv-executive', 'large-suv-executive', 'medium', 'large-suv-executive',
                                'n1 class i', 'large-suv-executive', 'large-suv-executive', 'n1 class i']}
        expected_result = pd.DataFrame(data=exp_data)

        class_index = IndexFleet(False, os.path.join(os.getcwd(), "data_advance_clean"))
        class_index.fleet_archive = mock_advance_clean
        class_index.fleet_index_year = 2023
        class_index._IndexFleet__advanced_clean()
        actual_result = class_index.fleet

        pd.testing.assert_frame_equal(actual_result, expected_result, check_dtype=False, check_like=True)

    def test__split_tables(self, mock_split_tables):
        exp_fleet_data = {'zone': ['E02002052', 'E02002325', 'E02002623', 'E02002623', 'E02002623', 'E02002627',
                                   'S02001261'],
                          'tally': [1, 1, 1, 1, 1, 1, 2],
                          'year': [2023, 2023, 2023, 2023, 2023, 2023, 2023],
                          'fuel': ['bev', 'diesel', 'bev', 'petrol', 'phev', 'hydrogen', 'diesel'],
                          'vehicle_type': ['car', 'car', 'lgv', 'lgv', 'car', 'car', 'car'],
                          'segment': ['large-suv-executive', 'medium', 'n1 class i', 'n1 class i',
                                      'large-suv-executive', 'medium', 'medium'],
                          'cohort': [2004, 2004, 2004, 2004, 2004, 2004, 2004]}
        expected_fleet_result = pd.DataFrame(data=exp_fleet_data)

        exp_charac_data = {'fuel': ['bev', 'phev', 'diesel', 'hydrogen', 'bev', 'petrol'],
                           'avg_mass': [1500.0, 1200.0, 1100.0, 1000.0, 100.0, 100.0],
                           'avg_cc': [50, 1000, 1500, 2000, 3000, 3000],
                           'avg_co2': [100.0, 220.0, 120.0, 150.0, 200.0, 200.0],
                           'vehicle_type': ['car', 'car', 'car', 'car', 'lgv', 'lgv'],
                           'segment': ['large-suv-executive', 'large-suv-executive', 'medium', 'medium', 'n1 class i',
                                       'n1 class i'],
                           'cohort': [2004, 2004, 2004, 2004, 2004, 2004]}
        expected_charac_result = pd.DataFrame(data=exp_charac_data)

        class_index = IndexFleet(False, os.path.join(os.getcwd(), "data_split_tables"))
        class_index.fleet = mock_split_tables
        class_index.fleet_index_year = 2023
        class_index._IndexFleet__split_tables()
        actual_fleet_result = class_index.fleet
        actual_charac_result = class_index.characteristics

        print(actual_fleet_result)
        print(actual_charac_result)
        pd.testing.assert_frame_equal(actual_fleet_result, expected_fleet_result, check_dtype=False, check_like=True)
        pd.testing.assert_frame_equal(actual_charac_result, expected_charac_result, check_dtype=False, check_like=True)