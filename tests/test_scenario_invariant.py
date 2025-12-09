# -*- coding: utf-8 -*-
"""Tests for the scenario invariant processes"""
# Built-Ins
import os.path
from typing import Any

# Third Party
import pandas as pd
import numpy as np
import pytest


# Local Imports
from caf.carbon.scenario_invariant import IndexFleet, CurveFitting

# # # CONSTANTS # # #


# # # TESTS # # #
class TestIndexFleet:
    def test__basic_clean(self, mock_basic_clean):
        exp_data = {
            "zone": [
                "E02002052",
                "E02002325",
                "E02002325",
                "E02002623",
                "E02002623",
                "E02002623",
                "E02002627",
                "E02002627",
                "S02001261",
                "S02001261",
            ],
            "tally": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "year": [2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023],
            "fuel": [
                "bev",
                "diesel",
                "diesel",
                "bev",
                "petrol",
                "phev",
                "hydrogen",
                "hydrogen",
                "diesel",
                "diesel",
            ],
            "avg_mass": [
                1500.0,
                2000.0,
                2000.0,
                0.0,
                0.0,
                1200.0,
                1000.0,
                1000.0,
                1000.0,
                1000.0,
            ],
            "avg_cc": [50, 1400, 1400, 3000, 3000, 1000, 2000, 2000, 1400, 1400],
            "avg_co2": [100.0, 140.0, 140.0, 200.0, 200.0, 220.0, 150.0, 150.0, 110.0, 110.0],
            "vehicle_type": [
                "car",
                "car",
                "car",
                "lgv",
                "lgv",
                "car",
                "car",
                "car",
                "car",
                "car",
            ],
            "cya": [19, 19, 19, 19, 19, 19, 19, 19, 19, 19],
            "segment": [
                "large-suv-executive",
                "large-suv-executive",
                "medium",
                "n1 class i",
                "n1 class i",
                "large-suv-executive",
                "large-suv-executive",
                "medium",
                "large-suv-executive",
                "medium",
            ],
            "split": [1, 0.375, 0.625, 1, 1, 1, 0.375, 0.625, 0.375, 0.625],
        }
        expected_result = pd.DataFrame(data=exp_data)

        class_index = IndexFleet(False, 2023, os.path.join(os.getcwd(), "data_basic_clean"))
        class_index.fleet_archive = mock_basic_clean
        class_index.fleet_index_year = 2023
        class_index._IndexFleet__basic_clean()

        actual_result = class_index.fleet_archive

        pd.testing.assert_frame_equal(
            actual_result, expected_result, check_dtype=False, check_like=True
        )

    def test__advanced_clean(self, mock_advance_clean):
        exp_data = {
            "zone": [
                "E02002052",
                "E02002052",
                "E02002627",
                "E02002623",
                "E02002623",
                "E02002325",
                "S02001261",
                "E02002623",
            ],
            "tally": [1, 1, 1, 1, 1, 1, 1, 1],
            "year": [2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023],
            "fuel": ["bev", "bev", "hydrogen", "phev", "petrol", "diesel", "diesel", "petrol"],
            "avg_mass": [1500.0, 750.0, 1000.0, 1200.0, 100.0, 1000.0, 1500.0, 100.0],
            "avg_cc": [25, 50, 2000, 1000, 3000, 1400, 1400, 3000],
            "avg_co2": [100.0, 50.0, 150.0, 220.0, 200.0, 140.0, 110.0, 200.0],
            "vehicle_type": ["car", "car", "car", "car", "lgv", "car", "car", "lgv"],
            "cya": [19, 19, 19, 19, 19, 19, 19, 19],
            "segment": [
                "large-suv-executive",
                "large-suv-executive",
                "medium",
                "large-suv-executive",
                "n1 class i",
                "large-suv-executive",
                "large-suv-executive",
                "n1 class i",
            ],
        }
        expected_result = pd.DataFrame(data=exp_data)

        class_index = IndexFleet(False, 2023, os.path.join(os.getcwd(), "data_advance_clean"))
        class_index.fleet_archive = mock_advance_clean
        class_index.fleet_index_year = 2023
        class_index._IndexFleet__advanced_clean()

        actual_result = class_index.fleet

        pd.testing.assert_frame_equal(
            actual_result, expected_result, check_dtype=False, check_like=True
        )

    def test__split_tables(self, mock_split_tables):
        exp_fleet_data = {
            "zone": [
                "E02002052",
                "E02002325",
                "E02002623",
                "E02002623",
                "E02002623",
                "E02002627",
                "S02001261",
            ],
            "tally": [1, 1, 1, 1, 1, 1, 2],
            "year": [2023, 2023, 2023, 2023, 2023, 2023, 2023],
            "fuel": ["bev", "diesel", "bev", "petrol", "phev", "hydrogen", "diesel"],
            "vehicle_type": ["car", "car", "lgv", "lgv", "car", "car", "car"],
            "segment": [
                "large-suv-executive",
                "medium",
                "n1 class i",
                "n1 class i",
                "large-suv-executive",
                "medium",
                "medium",
            ],
            "cohort": [2004, 2004, 2004, 2004, 2004, 2004, 2004],
        }
        expected_fleet_result = pd.DataFrame(data=exp_fleet_data)

        exp_charac_data = {
            "fuel": ["bev", "phev", "diesel", "hydrogen", "bev", "petrol"],
            "avg_mass": [1500.0, 1200.0, 1100.0, 1000.0, 100.0, 100.0],
            "avg_cc": [50, 1000, 1500, 2000, 3000, 3000],
            "avg_co2": [100.0, 220.0, 120.0, 150.0, 200.0, 200.0],
            "vehicle_type": ["car", "car", "car", "car", "lgv", "lgv"],
            "segment": [
                "large-suv-executive",
                "large-suv-executive",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
            ],
            "cohort": [2004, 2004, 2004, 2004, 2004, 2004],
        }
        expected_charac_result = pd.DataFrame(data=exp_charac_data)

        class_index = IndexFleet(False, 2023, os.path.join(os.getcwd(), "data_split_tables"))
        class_index.fleet = mock_split_tables
        class_index._IndexFleet__split_tables()

        actual_fleet_result = class_index.fleet
        actual_charac_result = class_index.characteristics

        pd.testing.assert_frame_equal(
            actual_fleet_result, expected_fleet_result, check_dtype=False, check_like=True
        )
        pd.testing.assert_frame_equal(
            actual_charac_result, expected_charac_result, check_dtype=False, check_like=True
        )


class TestCurveFitting:
    def test_generate_scrappage(self, mock_curvefitting_invariant_obj):
        exp_data = {
            "cya": [0, 0, 1, 1, 2, 3, 2, 3, 4, 4],
            "vehicle_type": [
                "car",
                "lgv",
                "lgv",
                "car",
                "car",
                "car",
                "lgv",
                "lgv",
                "lgv",
                "car",
            ],
            "survival": [1, 1, 0.5, 0.4472, 0.4472, 0.4472, 0.4472, 0.4472, 0.4472, 4.4721],
        }
        exp_scrappage = pd.DataFrame(data=exp_data)

        class_curve = CurveFitting()
        actual = class_curve.generate_scrappage(invariant_obj=mock_curvefitting_invariant_obj)
        actual_scrappage = actual.round(4)

        pd.testing.assert_frame_equal(
            exp_scrappage, actual_scrappage, check_dtype=False, check_like=True
        )

    def test_characteristics_to_rwc(self, mock_curvefitting_invariant_obj):
        exp_data = {
            "cohort": [2010, 2010, 2010, 2010],
            "vehicle_type": ["car", "car", "lgv", "lgv"],
            "segment": ["medium", "medium", "n1 class i", "n1 class i"],
            "fuel": ["clean", "petrol", "diesel", "petrol hybrid"],
            "fuel_type": ["bev", "petrol", "diesel", "phev"],
            "mj_co2": [0, 90, 100, 70],
            "rw_multiplier": [1, 131.5955, 147.5293, 0.5],
        }
        exp_rw = pd.DataFrame(data=exp_data)

        class_curve = CurveFitting()
        actual = class_curve.characteristics_to_rwc(
            invariant_obj=mock_curvefitting_invariant_obj
        )
        actual_rw = actual.round(4)

        pd.testing.assert_frame_equal(exp_rw, actual_rw, check_dtype=False, check_like=True)

    def test_fit_se_curves(self, mock_curvefitting_invariant_obj):
        exp_data = {
            "cohort": [2010, 2010, 2010, 2010],
            "fuel": ["bev", "petrol", "diesel", "phev"],
            "vehicle_type": ["car", "car", "lgv", "lgv"],
            "segment": ["medium", "medium", "n1 class i", "n1 class i"],
            "formula": [
                "0",
                "(0.2*speed**2 + 0.05*speed + 20.0 + 0.1/speed) / (-0.1*speed**2 + 0.05*speed + 1.0) *120.0*90",
                "(0.2*speed**2 + -0.2*speed + 10.0 + 0.2/speed) / (-0.1*speed**2 + 0.1*speed + 2.0) *150.0*100",
                "(0.15*speed**2 + -5.0*speed + 15.0 + 0.1/speed) / (-0.2*speed**2 + 0.2*speed + 3.0) *0.5*70",
            ],
            "min_speed": [0, 30.0, 25.0, 20.0],
            "max_speed": [0, 120.0, 100.0, 90.0],
        }
        exp_se_curve = pd.DataFrame(data=exp_data)
        exp_se_curve.replace({0: np.nan}, inplace=True)

        class_curve = CurveFitting()
        actual = class_curve.fit_se_curves(invariant_obj=mock_curvefitting_invariant_obj)

        pd.testing.assert_frame_equal(exp_se_curve, actual, check_dtype=False, check_like=True)

    def test_calculate_speed_band_emissions(self, mock_curvefitting_invariant_obj):
        exp_data = {
            "segment": [
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
            ],
            "fuel": [
                "bev",
                "petrol",
                "diesel",
                "phev",
                "bev",
                "petrol",
                "diesel",
                "phev",
                "bev",
                "petrol",
                "diesel",
                "phev",
                "bev",
                "petrol",
                "diesel",
                "phev",
                "bev",
                "petrol",
                "diesel",
                "phev",
                "bev",
                "petrol",
                "diesel",
                "phev",
            ],
            "e_cohort": [
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
                2010,
            ],
            "vehicle_type": [
                "car",
                "car",
                "lgv",
                "lgv",
                "car",
                "car",
                "lgv",
                "lgv",
                "car",
                "car",
                "lgv",
                "lgv",
                "car",
                "car",
                "lgv",
                "lgv",
                "car",
                "car",
                "lgv",
                "lgv",
                "car",
                "car",
                "lgv",
                "lgv",
            ],
            "speed_band": [
                "10_30",
                "10_30",
                "10_30",
                "10_30",
                "30_50",
                "30_50",
                "30_50",
                "30_50",
                "50_70",
                "50_70",
                "50_70",
                "50_70",
                "70_90",
                "70_90",
                "70_90",
                "70_90",
                "90_110",
                "90_110",
                "90_110",
                "90_110",
                "110_130",
                "110_130",
                "110_130",
                "110_130",
            ],
            "gco2/km": [
                0,
                10e20,
                10e20,
                11.98,
                0,
                -23526.29,
                -31364.12,
                -6.23,
                0,
                -22540.50,
                -30596.73,
                -12.66,
                0,
                -22178.29,
                -30333.39,
                -15.96,
                0,
                -22002.02,
                -30212.58,
                10e20,
                0,
                -21901.47,
                10e20,
                10e20,
            ],
        }
        exp_se_calcs = pd.DataFrame(data=exp_data)

        class_curve = CurveFitting()
        actual_df = class_curve.calculate_speed_band_emissions(
            invariant_obj=mock_curvefitting_invariant_obj
        )
        actual = actual_df.round(2)

        pd.testing.assert_frame_equal(exp_se_calcs, actual, check_dtype=False, check_like=True)
