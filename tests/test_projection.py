# -*- coding: utf-8 -*-
"""Tests for the scenario invariant processes"""
# Built-Ins
import os.path
from typing import Any

# Third Party
import pandas as pd
import pytest


# Local Imports
from caf.carbon.projection import Model

# # # CONSTANTS # # #


# # # TESTS # # #
class TestModel:
    def test__predict_fleet_size(
        self, mock_model_class_invariant_obj, mock_model_class_scenario_obj, mock_region_filter
    ):
        exp_data = {
            "vehicle_type": [
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
            ],
            "year": [
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
            ],
            "fleet_forecast": [
                30,
                30.75,
                31.5,
                31.8,
                32.1,
                32.4,
                32.7,
                33,
                33.6,
                34.2,
                34.8,
                35.4,
                36,
                37.2,
                38.4,
                39.6,
                40.8,
                42,
                42.6,
                43.2,
                43.8,
                44.4,
                45,
                48,
                51,
                54,
                57,
                60,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
            ],
        }
        exp_fleet_size = pd.DataFrame(data=exp_data)

        class_model = Model(
            region_filter=mock_region_filter,
            invariant_obj=mock_model_class_invariant_obj,
            scenario_obj=mock_model_class_scenario_obj,
            ev_redistribution=False,
            run_name="testing",
            ev_redistribution_fresh=False,
            years_to_include=[
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
            ],
        )
        class_model._Model__predict_fleet_size()

        actual_result = class_model.scenario.fleet_size

        pd.testing.assert_frame_equal(
            actual_result, exp_fleet_size, check_dtype=False, check_like=True
        )

    def test__create_future_fleet(
        self, mock_region_filter, mock_model_class_scenario_obj, mock_model_class_invariant_obj
    ):
        exp_data = {
            "zone": ["E02000001", "E02000002", "E02000003"],
            "segment": ["medium", "medium", "n1 class i"],
            "vehicle_type": ["car", "car", "lgv"],
            "cohort": [2023, 2023, 2023],
            "fuel": ["bev", "bev", "phev"],
        }
        exp_future_fleet = pd.DataFrame(data=exp_data)

        class_model = Model(
            region_filter=mock_region_filter,
            invariant_obj=mock_model_class_invariant_obj,
            scenario_obj=mock_model_class_scenario_obj,
            ev_redistribution=False,
            run_name="testing",
            ev_redistribution_fresh=False,
            years_to_include=[
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
            ],
        )
        class_model._Model__create_future_fleet()

        actual_result = class_model.scenario.future_fleet

        pd.testing.assert_frame_equal(
            actual_result, exp_future_fleet, check_dtype=False, check_like=True
        )

    def test__predict_sales(
        self,
        mock_region_filter,
        mock_model_class_scenario_obj,
        mock_model_class_invariant_obj,
        mock_predict_sales_result,
    ):

        exp_fleet_sales = mock_predict_sales_result

        class_model = Model(
            region_filter=mock_region_filter,
            invariant_obj=mock_model_class_invariant_obj,
            scenario_obj=mock_model_class_scenario_obj,
            ev_redistribution=False,
            run_name="testing",
            ev_redistribution_fresh=False,
            years_to_include=[
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
            ],
        )
        class_model._Model__predict_sales()

        actual_result_df = class_model.scenario.fleet_sales
        actual_result = actual_result_df.round(4)

        pd.testing.assert_frame_equal(
            actual_result, exp_fleet_sales, check_dtype=False, check_like=True
        )

    # The following also covers function __jump_forward as the function __project_fleet calls it
    def test__project_fleet(
        self, mock_region_filter, mock_model_class_scenario_obj, mock_model_class_invariant_obj
    ):

        exp_data = {
            "zone": [
                "E02000001",
                "E02000002",
                "E02000003",
                "E02000004",
                "E02000001",
                "E02000002",
                "E02000003",
                "E02000004",
                "E02000003",
            ],
            "fuel": ["bev", "phev", "phev", "bev", "bev", "phev", "phev", "bev", "phev"],
            "segment": [
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "n1 class i",
            ],
            "vehicle_type": ["car", "car", "lgv", "lgv", "car", "car", "lgv", "lgv", "lgv"],
            "cohort": [2023, 2023, 2023, 2020, 2023, 2023, 2023, 2020, 2024],
            "tally": [20.000, 10.000, 10.000, 10.000, 22.000, 11.000, 9.800, 9.800, 0.008],
            "year": [2023, 2023, 2023, 2023, 2024, 2024, 2024, 2024, 2024],
        }
        exp_projected_fleet = pd.DataFrame(data=exp_data)

        class_model = Model(
            region_filter=mock_region_filter,
            invariant_obj=mock_model_class_invariant_obj,
            scenario_obj=mock_model_class_scenario_obj,
            ev_redistribution=False,
            run_name="testing",
            ev_redistribution_fresh=False,
            years_to_include=[
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
            ],
        )
        class_model._Model__project_fleet()

        # first 9 lines only as this checks the functions are working correctly without creating the full 1000+ line df-
        # these lines cover process for current year 2024, but in the code it is then repeated for each year up to 2050
        actual = class_model.projected_fleet
        actual_round = actual.round(3)
        actual_result = actual_round.head(9)

        pd.testing.assert_frame_equal(
            actual_result, exp_projected_fleet, check_dtype=False, check_like=True
        )

    def test_fleet_transform(
        self,
        mock_region_filter,
        mock_model_class_scenario_obj,
        mock_model_class_invariant_obj,
        mock_fleet_transform,
    ):
        exp_proj_data = {
            "origin": ["E02000004", "E02000001", "E02000001", "E02000003", "E02000004"],
            "fuel": ["bev", "bev", "phev", "phev", "bev"],
            "segment": ["n1 class i", "medium", "medium", "n1 class i", "n1 class i"],
            "vehicle_type": ["lgv", "car", "car", "lgv", "lgv"],
            "cohort": [2020, 2023, 2023, 2023, 2020],
            "tally": [10, 20, 20, 9.8, 9.8],
            "year": [2023, 2024, 2024, 2029, 2024],
            "cya": ["3", "1", "1", "06_08", "4"],
            "prop_by_fuel_seg_cohort": [1, 0.5, 0.5, 1, 1],
        }
        exp_projected = pd.DataFrame(data=exp_proj_data)
        exp_curve_data = {
            "segment": ["medium", "medium", "n1 class i", "n1 class i"],
            "fuel": ["bev", "phev", "bev", "phev"],
            "vehicle_type": ["car", "car", "lgv", "lgv"],
            "e_cohort": ["a", "a", "b", "b"],
            "tailpipe_gco2": [30, 66, 99, 144],
            "speed_band": ["a", "b", "c", "d"],
        }
        exp_curve = pd.DataFrame(data=exp_curve_data)
        exp_emiss_data = {
            "cohort": [
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
                2012,
            ],
            "vehicle_type": [
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "car",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
                "lgv",
            ],
            "segment": [
                "medium",
                "medium",
                "medium",
                "medium",
                "medium",
                "medium",
                "medium",
                "medium",
                "n1 class i",
                "n1 class i",
                "n1 class i",
                "n1 class i",
                "n1 class i",
                "n1 class i",
                "n1 class i",
                "n1 class i",
            ],
            "fuel": [
                "bev",
                "bev",
                "bev",
                "bev",
                "phev",
                "phev",
                "phev",
                "phev",
                "bev",
                "bev",
                "bev",
                "bev",
                "phev",
                "phev",
                "phev",
                "phev",
            ],
            "year": [
                2010,
                2023,
                2040,
                2045,
                2010,
                2023,
                2040,
                2045,
                2010,
                2023,
                2040,
                2045,
                2010,
                2023,
                2040,
                2045,
            ],
            "grid_gco2": [80, 20, 2, 0.4, 200, 50, 5, 1, 400, 100, 10, 2, 480, 120, 12, 2.4],
        }
        exp_emissions = pd.DataFrame(data=exp_emiss_data)

        class_model = Model(
            region_filter=mock_region_filter,
            invariant_obj=mock_model_class_invariant_obj,
            scenario_obj=mock_model_class_scenario_obj,
            ev_redistribution=False,
            run_name="testing",
            ev_redistribution_fresh=False,
            years_to_include=[
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
            ],
        )

        class_model.projected_fleet = mock_fleet_transform
        class_model.fleet_transform()

        actual_projected = class_model.projected_fleet
        actual_curve = class_model.se_curve
        actual_emissions = class_model.grid_emissions

        pd.testing.assert_frame_equal(
            actual_projected, exp_projected, check_dtype=False, check_like=True
        )
        pd.testing.assert_frame_equal(
            actual_curve, exp_curve, check_dtype=False, check_like=True
        )
        pd.testing.assert_frame_equal(
            actual_emissions, exp_emissions, check_dtype=False, check_like=True
        )

    def test_assign_chunk_emissions(
        self,
        mock_region_filter,
        mock_model_class_scenario_obj,
        mock_model_class_invariant_obj,
        mock_assign_chunk_emissions,
    ):

        exp_data = {
            "origin": ["E02000001", "E02000002", "E02000003", "E02000004"],
            "destination": ["E02000002", "E02000003", "E02000004", "E02000001"],
            "through": ["E02000004", "E02000001", "E02000002", "E02000003"],
            "vehicle_type": ["car", "lgv", "lgv", "lgv"],
            "fuel": ["bev", "bev", "phev", "bev"],
            "tally": [20, 9.8, 9.8, 10],
            "year": [2024, 2024, 2029, 2023],
            "user_class": [1, 1, 1, 1],
            "trip_band": [2, 2, 2, 2],
            "vkm": [2, 16, 12, 16],
            "tailpipe_gco2": [60, 800, 720, 800],
            "grid_gco2": [100, 2000, 144, 16],
        }
        exp_fleet = pd.DataFrame(data=exp_data)

        class_model = Model(
            region_filter=mock_region_filter,
            invariant_obj=mock_model_class_invariant_obj,
            scenario_obj=mock_model_class_scenario_obj,
            ev_redistribution=False,
            run_name="testing",
            ev_redistribution_fresh=False,
            years_to_include=[
                2023,
                2024,
                2025,
                2026,
                2027,
                2028,
                2029,
                2030,
                2031,
                2032,
                2033,
                2034,
                2035,
                2036,
                2037,
                2038,
                2039,
                2040,
                2041,
                2042,
                2043,
                2044,
                2045,
                2046,
                2047,
                2048,
                2049,
                2050,
            ],
        )

        class_model.se_curve = mock_assign_chunk_emissions.se_curve
        class_model.grid_emissions = mock_assign_chunk_emissions.emissions
        actual_fleet = class_model.assign_chunk_emissions(
            mock_assign_chunk_emissions.fleet, mock_assign_chunk_emissions.demand
        )

        pd.testing.assert_frame_equal(
            actual_fleet, exp_fleet, check_dtype=False, check_like=True
        )
