# Built-Ins
from datetime import datetime
from caf.carbon.load_data import OUT_PATH, DEMOGRAPHICS_DATA, TRAVELLER_DATA


# Third Party
import pandas as pd


class Redistribution:
    """Redistribute the vehicle fleet, using a determined relation from socioeconomic factors."""

    def __init__(self, invariant_obj, scenario_obj, projected_fleet, run_fresh):
        """Initialise functions and set class variables.

        Parameters
        ----------
        invariant_obj : class obj
            Includes the baseline fleet and scenario invariant tables.
        scenario_obj : class obj
            Includes scenario tables.
        """
        self.outpath = OUT_PATH
        self.invariant = invariant_obj
        self.scenario = scenario_obj
        self.projected_fleet = projected_fleet.copy()
        self.date = datetime.today().strftime("%Y_%m_%d")
        self.__load_redistributed_data()
        if run_fresh:
            self.__input_weights()
            self.__redistribution_parameters()
        else:
            self.__load_redistributed_data()

        self.__transform_fleet()

    def __load_redistributed_data(self):
        """Load EV weights if already built from data."""
        self.calibration_data = pd.read_csv(
            "{self.outpath}/audit/fleet_weights.csv".format(**locals())
        )

    def __input_weights(self):
        """Build EV weights if necessary."""
        base_pop = pd.read_pickle(DEMOGRAPHICS_DATA, compression="bz2")
        base_pop = base_pop[["MSOA", "tfn_tt", "people"]].rename(columns={"MSOA": "zone"})
        traveller_types = pd.read_csv(TRAVELLER_DATA)
        # deselect traveller types who can not own cars
        deselection_criteria = [["age_str", "under 16"], ["hh_cars", 0]]
        traveller_types["selection"] = 1
        for criterion in deselection_criteria:
            traveller_types.loc[
                (traveller_types[criterion[0]] == criterion[1]), "selection"
            ] = 0
        traveller_types = traveller_types[["tfn_tt", "soc", "selection"]]

        base_pop = pd.merge(base_pop, traveller_types, how="left", on="tfn_tt")
        base_pop = base_pop[base_pop["selection"] == 1]
        base_pop = base_pop.groupby(["zone", "soc"], as_index=False).sum()
        base_pop = base_pop[["zone", "soc", "people"]].pivot(
            values="people", index=["zone"], columns=["soc"]
        )
        base_pop = base_pop.rename(columns={1: "soc 1", 2: "soc 2", 3: "soc 3"})
        base_pop["total pop"] = base_pop["soc 1"] + base_pop["soc 2"] + base_pop["soc 3"]

        fleet_df = self.invariant.index_fleet.fleet.copy()
        fleet_df = fleet_df[fleet_df["vehicle_type"] != "hgv"]
        fleet_df.loc[(fleet_df["fuel"].isin(["bev", "phev"])), "fuel"] = "ev"
        fleet_df.loc[
            (fleet_df["fuel"].isin(["diesel", "petrol", "petrol hybrid"])), "fuel"
        ] = "ice"
        fleet_df = (
            fleet_df[["fuel", "zone", "tally"]].groupby(["fuel", "zone"], as_index=False).sum()
        )
        fleet_df = fleet_df.pivot(values="tally", index=["zone"], columns=["fuel"])
        fleet_df["total vehicles"] = fleet_df["ice"] + fleet_df["ev"]
        fleet_df["ev"] = fleet_df["ev"] / fleet_df["total vehicles"]
        base_pop["soc 1"] = base_pop["soc 1"] / base_pop["total pop"]
        base_pop["soc 2"] = base_pop["soc 2"] / base_pop["total pop"]
        base_pop["soc 3"] = base_pop["soc 3"] / base_pop["total pop"]

        calibration_data = pd.merge(fleet_df, base_pop, how="left", on="zone")
        self.calibration_data = calibration_data

    def __redistribution_parameters(self):
        """Transform fleet to reflect socioeconomic EV weights."""
        # step_minimisation
        step_size, sig_fig = 0.0001, 5
        self.calibration_data[["derived ev", "product iterative", "product initial"]] = 0, 0, 0
        # choose starting parameters
        # note to self, may require functional analysis here
        parameters = pd.DataFrame(
            {
                "par": ["soc 1", "soc 2", "soc 3"],
                "par val A": [-1, -1, -1],
                "step 1 A": [0, 0, 0],
                "step 2 A": [0, 0, 0],
                "step 3 A": [0, 0, 0],
                "par val B": [-1, -1, -1],
                "step 1 B": [0, 0, 0],
                "step 2 B": [0, 0, 0],
                "step 3 B": [0, 0, 0],
                "par val C": [-1, -1, -1],
                "step 1 C": [0, 0, 0],
                "step 2 C": [0, 0, 0],
                "step 3 C": [0, 0, 0],
            }
        )
        self.func(parameters)
        self.calibration_data["product initial"] = self.calibration_data["product iterative"]
        # initial parameter selection, compare minimisation by cycling through options, put into square column initial
        initial_parameters = parameters.copy()
        for par_val in ["par val A", "par val B", "par val C"]:
            for par in [0, 1, 2]:
                for i in [1, 0.75, 0.5, 0.25, 0.1, 0.05, -0.05, -0.1, -0.25, -0.5, -0.75, -1]:
                    parameters.loc[(parameters.index == par), par_val] = i
                    self.func(parameters)
                    if (
                        self.calibration_data["product iterative"].sum()
                        < self.calibration_data["product initial"].sum()
                    ):
                        initial_parameters.loc[(initial_parameters.index == par), par_val] = (
                            parameters.loc[par][par_val]
                        )
                        self.calibration_data["product initial"] = self.calibration_data[
                            "product iterative"
                        ]
        parameters = initial_parameters

        # func inserts parameters into equation with calibration data
        # the func column is subtracted from the y column and squared as new columns
        # square column iteration and square column initial are summed, lowest wins
        # cap As at 1 and reset those that max out?
        print("Running fresh, beginning calculation of regional EV weighting function \n")
        print("Completion progress at: \n0 percent")
        completion_progress = 0
        while (
            int(
                parameters["step 3 A"].sum()
                + parameters["step 3 B"].sum()
                + parameters["step 3 C"].sum()
            )
            != 9
        ):
            for par in [0, 1, 2]:
                for par_val in ["A", "B", "C"]:
                    if parameters.loc[par]["step 1 " + par_val] == 0:
                        # try step up
                        parameters.loc[(parameters.index == par), "par val " + par_val] = (
                            parameters.loc[par]["par val " + par_val] - (100 * step_size)
                        )
                        self.func(parameters)
                        if (
                            self.calibration_data["product iterative"].sum()
                            < self.calibration_data["product initial"].sum()
                        ):
                            self.calibration_data["product initial"] = self.calibration_data[
                                "product iterative"
                            ]
                        else:
                            # try step down
                            parameters.loc[(parameters.index == par), "par val " + par_val] = (
                                parameters.loc[par]["par val " + par_val]
                                + (2 * 100 * step_size)
                            )
                            self.func(parameters)
                            if (
                                self.calibration_data["product iterative"].sum()
                                < self.calibration_data["product initial"].sum()
                            ):
                                self.calibration_data["product initial"] = (
                                    self.calibration_data["product iterative"]
                                )
                            else:
                                # move to next smallest resolution
                                parameters.loc[
                                    (parameters.index == par), "step 1 " + par_val
                                ] = 1
                                completion_progress = completion_progress + 1
                                update = completion_progress * 100 / 27
                                print("%1.1f percent" % update)

                    elif parameters.loc[par]["step 2 " + par_val] == 0:
                        # try step up
                        parameters.loc[(parameters.index == par), "par val " + par_val] = (
                            parameters.loc[par]["par val " + par_val] - (10 * step_size)
                        )
                        self.func(parameters)
                        if (
                            self.calibration_data["product iterative"].sum()
                            < self.calibration_data["product initial"].sum()
                        ):
                            self.calibration_data["product initial"] = self.calibration_data[
                                "product iterative"
                            ]
                        else:
                            # try step down
                            parameters.loc[(parameters.index == par), "par val " + par_val] = (
                                parameters.loc[par]["par val " + par_val]
                                + (2 * 10 * step_size)
                            )
                            self.func(parameters)
                            if (
                                self.calibration_data["product iterative"].sum()
                                <= self.calibration_data["product initial"].sum()
                            ):
                                self.calibration_data["product initial"] = (
                                    self.calibration_data["product iterative"]
                                )
                            else:
                                # move to next smallest resolution
                                parameters.loc[
                                    (parameters.index == par), "step 2 " + par_val
                                ] = 1
                                completion_progress = completion_progress + 1
                                update = completion_progress * 100 / 27
                                print("%1.1f percent" % update)

                    elif parameters.loc[par]["step 3 " + par_val] == 0:
                        # try step up
                        parameters.loc[(parameters.index == par), "par val " + par_val] = (
                            parameters.loc[par]["par val " + par_val] - step_size
                        )
                        self.func(parameters)
                        if (
                            self.calibration_data["product iterative"].sum()
                            < self.calibration_data["product initial"].sum()
                        ):
                            self.calibration_data["product initial"] = self.calibration_data[
                                "product iterative"
                            ]
                        else:
                            # try step down
                            parameters.loc[(parameters.index == par), "par val " + par_val] = (
                                parameters.loc[par]["par val " + par_val] + (2 * step_size)
                            )
                            self.func(parameters)
                            if (
                                self.calibration_data["product iterative"].sum()
                                < self.calibration_data["product initial"].sum()
                            ):
                                self.calibration_data["product initial"] = (
                                    self.calibration_data["product iterative"]
                                )
                            else:
                                # move to next smallest resolution
                                parameters.loc[
                                    (parameters.index == par), "step 3 " + par_val
                                ] = 1
                                completion_progress = completion_progress + 1
                                update = completion_progress * 100 / 27
                                print("%1.1f percent" % update)

        print("Function parameters determined")
        self.calibration_data = self.calibration_data.reset_index()
        self.calibration_data = self.calibration_data[["zone", "derived ev"]]
        self.calibration_data.to_csv(f"{self.outpath}audit/fleet_weights.csv")

    def __transform_fleet(self):
        """Transform fleet to reflect socioeconomic EV weights."""
        print("Reassigning EV Fleet based on new distribution.")
        # create real fleet ev difference
        ev_fleet = self.projected_fleet.copy()
        ev_fleet = ev_fleet[ev_fleet["vehicle_type"] != "hgv"]
        ev_fleet.loc[(ev_fleet["fuel"].isin(["bev", "phev"])), "fuel"] = "ev"
        ev_fleet.loc[
            (ev_fleet["fuel"].isin(["diesel", "petrol", "petrol hybrid"])), "fuel"
        ] = "ice"
        ev_fleet = (
            ev_fleet[["fuel", "zone", "tally", "year"]]
            .groupby(["fuel", "zone", "year"], as_index=False)
            .sum()
        )
        ev_fleet = ev_fleet.pivot(values="tally", index=["zone", "year"], columns=["fuel"])
        ev_fleet["total vehicles"] = ev_fleet["ice"] + ev_fleet["ev"]
        ev_fleet["ev"] = ev_fleet["ev"] / ev_fleet["total vehicles"]
        ev_fleet = ev_fleet.reset_index()
        ev_fleet = ev_fleet[["zone", "year", "ev", "total vehicles"]]

        # merge ev fleet and derived ev fleet to projected fleet
        self.calibration_data.loc[self.calibration_data["derived ev"] < 0, "derived ev"] = 0
        self.calibration_data.loc[self.calibration_data["derived ev"] > 1, "derived ev"] = 1
        self.projected_fleet = self.projected_fleet.merge(
            self.calibration_data, how="left", on="zone"
        )
        self.projected_fleet = self.projected_fleet.merge(
            ev_fleet, how="left", on=["zone", "year"]
        )

        # siphon off ev fleet to redistribute, redistribute by category
        # redistribute fleet to those on other side of the scale
        self.projected_fleet["difference"] = (
            self.projected_fleet["derived ev"] - self.projected_fleet["ev"]
        )
        self.projected_fleet.loc[self.projected_fleet["difference"] > 0, "dir"] = 1
        self.projected_fleet.loc[self.projected_fleet["difference"] < 0, "dir"] = -1
        self.projected_fleet.loc[self.projected_fleet["difference"] < 0, "difference"] = (
            -1 * self.projected_fleet["difference"]
        )

        self.projected_fleet["fleet to reassign"] = 0
        # fleet to reassign = dif * total vehicles
        # for each row, proportion of total EV tally, non EV tally. Then add x to ice and subtract x from EV
        self.projected_fleet["fleet to reassign"] = (
            self.projected_fleet["total vehicles"] * self.projected_fleet["difference"]
        )
        self.projected_fleet.loc[
            self.projected_fleet["cohort"] > 2030, "fleet to reassign"
        ] = 0
        self.projected_fleet.loc[
            self.projected_fleet["vehicle_type"] == "hgv", "fleet to reassign"
        ] = 0
        # tally = tally plus the corrective difference needed for the zone (positive for EVs, negative for ICEs)
        # distributed proportionally across the segmentation of vehicles in that zone
        self.projected_fleet.loc[
            self.projected_fleet["fuel"].isin(["bev", "phev"]), "tally"
        ] = self.projected_fleet["tally"] + (
            self.projected_fleet["dir"]
            * self.projected_fleet["fleet to reassign"]
            * (
                self.projected_fleet["tally"]
                / self.projected_fleet["total vehicles"]
                * self.projected_fleet["ev"]
            )
        )
        self.projected_fleet.loc[
            self.projected_fleet["fuel"].isin(["petrol", "diesel", "petrol hybrid"]), "tally"
        ] = self.projected_fleet["tally"] - (
            self.projected_fleet["dir"]
            * self.projected_fleet["fleet to reassign"]
            * (
                self.projected_fleet["tally"]
                / self.projected_fleet["total vehicles"]
                * (1 - self.projected_fleet["ev"])
            )
        )

        self.projected_fleet = self.projected_fleet[
            [
                "fuel",
                "segment",
                "zone",
                "tally",
                "vehicle_type",
                "cohort",
                "year",
                "dir",
                "difference",
            ]
        ]
        self.projected_fleet.loc[self.projected_fleet["tally"] < 0, "tally"] = 0
        print("Reassignment of fleet complete.")

    def func(self, parameters):
        """test a given iteration of the function"""

        self.calibration_data["first order"] = (
            (parameters.iloc[0]["par val A"] * self.calibration_data["soc 1"])
            + (parameters.iloc[1]["par val A"] * self.calibration_data["soc 2"])
            + (parameters.iloc[2]["par val A"] * self.calibration_data["soc 3"])
        )
        self.calibration_data["second order"] = (
            (
                parameters.iloc[0]["par val B"]
                * self.calibration_data["soc 1"]
                * self.calibration_data["soc 1"]
            )
            + (
                parameters.iloc[1]["par val B"]
                * self.calibration_data["soc 2"]
                * self.calibration_data["soc 2"]
            )
            + (
                parameters.iloc[2]["par val B"]
                * self.calibration_data["soc 3"]
                * self.calibration_data["soc 3"]
            )
        )
        self.calibration_data["third order"] = (
            (
                parameters.iloc[0]["par val C"]
                * self.calibration_data["soc 1"]
                * self.calibration_data["soc 1"]
                * self.calibration_data["soc 1"]
            )
            + (
                parameters.iloc[1]["par val C"]
                * self.calibration_data["soc 2"]
                * self.calibration_data["soc 2"]
                * self.calibration_data["soc 2"]
            )
            + (
                parameters.iloc[2]["par val C"]
                * self.calibration_data["soc 3"]
                * self.calibration_data["soc 3"]
                * self.calibration_data["soc 3"]
            )
        )

        self.calibration_data["derived ev"] = (
            self.calibration_data["first order"]
            + self.calibration_data["second order"]
            + self.calibration_data["third order"]
        )
        self.calibration_data["product iterative"] = (
            self.calibration_data["derived ev"] - self.calibration_data["ev"]
        ) * (self.calibration_data["derived ev"] - self.calibration_data["ev"])
