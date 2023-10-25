# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 12:19:47 2022

@author: Invincible
"""

# to do s curve fitting, year 2020, alternative variables
# optimisation of traveller type demographics

import pandas as pd
import math
import numpy

# fitting function


def func(master, selection, scale_reduction, j, calc):
    f_o, s_o, t_o = 0, 0, 0
    length = int(len(selection)/3)
    if calc:
        for i in range(length):
            f_o = f_o + (selection.loc[i]["mods"]*master.loc[j][selection.loc[i]["type"]])/scale_reduction[0]
            s_o = s_o + (selection.loc[i+length]["mods"]*math.pow(master.loc[j][selection.loc[i]["type"]], 2))\
                / (scale_reduction[1]*2)
            t_o = t_o + (selection.loc[i+2*length]["mods"]*math.pow(master.loc[j][selection.loc[i]["type"]], 3))\
                / (scale_reduction[2]*6)
    else:
        for i in range(length):
            f_o = f_o + (selection.loc[i]["mods"]*master[selection.loc[i]["type"]])/scale_reduction[0]
            s_o = s_o + (selection.loc[i+length]["mods"]*master[selection.loc[i]["type"]]
                         * master[selection.loc[i]["type"]])/(scale_reduction[1]*2)
            t_o = t_o + (selection.loc[i+2*length]["mods"]*master[selection.loc[i]["type"]]
                         * master[selection.loc[i]["type"]]*master[selection.loc[i]["type"]])/(scale_reduction[2]*6)
    return f_o + s_o + t_o

# scale normalisation


def scale_reduction_constants(dataframe, selection):
    scale_const = ["f_o", "s_o", "t_o"]
    temp = dataframe.copy()
    length = int(len(selection)/3)
    temp["total"] = 0
    temp["total squared"] = 0
    temp["total cubed"] = 0
    for i in range(length):
        temp["total"] = temp["total"] + temp[selection.loc[i]["type"]]
        temp["total squared"] = temp["total squared"] + temp[selection.loc[i]["type"]]*temp[selection.loc[i]["type"]]
        temp["total cubed"] = temp["total cubed"] + temp[selection.loc[i]["type"]]*temp[selection.loc[i]["type"]]\
            * temp[selection.loc[i]["type"]]
    scale_const[0] = temp["total"].sum()/len(temp)
    scale_const[1] = temp["total squared"].sum() / len(temp)
    scale_const[2] = temp["total cubed"].sum() / len(temp)
    return scale_const

# square difference


def scale(f, y):
    return (f - y) * (f - y)


# year duration sum

def year_total(selection, master, scale_reductions):
    total = 0
    for i in range(len(master)):
        f_step = func(master, selection, scale_reductions, i, calc=True)
        total = total + scale(f_step, master.loc[i]["proportion"])
    return total


# minimisation function


def minimisation(selection, year, master, scale_reductions):
    # step_minimisation
    step_size = global_step_size
    scale_change = year_total(selection, master, scale_reductions)
    # match step  size sig fig
    sig_fig = 5
    selection_copy = selection.copy()
    initial_data = numpy.random.uniform(-0.1, 0.1, len(selection_copy))
    selection_copy["mods"] = initial_data
    print("###")
    print("running minimisation for %s" % year)
    print("initial parameters  :  ", selection_copy["mods"])
    print("###")
    while int(selection_copy["minima"].sum()) != len(selection):
        for i in range(len(selection)):
            if selection_copy.loc[i]["minima scale 3"] == 0:
                if selection_copy.loc[i]["minima scale 3"] == 0:
                    scale_change = year_total(selection_copy, master, scale_reductions)
                    selection_copy.loc[(selection_copy.index == i), "mods"] = selection_copy.loc[i]["mods"] \
                        + 100*step_size
                    proposed_scale_change = year_total(selection_copy, master, scale_reductions)
                    if proposed_scale_change > scale_change:
                        selection_copy.loc[(selection_copy.index == i), "mods"] = selection_copy.loc[i]["mods"] \
                                                                                  - 200*step_size
                        proposed_scale_change = year_total(selection_copy, master, scale_reductions)
                        if proposed_scale_change > scale_change:
                            selection_copy.loc[(selection_copy.index == i), "mods"] \
                                = round(selection_copy.loc[i]["mods"] + 100*step_size, sig_fig)
                            print("reducing %i to second order" % i)
                            selection_copy.loc[(selection_copy.index == i), "minima scale 3"] = 1
                        else:
                            scale_change = proposed_scale_change
                    else:
                        scale_change = proposed_scale_change
            elif selection_copy.loc[i]["minima scale 2"] == 0:
                if selection_copy.loc[i]["minima scale 2"] == 0:
                    scale_change = year_total(selection_copy, master, scale_reductions)
                    selection_copy.loc[(selection_copy.index == i), "mods"] = selection_copy.loc[i]["mods"] \
                        + 10*step_size
                    proposed_scale_change = year_total(selection_copy, master, scale_reductions)
                    if proposed_scale_change > scale_change:
                        selection_copy.loc[(selection_copy.index == i), "mods"] = selection_copy.loc[i]["mods"] \
                                                                                  - 20*step_size
                        proposed_scale_change = year_total(selection_copy, master, scale_reductions)
                        if proposed_scale_change > scale_change:
                            selection_copy.loc[(selection_copy.index == i), "mods"] \
                                = round(selection_copy.loc[i]["mods"] + 10*step_size, sig_fig)
                            print("reducing %i to first order" % i)
                            selection_copy.loc[(selection_copy.index == i), "minima scale 2"] = 1
                        else:
                            scale_change = proposed_scale_change
                    else:
                        scale_change = proposed_scale_change
            else:
                if selection_copy.loc[i]["minima"] == 0:
                    scale_change = year_total(selection_copy, master, scale_reductions)
                    selection_copy.loc[(selection_copy.index == i), "mods"] = selection_copy.loc[i]["mods"] + step_size
                    proposed_scale_change = year_total(selection_copy, master, scale_reductions)
                    if proposed_scale_change > scale_change:
                        selection_copy.loc[(selection_copy.index == i), "mods"] = selection_copy.loc[i]["mods"] \
                                                                                  - 2*step_size
                        proposed_scale_change = year_total(selection_copy, master, scale_reductions)
                        if proposed_scale_change > scale_change:
                            selection_copy.loc[(selection_copy.index == i), "mods"]\
                                = round(selection_copy.loc[i]["mods"] + step_size, sig_fig)
                            print("%i minimised!" % i)
                            selection_copy.loc[(selection_copy.index == i), "minima"] = 1
                        else:
                            scale_change = proposed_scale_change
                    else:
                        scale_change = proposed_scale_change
            if i == 0:  # change to above mod count to null
                print("###")
                if (selection_copy["mods"] > 1.1).any():
                    print("large value detected, ending associated thread")
                    selection_copy.loc[(selection_copy["mods"] > 1.1), "minima"] = 1
                    selection_copy.loc[(selection_copy["mods"] > 1.1), "minima scale 2"] = 1
                    selection_copy.loc[(selection_copy["mods"] > 1.1), "minima scale 3"] = 1
                    selection_copy.loc[(selection_copy["mods"] > 1.1), "mods"] = 1
                if (selection_copy["mods"] < -1.1).any():
                    print("large negative value detected, ending associated thread")
                    selection_copy.loc[(selection_copy["mods"] < -1.1), "minima"] = 1
                    selection_copy.loc[(selection_copy["mods"] < -1.1), "minima scale 2"] = 1
                    selection_copy.loc[(selection_copy["mods"] < -1.1), "minima scale 3"] = 1
                    selection_copy.loc[(selection_copy["mods"] < -1.1), "mods"] = -1
                print(selection_copy["minima"].sum(), " minimised of ", len(selection_copy["minima"]))
                print(selection_copy["minima scale 3"].sum() - selection_copy["minima"].sum(), " at 2nd order of ",
                      len(selection_copy["minima"]))
                print(len(selection_copy["minima"]) - selection_copy["minima scale 3"].sum(), " at 3rd order of ",
                      len(selection_copy["minima"]))
                print("###")
                # print(selection_copy["mods"], scale_change)
    print("final parameters  :  ", selection_copy["mods"])
    return selection_copy


# 'weight fading term'


def future_pop_read(scenario, msoa_to_noham, mods, scale, selection):
    scenario_values = pd.read_pickle("output_7_resi_gb_msoa_tfn_tt_2018_pop.pbz2", compression="bz2").rename(
        columns={"people": "2018"})
    scenario_values = scenario_values.merge(selection, how="left", on="tfn_tt").dropna()
    scenario_values = scenario_values.groupby(["MSOA", "type"]).sum().reset_index()
    scenario_values = scenario_values.merge(msoa_to_noham.rename(columns={"msoa11cd":"MSOA"}), how="outer", on="MSOA")
    scenario_values["2018"] = scenario_values["2018"] * scenario_values["overlap_msoa_split_factor"]
    scenario_values = scenario_values[["nohamZoneID", "type", "2018"]]
    scenario_values = scenario_values.groupby(["nohamZoneID", "type"]).sum().reset_index()
    scenario_values = scenario_values.pivot(index="nohamZoneID", columns="type", values="2018")
    scenario_values["generated 2018"] = func(scenario_values, mods, scale, 0, calc=False)
    scenario_values = scenario_values[["generated 2018"]]
    scenario_values["generated 2018"] = scenario_values["generated 2018"] * descaling(2018)

    for i in ["2020", "2025", "2030", "2035", "2040", "2045", "2050"]:
        temp = pd.read_pickle("NorMITs pop/%s/land_use_%s_emp.pbz2" % (scenario, i), compression="bz2"
                              ).rename(columns={"msoa_zone_id": "MSOA"})
        temp = temp.groupby(["MSOA", "soc"]).sum().reset_index()
        temp = temp.merge(msoa_to_noham.rename(columns={"msoa11cd": "MSOA"}), how="outer", on="MSOA")
        temp[i] = temp[i] * temp["overlap_msoa_split_factor"]
        temp = temp[["nohamZoneID", "soc", i]]
        temp = temp.groupby(["nohamZoneID", "soc"]).sum().reset_index()
        temp = temp.pivot(index="nohamZoneID", columns="soc", values=i)
        temp = temp[["1", "2", "3"]].rename(columns={"1": "soc1", "2": "soc2", "3": "soc3"})
        temp["generated %s" % i] = func(temp, mods, scale, 0, calc=False)
        temp = temp[["generated %s" % i]]
        temp["generated %s" % i] = temp["generated %s" % i] * descaling(float(i))
        scenario_values = scenario_values.merge(temp, how="left", left_index=True, right_index=True)

    scenario_values.to_csv("EV_weights_%s.csv" % scenario)


def descaling(year_final):
    multiplier = 1 - (1 / (1 + math.exp(-0.5 * (year_final - 2027))))
    return multiplier


def main():

    # preprocessing #
    
    pops_18_msoa = pd.read_pickle("output_7_resi_gb_msoa_tfn_tt_2018_pop.pbz2", compression="bz2")
    pops_19_msoa = pd.read_pickle("output_7_resi_gb_msoa_tfn_tt_2019_pop.pbz2", compression="bz2")
    selection_choices = pd.read_csv("nts_trav_types_soc.csv")[["tfn_tt", "selection", "type"]]
    selection = selection_choices.copy()
    msoa_la = pd.read_csv("MSOA_LAD_lookup.csv").rename(columns={"MSOA11CD": "MSOA", "TAG_LAD": "LAD Code"})
    historic_fleet = pd.read_csv("Historic LA fleet.csv")
    msoa_to_noham = pd.read_csv("noham_msoa_emp_weighted_lookup.csv").rename(columns={"msoa_zone_id": "msoa11cd"})
    
    pops_18_msoa = pd.merge(pops_18_msoa, selection, how="left", on="tfn_tt")
    pops_18_msoa = pops_18_msoa[pops_18_msoa["selection"] == 1]
    pops_18_msoa = pops_18_msoa.merge(msoa_la, how="left", on="MSOA")
    pops18 = pops_18_msoa[["people", "type",  "LAD Code"]]
    pops_18_msoa = pops_18_msoa[["people", "type",  "MSOA"]].groupby(["MSOA", "type"], as_index=False).sum()
    pops_18_msoa = pops_18_msoa.pivot(index="MSOA", columns="type", values="people")
    pops18 = pops18.groupby(["LAD Code", "type"], as_index=False).sum()
    pops18 = pops18.pivot(index="LAD Code", columns="type", values="people")
    
    pops_19_msoa = pd.merge(pops_19_msoa, selection, how="left", on="tfn_tt")
    pops_19_msoa = pops_19_msoa[pops_19_msoa["selection"] == 1]
    pops_19_msoa = pops_19_msoa.merge(msoa_la, how="left", on="MSOA")
    pops19 = pops_19_msoa[["people", "type",  "LAD Code"]]
    pops_19_msoa = pops_19_msoa[["people", "type", "MSOA"]].groupby(["MSOA", "type"], as_index=False).sum()
    pops_19_msoa = pops_19_msoa.pivot(index="MSOA", columns="type", values="people")
    pops19 = pops19.groupby(["LAD Code", "type"], as_index=False).sum()
    pops19 = pops19.pivot(index="LAD Code", columns="type", values="people")
    
    historic_fleet["2020 total"] = \
        (historic_fleet["2020 Car"] + historic_fleet["2020 HGV"] + historic_fleet["2020 LGV"])*1000
    historic_fleet["2019 total"] = \
        (historic_fleet["2019 Car"] + historic_fleet["2019 HGV"] + historic_fleet["2019 LGV"])*1000
    historic_fleet["2018 total"] = \
        (historic_fleet["2018 Car"] + historic_fleet["2018 HGV"] + historic_fleet["2018 LGV"])*1000
    
    historic_fleet["2020 ULEV"].replace(',', '', regex=True, inplace=True)
    historic_fleet["2019 ULEV"].replace(',', '', regex=True, inplace=True)
    historic_fleet["2018 ULEV"].replace(',', '', regex=True, inplace=True)
    
    historic_fleet["2020 proportion"] = historic_fleet["2020 ULEV"].astype(float)/historic_fleet["2020 total"]
    historic_fleet["2019 proportion"] = historic_fleet["2019 ULEV"].astype(float)/historic_fleet["2019 total"]
    historic_fleet["2018 proportion"] = historic_fleet["2018 ULEV"].astype(float)/historic_fleet["2018 total"]
    
    historic_fleet = historic_fleet[["LAD Code", "2020 proportion", "2019 proportion", "2018 proportion", "2020 total",
                                     "2019 total", "2018 total", "2020 ULEV", "2019 ULEV", "2018 ULEV"]]

    pops19 = pd.merge(pops19, historic_fleet, how="left", on="LAD Code").dropna()
    pops18 = pd.merge(pops18, historic_fleet, how="left", on="LAD Code").dropna()
    pops18 = pops18.reset_index()
    pops19 = pops19.reset_index()

    pops_19_18_shift = pops19.copy()
    pops_19_18_shift["shift"] = pops_19_18_shift["2019 total"] - pops_19_18_shift["2018 total"]
    pops_19_18_shift["ULEV shift"] = pops_19_18_shift["2019 ULEV"].astype(float) \
        - pops_19_18_shift["2018 ULEV"].astype(float)
    pops_19_18_shift["shift proportion"] = pops_19_18_shift["ULEV shift"]/pops_19_18_shift["shift"]

    pops18 = pops18.rename(columns={"2018 proportion": "proportion", "2018 total": "total", "2018 ULEV": "ULEV"})
    pops19 = pops19.rename(columns={"2019 proportion": "proportion", "2019 total": "total", "2019 ULEV": "ULEV"})
    pops_19_18_shift = pops_19_18_shift.rename(columns={"shift proportion": "proportion", "shift": "total",
                                                        "ULEV shift": "ULEV"})

    selection = selection.groupby("type", as_index=False).sum()
    selection_copy = selection.copy()
    selection = pd.concat([selection, selection_copy])
    selection = pd.concat([selection, selection_copy]).reset_index()
    selection["mods"] = 0
    selection["minima"] = 0
    selection["minima scale 2"] = 0
    selection["minima scale 3"] = 0
    selection = selection[["type", "mods", "minima", "minima scale 2", "minima scale 3"]]
    
    # minimisation #
    
    scale_2018 = scale_reduction_constants(pops18, selection)
    scale_2019 = scale_reduction_constants(pops19, selection)
    scale_2019_2018_shift = scale_reduction_constants(pops_19_18_shift, selection)
    for i in range(1):
        print("Running iteration %i" % i)
        # mods_2019 = minimisation(selection, "2019", pops19, scale_2019)
        # mods_2018 = minimisation(selection, "2018", pops18, scale_2018)
        mods_2019_2018_shift = minimisation(selection, "2019 from 2018 shift", pops_19_18_shift, scale_2019_2018_shift)
    
        # sales comparison
        # comparison back to LA and msoa data
    
        # pops18["generated proportion %i" % i] = func(pops18, mods_2018, scale_2018, 0, calc=False)
        # pops19["generated proportion %i" % i] = func(pops19, mods_2019, scale_2019, 0, calc=False)
        pops_19_18_shift["generated proportion %i" % i] = func(pops_19_18_shift, mods_2019_2018_shift,
                                                               scale_2019_2018_shift, 0, calc=False)
    
        # pops_18_msoa["generated proportion %i" % i] = func(pops_18_msoa, mods_2018, scale_2018, 0, calc=False)
        # pops_19_msoa["generated proportion %i" % i] = func(pops_19_msoa, mods_2019, scale_2019, 0, calc=False)
        pops_19_msoa["generated shift proportion %i" % i] = func(pops_19_msoa, mods_2019_2018_shift,
                                                                 scale_2019_2018_shift, 0, calc=False)
    
    # pops18.to_csv("2018_EV_sales_nssec.csv")
    # pops19.to_csv("2019_EV_sales_nssec.csv")
    ##pops_19_18_shift.to_csv("2019_EV_shift_sales_ns.csv")
    # pops_18_msoa.to_csv("msoa_2018_EV_sales_nssec.csv")
    ##pops_19_msoa.to_csv("msoa_2019_EV_sales_ns.csv")

    # NoHAM NoCarb data
    for scenario in ["SC01_JAM", "SC02_PP", "SC03_DD", "SC04_UZC"]:
        future_pop_read(scenario, msoa_to_noham, mods_2019_2018_shift, scale_2019_2018_shift, selection_choices)


global_step_size = 0.0001


if __name__ == "__main__":
    main()
