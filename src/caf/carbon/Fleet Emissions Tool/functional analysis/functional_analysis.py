# -*- coding: utf-8 -*-
"""
Created on Fri May 13 13:12:44 2022

@author: Invincible
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

pops_18_msoa = pd.read_pickle("output_7_resi_gb_msoa_tfn_tt_2018_pop.pbz2", compression="bz2")
pops_19_msoa = pd.read_pickle("output_7_resi_gb_msoa_tfn_tt_2019_pop.pbz2", compression="bz2")

types = pd.read_csv("nts_trav_types_nssec.csv")

msoa_la = pd.read_csv("MSOA_LAD_lookup.csv").rename(columns={"MSOA11CD": "MSOA", "TAG_LAD": "LAD Code"})
historic_fleet = pd.read_csv("Historic LA fleet.csv")
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

historic_fleet["20 18 shift"] = historic_fleet["2020 total"] - historic_fleet["2018 total"]
historic_fleet["ULEV shift"] = historic_fleet["2020 ULEV"].astype(float) \
        - historic_fleet["2018 ULEV"].astype(float)
historic_fleet["shift proportion"] = historic_fleet["ULEV shift"]/historic_fleet["20 18 shift"]
    
pops_18_msoa = pd.merge(pops_18_msoa, types, how="left", on="tfn_tt")
pops_18_msoa = pops_18_msoa[pops_18_msoa["selection"] == 1]
pops_18_msoa = pops_18_msoa.merge(msoa_la, how="left", on="MSOA")
pops18 = pops_18_msoa[["people", "nssoc", "ns1", "soc1", "empskill", "LAD Code"]]
# pops18 = pops18.groupby(["LAD Code", "nssoc", "ns1", "soc1", "empskill"], as_index=False).sum()
# pops18 = pops18.pivot(index="LAD Code", columns=["nssoc", "ns1", "soc1", "empskill"], values="people")
    
pops_19_msoa = pd.merge(pops_19_msoa, types, how="left", on="tfn_tt")
pops_19_msoa = pops_19_msoa[pops_19_msoa["selection"] == 1]
pops_19_msoa = pops_19_msoa.merge(msoa_la, how="left", on="MSOA")
pops19 = pops_19_msoa[["people", "nssoc", "ns1", "soc1", "empskill", "LAD Code"]]

#pops_18_msoa = pops_18_msoa[["people", "nssoc", "ns1", "soc1", "empskill", "MSOA"]].groupby(["MSOA", "type"], as_index=False).sum()
#pops_18_msoa = pops_18_msoa.pivot(index="MSOA", columns=["nssoc", "ns1", "soc1", "empskill"], values="people")

#pops_19_msoa = pops_19_msoa[["people", "nssoc", "ns1", "soc1", "empskill", "MSOA"]].groupby(["MSOA", "type"], as_index=False).sum()
#pops_19_msoa = pops_19_msoa.pivot(index="MSOA", columns=["nssoc", "ns1", "soc1", "empskill"], values="people")

def type_grapher(pop, zone, types):
    pop = pop[["people", zone, types]]
    pop["people"] = pop["people"]/10000
    pop = pop.groupby([zone, types], as_index=False).sum()
    pop = pop.pivot(index=zone, columns=types, values="people")
    t = len(pop.columns)
    pop = historic_fleet[["LAD Code", "shift proportion"]].merge(pop, how="left", on=zone)
    for i in range(t):
        x_name = pop.columns[i+2]
        # print(x_name)
        x = pd.DataFrame()
        y = pd.DataFrame()
        x[x_name] = pop[x_name]
        y["shift proportion"] = pop["shift proportion"]
        x = x.reset_index()
        y = y.reset_index()
        x["type rank"] = x[x_name].rank(method='max')
        y["shift rank"] = y["shift proportion"].rank(method='max')
        rho = pd.merge(x, y, left_index=True, right_index=True)
        rho["d"] = (rho["type rank"] - rho["shift rank"])  * (rho["type rank"] - rho["shift rank"])
        r = 1  - ((6*rho["d"].sum())/(len(rho)*(len(rho)*len(rho)-1)))
        # definitions for the axes
        left, width = 0.1, 0.65
        bottom, height = 0.1, 0.65
        spacing = 0.005
        
        
        rect_scatter = [left, bottom, width, height]
        rect_histx = [left, bottom + height + spacing, width, 0.2]
        rect_histy = [left + width + spacing, bottom, 0.2, height]
        
        # start with a square Figure
        fig = plt.figure(figsize=(8, 8))
        
        ax = fig.add_axes(rect_scatter)
        ax_histx = fig.add_axes(rect_histx, sharex=ax)
        ax_histy = fig.add_axes(rect_histy, sharey=ax)
        
        # use the previously defined function
        scatter_hist(pop[x_name], pop["shift proportion"], ax, ax_histx, ax_histy)
        display_name =  x_name  + ":\n rho = " + str(round(r,2))
        plt.title(display_name, fontsize=24)
        plt.savefig("%s.png" %x_name)
        plt.show()
    return pop

def scatter_hist(x, y, ax, ax_histx, ax_histy):
    # no labels
    ax_histx.tick_params(axis="x", labelbottom=False)
    ax_histy.tick_params(axis="y", labelleft=False)

    # the scatter plot:
    ax.scatter(x, y)

    # now determine nice limits by hand:
    binwidth = 0.25
    xymax = max(np.max(np.abs(x)), np.max(np.abs(y)))
    lim = (float(xymax/binwidth) + 1) * binwidth

    x_bins = np.arange(0, lim + binwidth, binwidth)
    y_bins = np.arange(0, 0.3, 0.01)
    ax_histx.hist(x, bins=x_bins)
    ax_histy.hist(y, bins=y_bins, orientation='horizontal')

test = type_grapher(pops19, "LAD Code", "nssoc")
test = type_grapher(pops19, "LAD Code", "soc1")
test = type_grapher(pops19, "LAD Code", "ns1")
test = type_grapher(pops19, "LAD Code", "empskill")