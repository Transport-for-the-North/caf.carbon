# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd

empty=pd.DataFrame()

for scenario in ["JAM", "PP", "DD", "UZC"]:

    base = pd.read_csv(f"D:\GitHub\caf.carbon\CAFCarb\outputs\TfN_FTS_{scenario}_fleet_emissions_2024_06_10.csv")
    
    base = base[["year", "vehicle_type", "fuel", "tally"]]
    
    # base = base[base["year"]==2023]
    # base = base[base["cohort"].isin([2023,2022,2021])]
    
    total = base.groupby(["year", "vehicle_type"], as_index=False).sum()
    
    total = total.rename(columns={"tally": "total"})
    
    base["scenario"] = scenario
    
    base = base.groupby(["year", "vehicle_type", "fuel", "scenario"], as_index=False).sum()
    
    base = base.merge(total, how="left", on=["year", "vehicle_type"])

    base["fuel percentage"] = base["tally"]/base["total"]
    
    empty = pd.concat([empty, base])

empty.to_csv("D:\FTS_fuel_percentages.csv")


