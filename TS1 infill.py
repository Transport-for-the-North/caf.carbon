# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 13:34:23 2024

@author: Renewed
"""

import pandas as pd
path = r"A:/QCR- assignments/Data for WSP/Proforma time periods/"


genesis_ts2_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS2__Genesis.csv")
genesis_ts3_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS3__Genesis.csv")

genesis_ts1_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS1__Genesis.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
genesis_ts2_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS2__Genesis.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
genesis_ts3_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS3__Genesis.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

genesis_2028 = pd.merge(genesis_ts1_2028_BAU, genesis_ts3_2028_BAU,
                        how="left", on=["LTA", "Genesis", "vehicle_type"])
genesis_2028["ratio"] = genesis_2028["ts1"] / genesis_2028["ts3"]
genesis_2028 = genesis_2028[["LTA", "Genesis", "vehicle_type", "ratio"]]
genesis_2018_ts1 = genesis_ts3_2018_BAU.merge(genesis_2028, how="left", on=["LTA", "Genesis", "vehicle_type"])
genesis_2018_ts1["Emissions (tCO2e)"] = genesis_2018_ts1["Emissions (tCO2e)"] * genesis_2018_ts1["ratio"]
genesis_2018_ts1 = genesis_2018_ts1[["LTA", "Genesis", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2018_TS1__Genesis.csv", index=False)


CO2_info_ts2_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS2__CO2_info.csv")
CO2_info_ts3_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS3__CO2_info.csv")

CO2_info_ts1_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS1__CO2_info.csv").rename(
    columns={"Emissions (MtCO2e)": "ts1"})
CO2_info_ts2_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS2__CO2_info.csv").rename(
    columns={"Emissions (MtCO2e)": "ts2"})
CO2_info_ts3_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS3__CO2_info.csv").rename(
    columns={"Emissions (MtCO2e)": "ts3"})

CO2_info_2028 = pd.merge(CO2_info_ts1_2028_BAU, CO2_info_ts3_2028_BAU,
                        how="left", on=["LTA", "vehicle_type"])
CO2_info_2028["ratio"] = CO2_info_2028["ts1"] / CO2_info_2028["ts3"]
CO2_info_2028 = CO2_info_2028[["LTA", "vehicle_type", "ratio"]]
CO2_info_2018_ts1 = CO2_info_ts3_2018_BAU.merge(CO2_info_2028, how="left", on=["LTA", "vehicle_type"])
CO2_info_2018_ts1["Emissions (MtCO2e)"] = CO2_info_2018_ts1["Emissions (MtCO2e)"] * CO2_info_2018_ts1["ratio"]
CO2_info_2018_ts1 = CO2_info_2018_ts1[["LTA", "vehicle_type", "Emissions (MtCO2e)"]].to_csv(rf"{path}BAU_2018_TS1__CO2_info.csv", index=False)


Trip_Band_ts2_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS2__Trip_Band.csv")
Trip_Band_ts3_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS3__Trip_Band.csv")

Trip_Band_ts1_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS1__Trip_Band.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
Trip_Band_ts2_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS2__Trip_Band.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
Trip_Band_ts3_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS3__Trip_Band.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

Trip_Band_2028 = pd.merge(Trip_Band_ts1_2028_BAU, Trip_Band_ts3_2028_BAU,
                        how="left", on=["LTA", "Trip Length", "vehicle_type"])
Trip_Band_2028["ratio"] = Trip_Band_2028["ts1"] / Trip_Band_2028["ts3"]
Trip_Band_2028 = Trip_Band_2028[["LTA", "Trip Length", "vehicle_type", "ratio"]]
Trip_Band_2018_ts1 = Trip_Band_ts3_2018_BAU.merge(Trip_Band_2028, how="left", on=["LTA", "Trip Length", "vehicle_type"])
Trip_Band_2018_ts1["Emissions (tCO2e)"] = Trip_Band_2018_ts1["Emissions (tCO2e)"] * Trip_Band_2018_ts1["ratio"]
Trip_Band_2018_ts1 = Trip_Band_2018_ts1[["LTA", "Trip Length", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2018_TS1__Trip_Band.csv", index=False)



Place_Type_ts2_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS2__Place_Type.csv")
Place_Type_ts3_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS3__Place_Type.csv")

Place_Type_ts1_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS1__Place_Type.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
Place_Type_ts2_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS2__Place_Type.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
Place_Type_ts3_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS3__Place_Type.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

Place_Type_2028 = pd.merge(Place_Type_ts1_2028_BAU, Place_Type_ts3_2028_BAU,
                        how="left", on=["LTA", "Origin Place Type", "vehicle_type"])
Place_Type_2028["ratio"] = Place_Type_2028["ts1"] / Place_Type_2028["ts3"]
Place_Type_2028 = Place_Type_2028[["LTA", "Origin Place Type", "vehicle_type", "ratio"]]
Place_Type_2018_ts1 = Place_Type_ts3_2018_BAU.merge(Place_Type_2028, how="left", on=["LTA", "Origin Place Type", "vehicle_type"])
Place_Type_2018_ts1["Emissions (tCO2e)"] = Place_Type_2018_ts1["Emissions (tCO2e)"] * Place_Type_2018_ts1["ratio"]
Place_Type_2018_ts1 = Place_Type_2018_ts1[["LTA", "Origin Place Type", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2018_TS1__Place_Type.csv", index=False)



vehicle_type_ts2_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS2__vehicle_type.csv")
vehicle_type_ts3_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS3__vehicle_type.csv")

vehicle_type_ts1_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS1__vehicle_type.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
vehicle_type_ts2_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS2__vehicle_type.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
vehicle_type_ts3_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS3__vehicle_type.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

vehicle_type_2028 = pd.merge(vehicle_type_ts1_2028_BAU, vehicle_type_ts3_2028_BAU,
                        how="left", on=["LTA", "vehicle_type"])
vehicle_type_2028["ratio"] = vehicle_type_2028["ts1"] / vehicle_type_2028["ts3"]
vehicle_type_2028 = vehicle_type_2028[["LTA", "vehicle_type", "ratio"]]
vehicle_type_2018_ts1 = vehicle_type_ts3_2018_BAU.merge(vehicle_type_2028, how="left", on=["LTA", "vehicle_type"])
vehicle_type_2018_ts1["Emissions (tCO2e)"] = vehicle_type_2018_ts1["Emissions (tCO2e)"] * vehicle_type_2018_ts1["ratio"]
vehicle_type_2018_ts1 = vehicle_type_2018_ts1[["LTA", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2018_TS1__vehicle_type.csv", index=False)



Purpose_ts2_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS2__Purpose.csv")
Purpose_ts3_2018_BAU = pd.read_csv(rf"{path}BAU_2018_TS3__Purpose.csv")

Purpose_ts1_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS1__Purpose.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
Purpose_ts2_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS2__Purpose.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
Purpose_ts3_2028_BAU = pd.read_csv(rf"{path}BAU_2028_TS3__Purpose.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

Purpose_2028 = pd.merge(Purpose_ts1_2028_BAU, Purpose_ts3_2028_BAU,
                        how="left", on=["LTA", "Purpose", "vehicle_type"])
Purpose_2028["ratio"] = Purpose_2028["ts1"] / Purpose_2028["ts3"]
Purpose_2028 = Purpose_2028[["LTA", "Purpose", "vehicle_type", "ratio"]]
Purpose_2018_ts1 = Purpose_ts3_2018_BAU.merge(Purpose_2028, how="left", on=["LTA", "Purpose", "vehicle_type"])
Purpose_2018_ts1["Emissions (tCO2e)"] = Purpose_2018_ts1["Emissions (tCO2e)"] * Purpose_2018_ts1["ratio"]
Purpose_2018_ts1 = Purpose_2018_ts1[["LTA", "Purpose", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2018_TS1__Purpose.csv", index=False)

### 2043 ts2


genesis_ts1_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS1__Genesis.csv")
genesis_ts3_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS3__Genesis.csv")

genesis_ts1_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS1__Genesis.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
genesis_ts2_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS2__Genesis.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
genesis_ts3_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS3__Genesis.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

genesis_2048 = pd.merge(genesis_ts2_2048_BAU, genesis_ts3_2048_BAU,
                        how="left", on=["LTA", "Genesis", "vehicle_type"])
genesis_2048["ratio"] = genesis_2048["ts2"] / genesis_2048["ts3"]
genesis_2048 = genesis_2048[["LTA", "Genesis", "vehicle_type", "ratio"]]
genesis_2043_ts2 = genesis_ts3_2043_BAU.merge(genesis_2048, how="left", on=["LTA", "Genesis", "vehicle_type"])
genesis_2043_ts2["Emissions (tCO2e)"] = genesis_2043_ts2["Emissions (tCO2e)"] * genesis_2043_ts2["ratio"]
genesis_2043_ts2 = genesis_2043_ts2[["LTA", "Genesis", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2043_TS2__Genesis.csv", index=False)


CO2_info_ts1_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS1__CO2_info.csv")
CO2_info_ts3_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS3__CO2_info.csv")

CO2_info_ts1_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS1__CO2_info.csv").rename(
    columns={"Emissions (MtCO2e)": "ts1"})
CO2_info_ts2_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS2__CO2_info.csv").rename(
    columns={"Emissions (MtCO2e)": "ts2"})
CO2_info_ts3_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS3__CO2_info.csv").rename(
    columns={"Emissions (MtCO2e)": "ts3"})

CO2_info_2048 = pd.merge(CO2_info_ts2_2048_BAU, CO2_info_ts3_2048_BAU,
                        how="left", on=["LTA", "vehicle_type"])
CO2_info_2048["ratio"] = CO2_info_2048["ts2"] / CO2_info_2048["ts3"]
CO2_info_2048 = CO2_info_2048[["LTA", "vehicle_type", "ratio"]]
CO2_info_2043_ts2 = CO2_info_ts3_2043_BAU.merge(CO2_info_2048, how="left", on=["LTA", "vehicle_type"])
CO2_info_2043_ts2["Emissions (MtCO2e)"] = CO2_info_2043_ts2["Emissions (MtCO2e)"] * CO2_info_2043_ts2["ratio"]
CO2_info_2043_ts2 = CO2_info_2043_ts2[["LTA", "vehicle_type", "Emissions (MtCO2e)"]].to_csv(rf"{path}BAU_2043_TS2__CO2_info.csv", index=False)


Trip_Band_ts1_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS1__Trip_Band.csv")
Trip_Band_ts3_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS3__Trip_Band.csv")

Trip_Band_ts1_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS1__Trip_Band.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
Trip_Band_ts2_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS2__Trip_Band.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
Trip_Band_ts3_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS3__Trip_Band.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

Trip_Band_2048 = pd.merge(Trip_Band_ts2_2048_BAU, Trip_Band_ts3_2048_BAU,
                        how="left", on=["LTA", "Trip Length", "vehicle_type"])
Trip_Band_2048["ratio"] = Trip_Band_2048["ts2"] / Trip_Band_2048["ts3"]
Trip_Band_2048 = Trip_Band_2048[["LTA", "Trip Length", "vehicle_type", "ratio"]]
Trip_Band_2043_ts2 = Trip_Band_ts3_2043_BAU.merge(Trip_Band_2048, how="left", on=["LTA", "Trip Length", "vehicle_type"])
Trip_Band_2043_ts2["Emissions (tCO2e)"] = Trip_Band_2043_ts2["Emissions (tCO2e)"] * Trip_Band_2043_ts2["ratio"]
Trip_Band_2043_ts2 = Trip_Band_2043_ts2[["LTA", "Trip Length", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2043_TS2__Trip_Band.csv", index=False)



Place_Type_ts1_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS1__Place_Type.csv")
Place_Type_ts3_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS3__Place_Type.csv")

Place_Type_ts1_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS1__Place_Type.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
Place_Type_ts2_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS2__Place_Type.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
Place_Type_ts3_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS3__Place_Type.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

Place_Type_2048 = pd.merge(Place_Type_ts2_2048_BAU, Place_Type_ts3_2048_BAU,
                        how="left", on=["LTA", "Origin Place Type", "vehicle_type"])
Place_Type_2048["ratio"] = Place_Type_2048["ts2"] / Place_Type_2048["ts3"]
Place_Type_2048 = Place_Type_2048[["LTA", "Origin Place Type", "vehicle_type", "ratio"]]
Place_Type_2043_ts2 = Place_Type_ts3_2043_BAU.merge(Place_Type_2048, how="left", on=["LTA", "Origin Place Type", "vehicle_type"])
Place_Type_2043_ts2["Emissions (tCO2e)"] = Place_Type_2043_ts2["Emissions (tCO2e)"] * Place_Type_2043_ts2["ratio"]
Place_Type_2043_ts2 = Place_Type_2043_ts2[["LTA", "Origin Place Type", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2043_TS2__Place_Type.csv", index=False)



vehicle_type_ts1_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS1__vehicle_type.csv")
vehicle_type_ts3_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS3__vehicle_type.csv")

vehicle_type_ts1_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS1__vehicle_type.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
vehicle_type_ts2_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS2__vehicle_type.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
vehicle_type_ts3_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS3__vehicle_type.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

vehicle_type_2048 = pd.merge(vehicle_type_ts2_2048_BAU, vehicle_type_ts3_2048_BAU,
                        how="left", on=["LTA", "vehicle_type"])
vehicle_type_2048["ratio"] = vehicle_type_2048["ts2"] / vehicle_type_2048["ts3"]
vehicle_type_2048 = vehicle_type_2048[["LTA", "vehicle_type", "ratio"]]
vehicle_type_2043_ts2 = vehicle_type_ts3_2043_BAU.merge(vehicle_type_2048, how="left", on=["LTA", "vehicle_type"])
vehicle_type_2043_ts2["Emissions (tCO2e)"] = vehicle_type_2043_ts2["Emissions (tCO2e)"] * vehicle_type_2043_ts2["ratio"]
vehicle_type_2043_ts2 = vehicle_type_2043_ts2[["LTA", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2043_TS2__vehicle_type.csv", index=False)



Purpose_ts1_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS1__Purpose.csv")
Purpose_ts3_2043_BAU = pd.read_csv(rf"{path}BAU_2043_TS3__Purpose.csv")

Purpose_ts1_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS1__Purpose.csv").rename(
    columns={"Emissions (tCO2e)": "ts1"})
Purpose_ts2_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS2__Purpose.csv").rename(
    columns={"Emissions (tCO2e)": "ts2"})
Purpose_ts3_2048_BAU = pd.read_csv(rf"{path}BAU_2048_TS3__Purpose.csv").rename(
    columns={"Emissions (tCO2e)": "ts3"})

Purpose_2048 = pd.merge(Purpose_ts2_2048_BAU, Purpose_ts3_2048_BAU,
                        how="left", on=["LTA", "Purpose", "vehicle_type"])
Purpose_2048["ratio"] = Purpose_2048["ts2"] / Purpose_2048["ts3"]
Purpose_2048 = Purpose_2048[["LTA", "Purpose", "vehicle_type", "ratio"]]
Purpose_2043_ts2 = Purpose_ts3_2043_BAU.merge(Purpose_2048, how="left", on=["LTA", "Purpose", "vehicle_type"])
Purpose_2043_ts2["Emissions (tCO2e)"] = Purpose_2043_ts2["Emissions (tCO2e)"] * Purpose_2043_ts2["ratio"]
Purpose_2043_ts2 = Purpose_2043_ts2[["LTA", "Purpose", "vehicle_type", "Emissions (tCO2e)"]].to_csv(rf"{path}BAU_2043_TS2__Purpose.csv", index=False)
