import pandas as pd
import pathlib
from pathlib import Path

input_dir = r"A:\QCR- assignments\Data for WSP/CAFCarb outputs"
output_dir = r"A:\QCR- assignments\Data for WSP/Proforma time periods/"
area_type_lookup = pd.read_csv(
            r"G:\raw_data\WSP Carbon Playbook\WSP_final\msoa_reclassifications_england_v6.csv")
area_type_lookup = area_type_lookup[["MSOA11CD", "RUC_Class"]].rename(columns={
            "MSOA11CD": "origin",
            "RUC_Class": "Origin Place Type"})
code_lookup = pd.read_csv(
            r"A:\QCR- assignments\03.Assignments\h5files\Other Inputs\North_through_lookup-MSOA11_lta.csv")


def create_purpose_component(purpose_data, purpose_name):
    purpose_data = purpose_data.groupby(["through", "user_class", "vehicle_type"], as_index=False).sum()
    purpose_data["Emissions (tCO2e)"] = purpose_data["total_gco2"] / 1000000
    purpose_data = purpose_data.rename(columns={"through": "LTA"})
    purpose_data.loc[purpose_data["user_class"] == "uc1", "Purpose"] = "Business"
    purpose_data.loc[purpose_data["user_class"] == "uc2", "Purpose"] = "Commute"
    purpose_data.loc[purpose_data["user_class"] == "uc3", "Purpose"] = "Other"
    purpose_data.loc[purpose_data["user_class"] == "uc4", "Purpose"] = "LGV"
    purpose_data.loc[purpose_data["user_class"] == "uc5", "Purpose"] = "HGV"
    purpose_data = purpose_data[["LTA", "Purpose", "vehicle_type", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Purpose", "vehicle_type"], as_index=False).sum()
    purpose_data.to_csv(output_dir + purpose_name + "_Purpose.csv", index=False)


def create_vehicle_type_component(vehicle_data, vehicle_name):
    vehicle_data["Emissions (tCO2e)"] = vehicle_data["total_gco2"] / 1000000
    vehicle_data = vehicle_data.rename(columns={"through": "LTA"})
    vehicle_data = vehicle_data[["LTA", "vehicle_type",  "Emissions (tCO2e)"]].groupby(
        ["LTA", "vehicle_type"], as_index=False).sum()
    vehicle_data.to_csv(output_dir + vehicle_name + "_Vehicle_Type.csv", index=False)


def create_place_type_component(type_data, type_name):
    type_data = type_data.merge(area_type_lookup, how="left", on="origin")
    origin_lta = code_lookup[["zone_cd", "lad"]].rename(columns={"zone_cd": "origin",
                                                                 "lad": "LTA"})
    type_data = type_data.merge(origin_lta, how="left", on="origin")
    type_data = type_data[type_data["LTA"] == type_data["through"]]
    type_data["Emissions (tCO2e)"] = type_data["total_gco2"] / 1000000
    type_data = type_data[["LTA", "Origin Place Type", "vehicle_type",  "Emissions (tCO2e)"]].groupby(
        ["LTA", "Origin Place Type", "vehicle_type"], as_index=False).sum()
    type_data.to_csv(output_dir + type_name + "_Place_Type.csv", index=False)


def create_trip_length_component(trip_data, trip_name):
    trip_data["Emissions (tCO2e)"] = trip_data["total_gco2"] / 1000000
    origin_lta = code_lookup[["zone_cd", "lad"]].rename(columns={"zone_cd": "origin",
                                                                 "lad": "LTA"})
    trip_data = trip_data.merge(origin_lta, how="left", on="origin")
    trip_data = trip_data.rename(columns={"trip_band": "Trip Length"})
    trip_data = trip_data[trip_data["LTA"] == trip_data["through"]]
    trip_data = trip_data[["LTA", "Trip Length", "vehicle_type", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Trip Length", "vehicle_type"], as_index=False).sum()
    trip_data.to_csv(output_dir + trip_name + "_Trip_Band.csv", index=False)


def create_genesis_component(genesis_data, genesis_name):
    # genesis_data = genesis_data[["origin", "destination", "through", "trip_band", "tailpipe_gco2", "grid_gco2"]]
    origin_lta = code_lookup[["zone_cd", "lad"]].rename(columns={"zone_cd": "origin",
                                                                               "lad": "o_lta"})
    genesis_data = genesis_data.merge(origin_lta, how="left", on="origin")
    destination_lta = code_lookup[["zone", "lad"]].rename(columns={"zone": "destination",
                                                                               "lad": "d_lta"})
    genesis_data = genesis_data.merge(destination_lta, how="left", on="destination")
    genesis_data = genesis_data.groupby(["o_lta", "d_lta", "through", "vehicle_type"], as_index=False).sum()
    genesis_data["Emissions (tCO2e)"] = genesis_data["total_gco2"] / 1000000
    genesis_data = genesis_data[["o_lta", "d_lta", "through", "vehicle_type", "Emissions (tCO2e)"]]  # .rename(columns={"through": "t_lta"})
    genesis_data["Genesis"] = "Through"
    genesis_data.loc[(genesis_data["o_lta"] == genesis_data["d_lta"]) &
                     (genesis_data["o_lta"] == genesis_data["through"]), "Genesis"] = "Internal"
    genesis_data.loc[(genesis_data["o_lta"] == genesis_data["through"]) &
                     (genesis_data["d_lta"] != genesis_data["through"]), "Genesis"] = "Outbound"
    genesis_data.loc[(genesis_data["d_lta"] == genesis_data["through"]) &
                     (genesis_data["o_lta"] != genesis_data["through"]), "Genesis"] = "Inbound"

    genesis_data = genesis_data.rename(columns={"through": "LTA"})
    genesis_data = genesis_data[["LTA", "Genesis", "vehicle_type", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Genesis", "vehicle_type"], as_index=False).sum()
    genesis_data.to_csv(output_dir + genesis_name + "_Genesis.csv", index=False)


def vkm_info_component(vkm_data, vkm_name):
    vkm_data.loc[vkm_data["fuel"] == "hydrogen", "fuel"] = "ZEV"
    vkm_data.loc[vkm_data["fuel"] == "bev", "fuel"] = "ZEV"
    vkm_data_totals = vkm_data[["through", "vehicle_type", "vkm"]].groupby(
        ["through", "year", "vehicle_type"], as_index=False).sum()
    vkm_data = vkm_data[vkm_data["fuel"] == "ZEV"]
    vkm_data = vkm_data[["through", "vkm"]].groupby(
        ["through", "year"], as_index=False).sum()
    vkm_data_totals = vkm_data_totals.rename(columns={"vkm": "vkm_total"})
    vkm_data = vkm_data.merge(vkm_data_totals, how="left", on=["through", "vehicle_type"])
    vkm_data = vkm_data.rename(columns={"through": "LTA"})
    vkm_data = vkm_data[["LTA", "vehicle_type", "vkm", "vkm_total"]].groupby(
        ["LTA", "vehicle_type"], as_index=False).sum()
    vkm_data.to_csv(output_dir + vkm_name + "_VKM_Info.csv", index=False)


def co2_info_component(co2_data, co2_name):
    co2_data = co2_data[["through", "vehicle_type", "total_gco2"]].groupby(
        ["through", "vehicle_type"], as_index=False).sum()
    co2_data["Emissions (MtCO2e)"] = co2_data["total_gco2"] / (1000000 * 1000000)
    co2_data = co2_data.rename(columns={"through": "LTA"})
    co2_data = co2_data[["LTA", "vehicle_type", "Emissions (MtCO2e)"]].groupby(["LTA", "vehicle_type"], as_index=False).sum()
    co2_data.to_csv(output_dir + co2_name + "_CO2_Info.csv", index=False)


for path in Path(input_dir).glob("*.h5"):
    print(f"Running data for {path}")
    print(path)
    keystoenum = pd.HDFStore(path, mode="r").keys()
    demand = pd.DataFrame()
    file_name = "key_unspecififed"
    for scenario in ["BAU", "UE"]:
        if scenario in str(path):
            file_name = scenario + "_"
    for year in ["2018", "2028", "2038", "2043", "2048"]:
        if year in str(path):
            file_name = file_name + year + "_"

    for time in ["TS1", "TS2", "TS3"]:
        if time in str(path):
            file_name = file_name + time + "_"

    for demand_key in keystoenum:
        print(f"Appending slice {demand_key}")
        demand_slice = pd.read_hdf(path, demand_key, mode="r")
        demand = pd.concat([demand, demand_slice])

    # demand = demand[demand.through != -1]
    # demand = demand[demand.through != 22]

    print("creating purpose")
    create_purpose_component(demand[["through", "user_class", "vehicle_type", "total_gco2"]], file_name)
    print("creating vehicle type")
    create_vehicle_type_component(demand[["through", "vehicle_type", "total_gco2"]], file_name)
    print("creating place type")
    create_place_type_component(demand[["through", "origin", "vehicle_type", "total_gco2"]], file_name)
    print("creating trip length")
    create_trip_length_component(demand[["origin", "through", "trip_band", "vehicle_type", "total_gco2"]], file_name)
    print("creating genesis")
    create_genesis_component(demand[["origin", "destination", "through", "vehicle_type", "total_gco2"]]
                             , file_name)
    print("creating co2 info")
    co2_info_component(demand[["through", "vehicle_type", "total_gco2"]], file_name)
