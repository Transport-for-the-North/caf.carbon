import pandas as pd
import pathlib
from pathlib import Path

input_dir = r"D:/GitHub/caf.carbon/CAFCarb/outputs"
output_dir = r"A:/QCR- assignments/03.Assignments/h5files/playbook_tables/"
area_type_lookup = pd.read_csv(
            r"G:\raw_data\WSP Carbon Playbook\WSP_final\msoa_reclassifications_england_v6.csv")
area_type_lookup = area_type_lookup[["MSOA11CD", "RUC_Class"]].rename(columns={
            "MSOA11CD": "origin",
            "RUC_Class": "Origin Place Type"})
code_lookup = pd.read_csv(
            r"A:\QCR- assignments\03.Assignments\h5files\Other Inputs\North_through_lookup-MSOA11_lta.csv")


def create_purpose_component(purpose_data, purpose_name):
    # purpose_data = purpose_data[["through", "user_class", "tailpipe_gco2", "grid_gco2"]]
    purpose_data["Emissions (tCO2e)"] = purpose_data["tailpipe_gco2"] + purpose_data["grid_gco2"]
    purpose_data["Emissions (tCO2e)"] = purpose_data["Emissions (tCO2e)"] / (1000 * 1000)
    purpose_data = purpose_data.rename(columns={"through": "LTA"})
    purpose_data.loc[purpose_data["user_class"] == "uc1", "Purpose"] = "Business"
    purpose_data.loc[purpose_data["user_class"] == "uc2", "Purpose"] = "Commute"
    purpose_data.loc[purpose_data["user_class"] == "uc3", "Purpose"] = "Other"
    purpose_data.loc[purpose_data["user_class"] == "uc4", "Purpose"] = "LGV"
    purpose_data.loc[purpose_data["user_class"] == "uc5", "Purpose"] = "HGV"
    purpose_data = purpose_data[["LTA", "Purpose", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Purpose"], as_index=False).sum()
    purpose_data.to_csv(output_dir + purpose_name + "_Purpose.csv")


def create_vehicle_type_component(vehicle_data, vehicle_name):
    # vehicle_data = vehicle_data[["through", "vehicle_type", "tailpipe_gco2", "grid_gco2"]]
    vehicle_data["Emissions (tCO2e)"] = vehicle_data["tailpipe_gco2"] + vehicle_data["grid_gco2"]
    vehicle_data["Emissions (tCO2e)"] = vehicle_data["Emissions (tCO2e)"] / (1000 * 1000)
    vehicle_data.loc[vehicle_data["vehicle_type"] == "car", "vehicle_type"] = "Car"
    vehicle_data.loc[vehicle_data["vehicle_type"] == "lgv", "vehicle_type"] = "LGV"
    vehicle_data.loc[vehicle_data["vehicle_type"] == "hgv", "vehicle_type"] = "HGV"
    vehicle_data = vehicle_data.rename(columns={"through": "LTA", "vehicle_type": "Vehicle"})
    vehicle_data = vehicle_data[["LTA", "Vehicle", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Vehicle"], as_index=False).sum()
    vehicle_data.to_csv(output_dir + vehicle_name + "_Vehicle_Type.csv")


def create_place_type_component(type_data, type_name):
    # type_data = type_data[["through", "origin", "tailpipe_gco2", "grid_gco2"]]
    type_data = type_data.merge(area_type_lookup, how="left", on="origin")
    type_data["Emissions (tCO2e)"] = type_data["tailpipe_gco2"] + type_data["grid_gco2"]
    type_data["Emissions (tCO2e)"] = type_data["Emissions (tCO2e)"] / (1000 * 1000)
    type_data = type_data.rename(columns={"through": "LTA"})
    type_data = type_data[["LTA", "Origin Place Type", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Origin Place Type"], as_index=False).sum()
    type_data.to_csv(output_dir + type_name + "_Place_Type.csv")


def create_trip_length_component(trip_data, trip_name):
    # trip_data = trip_data[["origin", "destination", "through", "trip_band", "tailpipe_gco2", "grid_gco2"]]
    trip_data["Emissions (tCO2e)"] = trip_data["tailpipe_gco2"] + trip_data["grid_gco2"]
    trip_data["Emissions (tCO2e)"] = trip_data["Emissions (tCO2e)"] / (1000 * 1000)
    trip_data = trip_data.rename(columns={"through": "LTA", "trip_band": "Trip Length"})
    trip_data = trip_data[["LTA", "Trip Length", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Trip Length"], as_index=False).sum()
    trip_data.to_csv(output_dir + trip_name + "_Trip_Band.csv")


def create_genesis_component(genesis_data, genesis_name):
    # genesis_data = genesis_data[["origin", "destination", "through", "trip_band", "tailpipe_gco2", "grid_gco2"]]
    origin_lta = code_lookup[["zone_cd", "through_name"]].rename(columns={"zone_cd": "origin",
                                                                               "through_name": "o_lta"})
    genesis_data = genesis_data.merge(origin_lta, how="left", on="origin")
    destination_lta = code_lookup[["zone", "through_name"]].rename(columns={"zone": "destination",
                                                                               "through_name": "d_lta"})
    genesis_data = genesis_data.merge(destination_lta, how="left", on="destination")
    genesis_data["Emissions (tCO2e)"] = genesis_data["tailpipe_gco2"] + genesis_data["grid_gco2"]
    genesis_data["Emissions (tCO2e)"] = genesis_data["Emissions (tCO2e)"] / (1000 * 1000)
    genesis_data = genesis_data[["o_lta", "d_lta", "t_lta", "Emissions"]]
    genesis_data["Genesis"] = "Through"
    genesis_data.loc[(genesis_data["o_lta"] == genesis_data["d_lta"]) &
                     (genesis_data["o_lta"] == genesis_data["through"]), "Genesis"] = "Internal"
    genesis_data.loc[(genesis_data["o_lta"] == genesis_data["through"]) &
                     (genesis_data["d_lta"] != genesis_data["through"]), "Genesis"] = "Outbound"
    genesis_data.loc[(genesis_data["d_lta"] == genesis_data["through"]) &
                     (genesis_data["o_lta"] != genesis_data["through"]), "Genesis"] = "Inbound"

    genesis_data = genesis_data.rename(columns={"through": "LTA"})
    genesis_data = genesis_data[["LTA", "Genesis", "Emissions (tCO2e)"]].groupby(
        ["LTA", "Trip Length"], as_index=False).sum()
    genesis_data.to_csv(output_dir + genesis_name + "_Genesis.csv")


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
    vkm_data.to_csv(output_dir + vkm_name + "_VKM_Info.csv")


def co2_info_component(co2_data, co2_name):
    co2_data = co2_data[["through", "vehicle_type", "tailpipe_gco2", "grid_gco2"]].groupby(
        ["through", "vehicle_type"], as_index=False).sum()
    co2_data["Emissions (MtCO2e)"] = co2_data["tailpipe_gco2"] + co2_data["grid_gco2"]
    co2_data["Emissions (MtCO2e)"] = co2_data["Emissions (MtCO2e)"] / (1000 * 1000 * 1000 * 1000)
    co2_data = co2_data.rename(columns={"through": "LTA"})
    co2_data = co2_data[["LTA", "vehicle_type", "Emissions (MtCO2e)"]].groupby("LTA", as_index=False).sum()
    co2_data.to_csv(output_dir + co2_name + "_CO2_Info.csv")


for path in Path(input_dir).glob("*.h5"):
    print(f"Running data for {path}")
    keystoenum = pd.HDFStore(path, mode="r").keys()
    for demand_key in keystoenum:
        demand = pd.read_hdf(path, demand_key, mode="r")
        if "BAU" in path:
            file_name = "BAU_" + str(demand_key)
        else:
            file_name = "CAS_" + str(demand_key)

        create_purpose_component(demand[["through", "user_class", "tailpipe_gco2", "grid_gco2"]], file_name)
        create_vehicle_type_component(demand[["through", "vehicle_type", "tailpipe_gco2", "grid_gco2"]], file_name)
        create_place_type_component(demand[["through", "origin", "tailpipe_gco2", "grid_gco2"]], file_name)
        create_trip_length_component(demand[["through", "trip_band", "tailpipe_gco2", "grid_gco2"]], file_name)
        create_genesis_component(demand[["origin", "destination", "through", "trip_band", "tailpipe_gco2", "grid_gco2"]]
                                 , file_name)
        vkm_info_component(demand[["through", "fuel", "year", "vehicle_type", "vkm"]], file_name)
        co2_info_component(demand[["through", "vehicle_type", "tailpipe_gco2", "grid_gco2"]], file_name)
