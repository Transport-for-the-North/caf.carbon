import pandas as pd
from load_data import (
    REGION_FILTER,
    DEMAND_FACTORS,
    DEMAND_DATA,
    LINK_DATA,
    MSOA_RTM_TRANSLATION,
    LINK_ROAD_TYPES,
    DEMAND_OUT_PATH
)


class Demand:
    """Predict emissions using emissions, demand and projected fleet data."""

    def __init__(self, available_years):
        """Initialise functions and set class variables."""

        self.demand_factors = pd.read_csv(DEMAND_FACTORS)
        self.region_filter = pd.read_csv(REGION_FILTER)
        self.region_filter = self.region_filter[
            self.region_filter["stb_name"].isin(["Transport for the South East"])
        ]
        self.demand_data_path = str(DEMAND_DATA)
        self.link_data_path = str(LINK_DATA)
        self.out_path = str(DEMAND_OUT_PATH)
        self.link_road_types = pd.read_csv(LINK_ROAD_TYPES)
        self.link_road_types = self.link_road_types[["a", "b", "rdtype"]].rename(
            columns={"rdtype": "road_type"}
        )
        self.link_road_types["a"] = self.link_road_types["a"].astype(str)
        self.link_road_types["b"] = self.link_road_types["b"].astype(str)
        self.msoa_rtm_lookup = pd.read_csv(str(MSOA_RTM_TRANSLATION)).rename(
            columns={"msoa11_id": "origin_zone"}  # msoa11cd_id or msoa11_id
        )
        self.msoa_rtm_lookup = self.msoa_rtm_lookup[
            ["origin_zone", "SERTM_id", "SERTM_to_msoa11"]  # ["origin_zone",  "MRTM2_v14_id", "MRTM2_v14_to_msoa11"]
        ]
        self.msoa_rtm_lookup = self.msoa_rtm_lookup.loc[
            self.msoa_rtm_lookup["origin_zone"].isin(self.region_filter["msoa11_id"])
        ].reset_index(drop=True)
        self.available_years = available_years

        self.car_demand = pd.DataFrame(
            columns=[
                "origin_zone",
                "vkm_10-30_kph",
                "vkm_30-50_kph",
                "vkm_50-70_kph",
                "vkm_70-90_kph",
                "vkm_90-110_kph",
                "total_vehicle_km",
                "year",
            ]
        )
        self.lgv_demand = pd.DataFrame(
            columns=[
                "origin_zone",
                "vkm_10-30_kph",
                "vkm_30-50_kph",
                "vkm_50-70_kph",
                "vkm_70-90_kph",
                "vkm_90-110_kph",
                "total_vehicle_km",
                "year",
            ]
        )
        self.hgv_demand = pd.DataFrame(
            columns=[
                "origin_zone",
                "vkm_10-30_kph",
                "vkm_30-50_kph",
                "vkm_50-70_kph",
                "vkm_70-90_kph",
                "vkm_90-110_kph",
                "total_vehicle_km",
                "year",
            ]
        )
        self.process_demand()
        self.output_demand_data()

    def assign_speed_band_vkm(self, link_data, demand_data, user_class, year, component):
        """Assign and classify demand by speed band"""
        user_class_speed = "Speed_" + user_class
        link_data = link_data[["a", "b", "Distance", user_class_speed]]
        demand_data = demand_data[["o", "a", "b", "abs_demand"]]
        demand_data = demand_data.groupby(["o", "a", "b"], as_index=False).sum()
        demand_data = self.spatial_translation(demand_data)
        demand_data = demand_data.merge(link_data, how="left", on=["a", "b"])
        if demand_data.empty is False:
            print(f"Component {component} contains regional data, processing.")
            demand_data.loc[(demand_data[user_class_speed] < 30), "speed_band"] = (
                "vkm_10-30_kph"
            )
            demand_data.loc[
                ((demand_data[user_class_speed] >= 30) & (demand_data[user_class_speed] < 50)),
                "speed_band",
            ] = "vkm_30-50_kph"
            demand_data.loc[
                ((demand_data[user_class_speed] >= 50) & (demand_data[user_class_speed] < 70)),
                "speed_band",
            ] = "vkm_50-70_kph"
            demand_data.loc[
                ((demand_data[user_class_speed] >= 70) & (demand_data[user_class_speed] < 90)),
                "speed_band",
            ] = "vkm_70-90_kph"
            demand_data.loc[(demand_data[user_class_speed] >= 90), "speed_band"] = (
                "vkm_90-110_kph"
            )
            demand_data = pd.merge(
                demand_data, self.link_road_types, how="left", on=["a", "b"]
            )
            demand_data["abs_demand"] = (
                demand_data["abs_demand"] * demand_data["Distance"] / 1000
            )
            if user_class in ("UC1", "UC2", "UC3"):
                demand_data["abs_demand"] = demand_data["abs_demand"] * 348 * 1.238 * 3
            elif user_class == "UC4":
                demand_data["abs_demand"] = demand_data["abs_demand"] * 329 * 1.216 * 6
            elif user_class == "UC5":
                demand_data["abs_demand"] = demand_data["abs_demand"] * 297 * 1.331 * 3
            else:
                print("Could not apply annualisation factors.")
            demand_data = demand_data[["origin_zone", "speed_band", "abs_demand", "road_type"]]
            demand_data = demand_data.groupby(
                ["origin_zone", "speed_band", "road_type"], as_index=False
            ).sum()
            demand_data = demand_data.pivot_table(
                values="abs_demand", index=["origin_zone", "road_type"], columns="speed_band"
            )
            # for i in [
            #     "vkm_10-30_kph",
            #     "vkm_30-50_kph",
            #     "vkm_50-70_kph",
            #     "vkm_70-90_kph",
            #     "vkm_90-110_kph",
            #     "total_vehicle_km",
            # ]:
            #     if i not in demand_data.columns:
            #         demand_data[i] = 0
            # print(demand_data)
            demand_data["total_vehicle_km"] = (
                demand_data["vkm_10-30_kph"]
                + demand_data["vkm_30-50_kph"]
                + demand_data["vkm_70-90_kph"]
                + demand_data["vkm_90-110_kph"]
                + demand_data["vkm_50-70_kph"]
            )
            demand_data["year"] = "20" + str(year)
            demand_data = demand_data.reset_index()
            demand_data = demand_data[
                [
                    "origin_zone",
                    "road_type",
                    "vkm_10-30_kph",
                    "vkm_30-50_kph",
                    "vkm_50-70_kph",
                    "vkm_70-90_kph",
                    "vkm_90-110_kph",
                    "total_vehicle_km",
                    "year",
                ]
            ]
            print("Dataframe component is processed.")
        return demand_data

    def linear_interpol(self, vehicle_type, year1, year2, target_year):
        """Create data for a particular year and vehicle type using linear interpolation"""
        df1year, df2year, year = year1, year2, target_year
        if vehicle_type == "car":
            df1 = self.car_demand[self.car_demand["year"] == year1].copy()
            df2 = self.car_demand[self.car_demand["year"] == year2].copy()
        elif vehicle_type == "lgv":
            df1 = self.lgv_demand[self.lgv_demand["year"] == year1].copy()
            df2 = self.lgv_demand[self.lgv_demand["year"] == year2].copy()
        else:
            df1 = self.hgv_demand[self.hgv_demand["year"] == year1].copy()
            df2 = self.hgv_demand[self.hgv_demand["year"] == year2].copy()
        target = df1.copy()
        target["year"] = target_year
        for i in [
            "vkm_10-30_kph",
            "vkm_30-50_kph",
            "vkm_50-70_kph",
            "vkm_70-90_kph",
            "vkm_90-110_kph",
            "total_vehicle_km",
        ]:
            a = df2year - year
            b = year - df1year
            target[i] = (a * df1[i] + b * df2[i]) / (a + b)

        if vehicle_type == "car":
            self.car_demand = pd.concat([self.car_demand, target], ignore_index=True)
        elif vehicle_type == "lgv":
            self.lgv_demand = pd.concat([self.lgv_demand, target], ignore_index=True)
        else:
            self.hgv_demand = pd.concat([self.hgv_demand, target], ignore_index=True)

    def interpolate_missing_years(self):
        """Interpolate and add the missing years to the vkm data."""
        for vehicle in ["car", "lgv", "hgv"]:
            self.linear_interpol(vehicle, 2019, 2025, 2018)
            self.linear_interpol(vehicle, 2019, 2025, 2020)

    def spatial_translation(self, dataframe):
        """Interpolate and add the missing years to the vkm data."""
        dataframe = dataframe.rename(columns={"o": "SERTM_id"})  # SERTM_id" "MRTM2_v14_id"
        dataframe = dataframe.merge(self.msoa_rtm_lookup, how="inner", on="SERTM_id")  # "MRTM2_v14_id"

        dataframe["abs_demand"] = dataframe["abs_demand"] * dataframe["SERTM_to_msoa11"]  # "MiHAMv10_to_msoa11cd" "SERTM_to_msoa11"
        dataframe = dataframe[["origin_zone", "a", "b", "abs_demand"]]
        dataframe = dataframe.groupby(["origin_zone", "a", "b"], as_index=False).sum()
        return dataframe

    def output_demand_data(self):
        """Write out the VKM data"""
        for year in self.available_years:
            car_year_segment = self.car_demand[self.car_demand["year"] == "20" + str(year)]
            car_year_segment.to_csv(self.out_path + rf"\vkm_by_speed_and_type_20{year}_car.csv")
            lgv_year_segment = self.lgv_demand[self.lgv_demand["year"] == "20" + str(year)]
            lgv_year_segment.to_csv(self.out_path + rf"\vkm_by_speed_and_type_20{year}_lgv.csv")
            hgv_year_segment = self.hgv_demand[self.hgv_demand["year"] == "20" + str(year)]
            hgv_year_segment.to_csv(self.out_path + rf"\vkm_by_speed_and_type_20{year}_hgv.csv")

    def process_demand(self):
        """Reformat demand into the correct format for CAFCarb."""
        user_classes = ["UC1", "UC2", "UC3", "UC4", "UC5"]  # 'UC1', 'UC2', 'UC3', 'UC4', 'UC5'
        time_period = ["TS1", "TS2", "TS3"]

        for year in self.available_years:
            for time in time_period:
                if year == 19:
                    network_links = pd.read_csv(
                        self.link_data_path
                        + rf"\20{year}\link_table_SE_B19_{time}_net_v020.csv" # rf"\20{year}\link_table_MD2_b19_ass_v049_{time}.csv" "\20{year}\link_table_SE_B19_{time}_net_v020.csv"
                    ).rename(columns={"A": "a", "B": "b"})
                else:
                    network_links = pd.read_csv(
                        self.link_data_path
                        + rf"\20{year}\link_table_SE_DM_FY20{year}_{time}_net_v002.csv"  # + rf"\link_table_MD2_f{year}_ass_v049_{time}.csv" "\20{year}\link_table_SE_DM_FY{year}_{time}_net_v002.csv"
                    ).rename(columns={"A": "a", "B": "b"})
                network_links["a"] = network_links["a"].astype(str)
                network_links["b"] = network_links["b"].astype(str)
                for user_class in user_classes:
                    print(f"Loading demand table for {year} {time} {user_class}")
                    if year == 19:
                        demand_table = pd.DataFrame(
                            pd.read_hdf(
                                self.demand_data_path
                                + rf"\20{year}\SE_B19_{time}_net_v020_SatPig_{user_class}.h5"  # MD2_b19_ass_v049_{time}_SatPig_{user_class}.h5 SE_B19_{time}_net_v020_SatPig_{user_class}.h5"
                            )
                        ).reset_index()
                    else:
                        demand_table = pd.DataFrame(
                            pd.read_hdf(
                                self.demand_data_path
                                + rf"\20{year}\SE_DM_FY20{year}_{time}_net_v002_SatPig_{user_class}.h5"  #  MD2_f{year}_ass_v049_{time}_SatPig_{user_class}.h5 SE_DM_FY20{year}_{time}_net_v002_SatPig_{user_class}.h5
                            )
                        ).reset_index()
                        print(f"Loaded demand table")
                        demand_table["a"] = demand_table["a"].astype(str)
                        demand_table["b"] = demand_table["b"].astype(str)
                    demand_max = demand_table["o"].max()
                    self.component_no = 0
                    for component in batch(range(1, demand_max + 1), 1000):
                        self.component_no = self.component_no + 1
                        demand_component = demand_table[
                            (demand_table["o"] >= component[0])
                            & (demand_table["o"] <= component[-1])
                        ]
                        speed_data = self.assign_speed_band_vkm(
                            network_links, demand_component, user_class, year, component
                        )
                        if speed_data.empty == False:
                            if user_class in ("UC1", "UC2", "UC3"):
                                self.car_demand = pd.concat(
                                    [self.car_demand, speed_data], ignore_index=True
                                )
                                self.car_demand = self.car_demand.groupby(
                                    ["origin_zone", "road_type", "year"], as_index=False
                                ).sum()
                            elif user_class == "UC4":
                                self.lgv_demand = pd.concat(
                                    [self.lgv_demand, speed_data], ignore_index=True
                                )
                                self.lgv_demand = self.lgv_demand.groupby(
                                    ["origin_zone", "road_type", "year"], as_index=False
                                ).sum()
                            elif user_class == "UC5":
                                self.hgv_demand = pd.concat(
                                    [self.hgv_demand, speed_data], ignore_index=True
                                )
                                self.hgv_demand = self.hgv_demand.groupby(
                                    ["origin_zone", "road_type", "year"], as_index=False
                                ).sum()
                            else:
                                print("Could not assign data on user class.")


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]

run_2019 = Demand([19])

