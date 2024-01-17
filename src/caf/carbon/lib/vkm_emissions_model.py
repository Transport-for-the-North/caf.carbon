# Built-Ins
import dataclasses
import os

# Third Party
import pandas as pd


@dataclasses.dataclass
class VkmSplits:
    def __init__(self):
        self.current_directory = os.getcwd()
        self.tag_data = os.path.join(self.current_directory, r'CAFCarb\lookup\tag-data-book-v1.21-may-2023-v1.0.xlsm')
        self.sheet_name = 'A1.3.9'
        self.skiprows = 24
        self.car_cols = "B,D:F"
        self.lgv_cols = "B,G:I"
        self.hgv_cols = "B,J:K"

    def read_data(self):
        return (
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.car_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.lgv_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.hgv_cols)
            )


@dataclasses.dataclass
class FuelParameters:
    def __init__(self):
        current_directory = os.getcwd()
        self.tag_data = os.path.join(current_directory, r'CAFCarb\lookup\tag-data-book-v1.21-may-2023-v1.0.xlsm')
        self.sheet_name = 'A1.3.11'
        self.skiprows = 24
        self.pet_car_cols = "B,D:G"
        self.dies_car_cols = "B,H:K"
        self.elec_car_cols = "B,L:O"

        self.pet_lgv_cols = "B,P:S"
        self.dies_lgv_cols = "B,T:W"
        self.elec_lgv_cols = "B,X:AA"

        self.dies_ogv1_col = "B,AB:AE"
        self.dies_ogv2_col = "B,AF:AI"

    def read_data(self):
        return (
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.pet_car_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.dies_car_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.elec_car_cols),

            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.pet_lgv_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.dies_lgv_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.elec_lgv_cols),

            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.dies_ogv1_col),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.dies_ogv2_col)
        )

@dataclasses.dataclass
class CarbonPerLiter:
    def __init__(self):
        current_directory = os.getcwd()
        self.tag_data = os.path.join(current_directory, r'CAFCarb\lookup\tag-data-book-v1.21-may-2023-v1.0.xlsm')
        self.sheet_name = 'A3.3'
        self.skiprows = 25
        self.pet_cols = "B,D"
        self.dies_cols = "B,E"
        self.elec_cols = "B,G"


    def read_data(self):
        return(
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.pet_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.dies_cols),
            pd.read_excel(self.tag_data, sheet_name=self.sheet_name, skiprows=self.skiprows, usecols=self.elec_cols)
        )

def distance_bands(scenario,year, time, user_class):
    """
    Calculate the vkms and create distance bands
    :param scenario:
    :param year:
    :param time:
    :param user_class:
    :return:
    """
    skim = pd.read_csv(
        fr"Y:\Carbon\QCR_Assignments\03.Assignments\Skims\Merge\Dist_Trip_Merge_{scenario}_{year}_{time}_{user_class}.csv")
    # add in vkm distance bands, calculate vkm as distance * trips,
    # aggregate by trip type zone distance band sum vkm for each, add year column
    skim['check'] = skim['Distance'] * skim['Trips']
    skim = skim.drop(['check'], axis=1)
    skim.loc[(skim['Distance'] >= 0) & (skim['Distance'] < 5), '0-5km'] = skim['Distance'] * skim['Trips']
    skim.loc[(skim['Distance'] >= 5) & (skim['Distance'] < 10), '5-10km'] = skim['Distance'] * skim['Trips']
    skim.loc[(skim['Distance'] >= 10) & (skim['Distance'] < 30), '10-30km'] = skim['Distance'] * skim['Trips']
    skim.loc[(skim['Distance'] >= 30) & (skim['Distance'] < 50), '30-50km'] = skim['Distance'] * skim['Trips']
    skim.loc[(skim['Distance'] >= 50), '50km+'] = skim['Distance'] * skim['Trips']
    skim = skim.fillna({'0-5km': 0, '5-10km': 0, '10-30km': 0, '30-50km': 0, '50km+': 0})
    skim = pd.melt(skim, id_vars=['Origin', 'Destination'], var_name="Distance_band",
                   value_vars=['0-5km', '5-10km', '10-30km', '30-50km', '50km+'], value_name='Tot_VKM')
    skim = skim.groupby(['Origin', 'Destination', 'Distance_band'])['Tot_VKM'].sum().reset_index()
    skim = skim.sort_values(['Origin', 'Destination', 'Distance_band']).reset_index(drop=True)
    skim['Year'] = year
    return skim


def car_hb_nhb_splits(skim, user_class, year, time_hb):

    """
    aggregate NorMITS HB and NHB volumes for each purpose type in a user class to get total volume, match back onto skim
    by origin and destination pairs.

    :param skim:

    :return: skim
    """
    current_directory = os.getcwd()
    hb_splits = pd.DataFrame()
    # select NHB splits by user class and add the matrices
    if user_class == 'UC2': purpose_list = ['p1']
    if user_class == 'UC1': purpose_list = ['p2']
    if user_class == 'UC3': purpose_list = ['p3', 'p4', 'p5', 'p6', 'p7', 'p8']
    for p in purpose_list:
        purpose_to = pd.read_csv(os.path.join(current_directory, f'CAFCarb\input\HB_splits\hb_od_from_yr{year}_{p}_m3_{time_hb}.csv'))
        purpose_to = purpose_to.set_index('Unnamed: 0')
        purpose_from = pd.read_csv(os.path.join(current_directory, f'CAFCarb\input\\HB_splits\hb_od_to_yr{year}_{p}_m3_{time_hb}.csv'))
        purpose_from = purpose_from.set_index('Unnamed: 0')
        hb_splits = hb_splits.add(purpose_to, fill_value=0)
        hb_splits = hb_splits.add(purpose_from, fill_value=0)
    hb_splits = hb_splits.reset_index()
    hb_splits = hb_splits.rename(columns={'Unnamed: 0': 'Origin'})

    # NHB
    nhb_splits = pd.DataFrame()
    # select NHB splits by user class and add the matrices
    if user_class == 'UC1': purpose_list = ['p12']
    if user_class == 'UC3': purpose_list = ['p13', 'p14', 'p15', 'p16', 'p18']
    for p in purpose_list:
        purpose = pd.read_csv(os.path.join(current_directory, fr'CAFCarb\input\HB_splits\nhb_od_yr{year}_{p}_m3_{time_hb}.csv'))
        purpose = purpose.set_index('Unnamed: 0')
        nhb_splits = nhb_splits.add(purpose, fill_value=0)
    nhb_splits = nhb_splits.reset_index()
    nhb_splits = nhb_splits.rename(columns={'Unnamed: 0': 'Origin'})

    # get the column names
    range_zones = range(1,2271)
    range_zones = [str(x) for x in range_zones]
    # change to long format, change destination to numeric
    hb_splits = pd.melt(hb_splits, id_vars=['Origin'], var_name="Destination", value_vars=range_zones, value_name='HB')
    hb_splits["Destination"] = pd.to_numeric(hb_splits["Destination"])
    nhb_splits = pd.melt(nhb_splits, id_vars=['Origin'], var_name="Destination", value_vars=range_zones, value_name='NHB')
    nhb_splits["Destination"] = pd.to_numeric(nhb_splits["Destination"])
    # match to skim data
    skim = skim.merge(hb_splits, on=['Origin', 'Destination'])
    skim = skim.merge(nhb_splits, on=['Origin', 'Destination'])
    return skim

def car_fuel_consumption(skim, car_vkm_split, pet_car_fuel, dies_car_fuel, elec_car_fuel, user_class, year, time_hb):
    """
    Apply homebased and non-hombased splits for car, find fuel consumption per (kWh/km) electric and (l/km) petrol and diesel

    :param skim:
    :param car_vkm_split:
    :param pet_car_fuel:
    :param dies_car_fuel:
    :param elec_car_fuel:
    :param user_class:
    :param year:
    :param time_hb:
    :return:
    """
    # apply the hb/nhb split function, find the volume ration between the two, melt into long form
    # user class 2 is completely homebased
    if (user_class == 'UC1') | (user_class == 'UC3'):
        skim = car_hb_nhb_splits(skim, user_class, year, time_hb)
        skim['HB_ratio'] = skim['HB'] / (skim['HB'] + skim['NHB'])
        skim['NHB_ratio'] = skim['NHB'] / (skim['HB'] + skim['NHB'])
        skim = skim.drop(['HB', 'NHB'], axis=1).rename(columns={'HB_ratio': 'HB', 'NHB_ratio': 'NHB'})
    else:
        skim['HB'], skim['NHB'] = 1, 0
    skim = pd.melt(skim, id_vars=['Origin', 'Destination', 'Distance_band', 'Year', 'Tot_VKM'],var_name='HB/NHB', value_vars=['HB', 'NHB'], value_name='HB/NHB ratio')

    # select A1.3.9 year, combine with skim, multiply vkms by the split, drop calculation rows
    car_tag_vkm_split_year = car_vkm_split.loc[car_vkm_split['Year'] == year]
    skim = skim.merge(car_tag_vkm_split_year, on='Year')
    skim['Petrol'], skim['Diesel'], skim['Electric'] = skim['Petrol'] * skim['Tot_VKM'] * skim['HB/NHB ratio'], skim['Diesel'] * skim['Tot_VKM'] * skim['HB/NHB ratio'], skim['Electric'] * skim['Tot_VKM'] * skim['HB/NHB ratio']
    skim = pd.melt(skim, id_vars=['Origin', 'Destination', 'Distance_band', 'Year', 'HB/NHB'],  var_name="Fuel_type",  value_vars=['Petrol', 'Diesel', 'Electric'], value_name='VKM')

    # set true mean for each vehicle type add vehicle type and user type
    skim['Average_speed'] = sum(range(10, 131)) / len(range(10, 131))
    skim['Vehicle_type'] = 'car'
    skim['User_class'] = user_class

    # select year of fuel consumption for petrol, diesel and electric, fill empty in electric with 0
    pet_car_fuel_year = pet_car_fuel.loc[pet_car_fuel['Year'] == year]
    dies_car_fuel_year = dies_car_fuel.loc[dies_car_fuel['Year'] == year]
    elec_car_fuel_year = elec_car_fuel.loc[elec_car_fuel['Year'] == year].fillna(0)

    # find fuel consumption (L = a/v + b + c.v + d.v2), zero empty columns as electric (kWh/km) and petrol and diesel (l/km)
    skim.loc[skim['Fuel_type'] == 'Petrol', 'fuel consumption (l/km)'] = (pet_car_fuel_year['a'].loc[pet_car_fuel_year.index[0]]/ skim['Average_speed']) + (pet_car_fuel_year['b'].loc[pet_car_fuel_year.index[0]]) + (pet_car_fuel_year['c'].loc[pet_car_fuel_year.index[0]] * skim['Average_speed']) + (pet_car_fuel_year['d'].loc[pet_car_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim.loc[skim['Fuel_type'] == 'Diesel', 'fuel consumption (l/km)'] = (dies_car_fuel_year['a.1'].loc[dies_car_fuel_year.index[0]]/ skim['Average_speed']) + (dies_car_fuel_year['b.1'].loc[dies_car_fuel_year.index[0]]) + (dies_car_fuel_year['c.1'].loc[dies_car_fuel_year.index[0]] * skim['Average_speed']) + (dies_car_fuel_year['d.1'].loc[dies_car_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim.loc[skim['Fuel_type'] == 'Electric', 'fuel consumption (kWh/km)'] = (elec_car_fuel_year['a.2'].loc[elec_car_fuel_year.index[0]]/ skim['Average_speed']) + (elec_car_fuel_year['b.2'].loc[elec_car_fuel_year.index[0]]) + (elec_car_fuel_year['c.2'].loc[elec_car_fuel_year.index[0]] * skim['Average_speed']) + (elec_car_fuel_year['d.2'].loc[elec_car_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim = skim.fillna({'fuel consumption (kWh/km)': 0, 'fuel consumption (l/km)': 0})
    return skim

def lgv_fuel_consumption(skim, lgv_vkm_split, pet_lgv_fuel, dies_lgv_fuel, elec_lgv_fuel, user_class, year):
    """
    Apply homebased and non-hombased splits for lgv, find fuel consumption per (kWh/km) electric and (l/km) petrol and diesel

    :param skim:
    :param lgv_vkm_split:
    :param pet_lgv_fuel:
    :param dies_lgv_fuel:
    :param elec_lgv_fuel:
    :param user_class:
    :param year:
    :return:
    """
    # apply the hb/nhb split function, find the volume ration between the two, melt into long form
    skim['HB'], skim['NHB'] = 0, 1
    skim = pd.melt(skim, id_vars=['Origin', 'Destination', 'Distance_band', 'Year', 'Tot_VKM'], var_name='HB/NHB',value_vars=['HB', 'NHB'], value_name='HB/NHB ratio')

    # select A1.3.9 year, combine with skim, multiply vkms by the split, drop calculation rows
    lgv_tag_vkm_split_year = lgv_vkm_split.loc[lgv_vkm_split['Year'] == year]
    skim = skim.merge(lgv_tag_vkm_split_year, on='Year')
    skim = skim.rename(columns={'Petrol.1': 'Petrol', 'Diesel.1': 'Diesel', 'Electric.1': 'Electric'})
    skim['Petrol'], skim['Diesel'], skim['Electric'] = skim['Petrol'] * skim['Tot_VKM'] * skim['HB/NHB ratio'], skim['Diesel'] * skim['Tot_VKM'] * skim['HB/NHB ratio'], skim['Electric'] * skim['Tot_VKM'] * skim['HB/NHB ratio']
    skim = pd.melt(skim, id_vars=['Origin', 'Destination', 'Distance_band', 'Year', 'HB/NHB'],  var_name="Fuel_type", value_vars=['Petrol', 'Diesel', 'Electric'], value_name='VKM')

    # set true mean for each vehicle type add vehicle type and user type Note:lgv - diesel and petrol sperate max speeds
    skim.loc[skim['Fuel_type'] == 'Petrol', 'Average_speed'] = sum(range(10, 121)) / len(range(10, 121))
    skim.loc[skim['Fuel_type'] == 'Electric', 'Average_speed'] = sum(range(10, 121)) / len(range(10, 121))
    skim.loc[skim['Fuel_type'] == 'Diesel', 'Average_speed'] = sum(range(10, 111)) / len(range(10, 111))
    skim['Vehicle_type'] = 'lgv'
    skim['User_class'] = user_class

    # select year of fuel consumption for petrol, diesel and electric, fill empty in electric with 0
    pet_lgv_fuel_year = pet_lgv_fuel.loc[pet_lgv_fuel['Year'] == year]
    dies_lgv_fuel_year = dies_lgv_fuel.loc[dies_lgv_fuel['Year'] == year]
    elec_lgv_fuel_year = elec_lgv_fuel.loc[elec_lgv_fuel['Year'] == year].fillna(0)

    # find fuel consumption (L = a/v + b + c.v + d.v2), zero empty columns as electric (kWh/km) and petrol and diesel (l/km)
    skim.loc[skim['Fuel_type'] == 'Petrol', 'fuel consumption (l/km)'] = (pet_lgv_fuel_year['a.3'].loc[pet_lgv_fuel_year.index[0]]/ skim['Average_speed']) + (pet_lgv_fuel_year['b.3'].loc[pet_lgv_fuel_year.index[0]]) + (pet_lgv_fuel_year['c.3'].loc[pet_lgv_fuel_year.index[0]] * skim['Average_speed']) + (pet_lgv_fuel_year['d.3'].loc[pet_lgv_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim.loc[skim['Fuel_type'] == 'Diesel', 'fuel consumption (l/km)'] = (dies_lgv_fuel_year['a.4'].loc[dies_lgv_fuel_year.index[0]]/ skim['Average_speed']) + (dies_lgv_fuel_year['b.4'].loc[dies_lgv_fuel_year.index[0]]) + (dies_lgv_fuel_year['c.4'].loc[dies_lgv_fuel_year.index[0]] * skim['Average_speed']) + (dies_lgv_fuel_year['d.4'].loc[dies_lgv_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim.loc[skim['Fuel_type'] == 'Electric', 'fuel consumption (kWh/km)'] = (elec_lgv_fuel_year['a.5'].loc[elec_lgv_fuel_year.index[0]]/ skim['Average_speed']) + (elec_lgv_fuel_year['b.5'].loc[elec_lgv_fuel_year.index[0]]) + (elec_lgv_fuel_year['c.5'].loc[elec_lgv_fuel_year.index[0]] * skim['Average_speed']) + (elec_lgv_fuel_year['d.5'].loc[elec_lgv_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim = skim.fillna({'fuel consumption (kWh/km)': 0, 'fuel consumption (l/km)': 0})
    return skim

def hgv_fuel_consumption(skim, hgv_vkm_split, dies_hgv_ogv1_fuel, dies_hgv_ogv2_fuel, user_class, hgv_ogv1_split, hgv_ogv2_split, year):
    """
    Apply homebased and non-hombased splits for hgv, find fuel consumption per (kWh/km) electric and (l/km) petrol and diesel
    :param skim:
    :param hgv_vkm_split:
    :param dies_hgv_ogv1_fuel:
    :param dies_hgv_ogv2_fuel:
    :param user_class:
    :param hgv_ogv1_split:
    :param hgv_ogv2_split:
    :return: skim
    """

    # apply the hb/nhb split function, find the volume ration between the two, melt into long form
    skim['HB'], skim['NHB'] = 0, 1
    skim = pd.melt(skim, id_vars=['Origin', 'Destination', 'Distance_band', 'Year', 'Tot_VKM'], var_name='HB/NHB',value_vars=['HB', 'NHB'], value_name='HB/NHB ratio')

    # select A1.3.9 year, combine with skim, multiply vkms by the split, drop calculation rows
    hgv_tag_vkm_split_year = hgv_vkm_split.loc[hgv_vkm_split['Year'] == year]
    skim = skim.merge(hgv_tag_vkm_split_year, on='Year')

    # drop electric as all hgv assumed to be diesel
    skim = skim.rename(columns={'Electric.2': 'Electric', 'Diesel.2': 'Diesel'})
    skim = skim[['Origin', 'Distance_band', 'Tot_VKM', 'Year',  'Diesel', 'HB/NHB', 'HB/NHB ratio']]
    skim['Diesel'] = skim['Diesel'] * skim['Tot_VKM'] * skim['HB/NHB ratio']
    skim = pd.melt(skim, id_vars=['Origin', 'Distance_band', 'Year', 'HB/NHB'],  var_name="Fuel_type", value_vars=['Diesel'], value_name='TVKM')

    # split vkm by ogv1 and ogv2 proportions
    skim['ogv1'] = hgv_ogv1_split
    skim['ogv2'] = hgv_ogv2_split
    skim['ogv1'], skim['ogv2'] = skim['ogv1'] * skim['TVKM'], skim['ogv2'] * skim['TVKM']
    skim = pd.melt(skim, id_vars=['Origin', 'Distance_band', 'Year', 'HB/NHB', 'Fuel_type'], var_name="Vehicle_type", value_vars=['ogv1', 'ogv2'], value_name='VKM')

    # set true mean for each vehicle type add vehicle type and user type
    skim['Average_speed'] = sum(range(12, 86)) / len(range(12, 86))
    skim['User_class'] = user_class

    # select year of fuel consumption for petrol, diesel and electric, fill empty in electric with 0 Note: only diesel for hgv
    dies_hgv_ogv1_fuel_year = dies_hgv_ogv1_fuel.loc[dies_hgv_ogv1_fuel['Year'] == year]
    dies_hgv_ogv2_fuel_year = dies_hgv_ogv2_fuel.loc[dies_hgv_ogv2_fuel['Year'] == year]
    # find fuel consumption (L = a/v + b + c.v + d.v2), zero empty columns as electric (kWh/km) and petrol and diesel (l/km)
    skim.loc[skim['Vehicle_type'] == 'ogv1', 'fuel consumption (l/km)'] = (dies_hgv_ogv1_fuel_year['a.6'].loc[dies_hgv_ogv1_fuel_year.index[0]]/ skim['Average_speed']) + (dies_hgv_ogv1_fuel_year['b.6'].loc[dies_hgv_ogv1_fuel_year.index[0]]) + (dies_hgv_ogv1_fuel_year['c.6'].loc[dies_hgv_ogv1_fuel_year.index[0]] * skim['Average_speed']) + (dies_hgv_ogv1_fuel_year['d.6'].loc[dies_hgv_ogv1_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim.loc[skim['Vehicle_type'] == 'ogv2', 'fuel consumption (l/km)'] = (dies_hgv_ogv2_fuel_year['a.7'].loc[dies_hgv_ogv2_fuel_year.index[0]]/ skim['Average_speed']) + (dies_hgv_ogv2_fuel_year['b.7'].loc[dies_hgv_ogv2_fuel_year.index[0]]) + (dies_hgv_ogv2_fuel_year['c.7'].loc[dies_hgv_ogv2_fuel_year.index[0]] * skim['Average_speed']) + (dies_hgv_ogv2_fuel_year['d.7'].loc[dies_hgv_ogv2_fuel_year.index[0]] * (skim['Average_speed']**2))
    skim['fuel consumption (kWh/km)'] = 0
    skim = skim.fillna({'fuel consumption (l/km)': 0})
    return skim

def calculate_co2_emissions(skim, year, pet_carbon, dies_carbon, elec_carbon, time, combined_year):
    """
    calculate co2 emissions, combine and save to the year dataframe
    :param skim:
    :param year: year of skim
    :param pet_carbon: tag dataset carbon dioxide emissions per litre of fuel petrol
    :param dies_carbon: tag dataset carbon dioxide emissions per litre of fuel diesel
    :param elec_carbon:  tag dataset carbon dioxide emissions per litre of fuel electric
    :param time: time period of skim
    :param combined_year:
    :return: skim concatenated into combined year

    """
    # select year for carbon dioxide emissions per litre of fuel
    pet_carbon_year = pet_carbon.loc[pet_carbon['Year'] == year]
    dies_carbon_year = dies_carbon.loc[dies_carbon['Year'] == year]
    elec_carbon_year = elec_carbon.loc[elec_carbon['Year'] == year]

    # make carbon dioxide emissions per litre/ kwh equal tag value for given year, make empty rows equal zero
    skim.loc[skim['Fuel_type'] == 'Petrol', 'emissions (Kg CO2/l)'] = pet_carbon_year['Kg CO2e/l'].loc[pet_carbon_year.index[0]]
    skim.loc[skim['Fuel_type'] == 'Diesel', 'emissions (Kg CO2/l)'] = dies_carbon_year['Kg CO2e/l.1'].loc[dies_carbon_year.index[0]]
    skim.loc[skim['Fuel_type'] == 'Electric', 'emissions (Kg CO2e/kWh)'] = elec_carbon_year['Kg CO2e/kWh'].loc[elec_carbon_year.index[0]]
    skim['emissions (Kg CO2/l)'] = skim['emissions (Kg CO2/l)'].fillna(0)
    skim['emissions (Kg CO2e/kWh)'] = skim['emissions (Kg CO2e/kWh)'].fillna(0)

    # calculate emissions fuel consumption (l/km) * Fuel_VKM * emissions (Kg CO2/l for non-electric, Kg CO2e/kWh for electric
    skim.loc[skim['Fuel_type'] == 'Electric', "Total C02 emissions (Kg C02)"] = skim['fuel consumption (kWh/km)'] * skim['VKM'] * skim['emissions (Kg CO2e/kWh)']
    skim.loc[(skim['Fuel_type'] == 'Petrol') | (skim['Fuel_type'] == 'Diesel'), "Total C02 emissions (Kg C02)"] = skim['fuel consumption (l/km)'] * skim['VKM'] * skim['emissions (Kg CO2/l)']

    # add time period, select columns, sort, combine and save
    skim['Time'] = time
    skim = skim.groupby(['Origin', 'Distance_band', 'Year', 'Fuel_type', 'HB/NHB', 'Vehicle_type', 'User_class','Time'])[['VKM', 'Total C02 emissions (Kg C02)']].sum().reset_index()
    skim = skim[['Origin', 'Distance_band', 'Year', 'Fuel_type', 'HB/NHB', 'VKM', 'Vehicle_type', 'User_class', 'Total C02 emissions (Kg C02)','Time']]
    combined_year = pd.concat([combined_year, skim])
    return combined_year



def lininterpol(df1, df2, year1, year2, targetyear, current_directory, scenario):
    """
    Linear interpolation of 2 years to get dataframe for target year
    :param df1: Skim year of dataframe 1
    :param df2: Skim year of dataframe 2
    :param year1: year of dataframe 1
    :param year2: year of dataframe 2
    :param targetyear: target year of interpolation
    :param scenario: travel scenario
    :return: target year dataframe
    """

    df1year = year1
    df2year = year2
    year = targetyear
    target = df1.copy()
    for i in ["VKM", "Total C02 emissions (Kg C02)"]:
        target[i] = -1*((df2year - year)*df1[i] +(year - df1year)*df2[i])/(df1year-df2year)
    target["Year"] = targetyear
    target = target[['Origin', 'Destination', 'Distance_band',	'Year',	'Fuel_type', 'VKM', 'Vehicle_type',	'User_class', 'Total C02 emissions (Kg C02)', 'Time']]
    target.to_csv(os.path.join(current_directory, f'CAFCarb/outputs/{year}_{scenario}.csv'))
    return target

def interpolate_to_input(current_directory):

    """
    Read created files for each scenario, linear interpolation to get outputs for model input years
    """
    # TODO replace with scenarios from logging_main
    scenarios = ['core', 'high', 'low']
    for scenario in scenarios:

        year_2028 = pd.read_csv(os.path.join(current_directory, f'CAFCarb/outputs/2028_{scenario}.csv'))
        year_2038 = pd.read_csv(os.path.join(current_directory, f'CAFCarb/outputs/2038_{scenario}.csv'))
        year_2043 = pd.read_csv(os.path.join(current_directory, f'CAFCarb/outputs/2043_{scenario}.csv'))
        year_2048 = pd.read_csv(os.path.join(current_directory, f'CAFCarb/outputs/2048_{scenario}.csv'))
        year_2018 = pd.read_csv(os.path.join(current_directory, f'CAFCarb/outputs/2018_base.csv'))

        years = [2020, 2025]
        for year in years:
            interpolation = lininterpol(year_2018, year_2028, 2018, 2028, year, current_directory)
        years = [2030, 2035]
        for year in years:
            interpolation = lininterpol(year_2028, year_2038, 2028, 2038, year, current_directory)
        years = [2040]
        for year in years:
            interpolation = lininterpol(year_2038, year_2043, 2038, 2043, year, current_directory)
        years = [2045, 2050]
        for year in years:
            interpolation = lininterpol(year_2043, year_2048, 2043, 2048, year, current_directory)

class VkmEmissionsModel:
    def __init__(self, regions, scenarios):

        # to choose scenarios to run data from
        self.scenarios = scenarios

        #to choose regions for the tool to be run in
        self.regions = regions

        # read in tag data
        current_directory = os.getcwd()

        vkm = VkmSplits()
        car_vkm_split, lgv_vkm_split, hgv_vkm_split = vkm.read_data()

        fuel = FuelParameters()
        pet_car_fuel, dies_car_fuel, elec_car_fuel, pet_lgv_fuel, dies_lgv_fuel, elec_lgv_fuel, dies_hgv_ogv1_fuel, dies_hgv_ogv2_fuel = fuel.read_data()

        carbon = CarbonPerLiter()
        pet_carbon, dies_carbon, elec_carbon = carbon.read_data()

        years = [2018, 2028, 2038, 2043, 2048]
        user_classes = ['UC1', 'UC2', 'UC3', 'UC4', 'UC5']
        time_periods = ['TS1', 'TS2', 'TS3']
        time_periods_hb = ['TP1', 'TP2', 'TP3']


        for year in years:
            if year == 2018:
                scenarios = ['base']
            else:
                scenarios = ['core', 'high', 'low']
            for scenario in scenarios:
                # create an empty data frame for each year.
                combined_year = pd.DataFrame()
                for user_class in user_classes:
                    for time, time_hb in zip(time_periods, time_periods_hb):
                        skim = distance_bands(scenario, year, time, user_class)
                        # HGV- proportion of VKM ogv1 and ogv2 for calculation -Note assumed for now to be 50/50
                        hgv_ogv1_split, hgv_ogv2_split = 0.5, 0.5
                        if (user_class == 'UC1') | (user_class == 'UC2') | (user_class == 'UC3'):
                            skim = car_fuel_consumption(skim, car_vkm_split, pet_car_fuel, dies_car_fuel, elec_car_fuel, user_class, year, time_hb)
                            print(user_class)
                        elif user_class == 'UC4':
                            skim = lgv_fuel_consumption(skim, lgv_vkm_split, pet_lgv_fuel, dies_lgv_fuel, elec_lgv_fuel, user_class, year)
                            print(user_class)
                        elif user_class == 'UC5':
                            skim = hgv_fuel_consumption(skim, hgv_vkm_split, dies_hgv_ogv1_fuel, dies_hgv_ogv2_fuel, user_class, hgv_ogv1_split, hgv_ogv2_split, year)
                            print(user_class)
                        combined_year = calculate_co2_emissions(skim, year, pet_carbon, dies_carbon, elec_carbon, time, combined_year)
                combined_year = combined_year.sort_values(['Origin', 'Distance_band', 'Year', 'Fuel_type', 'HB/NHB', 'Vehicle_type', 'User_class', 'Time']).reset_index(drop=True)
                combined_year.to_csv(os.path.join(current_directory, f'CAFCarb/outputs/{year}_{scenario}.csv'))
        interpolate_to_input(current_directory)

