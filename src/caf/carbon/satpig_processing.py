# Built-Ins
import os

# Third Party
import numpy as np
import pandas as pd


def select_oa(lta, link_oa):
    # select the output area from the link_oa file and split id into a and b
    link_oa = link_oa.loc[link_oa['LTB_id'] == lta]
    link_oa[['a', 'b']] = link_oa['link_id'].str.split('_', expand=True)
    return link_oa


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def assign_movement(route_table, link_oa_selected, lta, link_attributes):
    # chunk the dataset to avoid memory issues, split by origin to ensure grouping and counting by o,d,route below correct
    processed = pd.DataFrame()
    chunk_max = route_table['o'].max()
    for x in batch(range(1, chunk_max + 1), 500):
        chunk = route_table[(route_table['o'] >= x[0]) & (route_table['o'] <= x[-1])]
        # route table to link_oa, count number of order
        chunk = chunk.merge(link_oa_selected, on=['a', 'b'], how='left').drop(
            columns=['link_id', 'LTB_to_link', 'link_to_LTB'])
        # count number of links in route, mark first final link in the route within LTA and count that are within the LTA
        chunk['LTB_id'] = chunk['LTB_id'].fillna('non-lta')
        chunk['link_count'] = chunk.groupby(['o', 'd', 'route'])['order'].transform('count')
        chunk['first_link'] = chunk.groupby(['o', 'd', 'route'])['LTB_id'].transform('first')
        chunk['last_link'] = chunk.groupby(['o', 'd', 'route'])['LTB_id'].transform('last')
        chunk['LTB_id'] = chunk['LTB_id'].astype(str)
        chunk['oa_count'] = chunk.groupby(['o', 'd', 'route'])['LTB_id'].transform(lambda x: x[x.str.contains(lta)].count())

        # define movement pattern
        chunk['movement'] = ''
        chunk['movement'] = np.where((chunk.first_link == lta) & (chunk.last_link == lta), 'within', chunk.movement)
        chunk['movement'] = np.where((chunk.first_link != lta) & (chunk.last_link != lta) & (chunk.oa_count > 0),'through', chunk.movement)
        chunk['movement'] = np.where((chunk.first_link == lta) & (chunk.last_link != lta), 'from', chunk.movement)
        chunk['movement'] = np.where((chunk.first_link != lta) & (chunk.last_link == lta), 'to', chunk.movement)
        chunk['movement'] = np.where(chunk.movement == '', 'outside', chunk.movement)

        # get distance and trips
        chunk = calculate_vkms(chunk, link_attributes)
        processed = pd.concat([processed,chunk])
    return processed

def calculate_vkms(chunk, link_attributes):
    # merge with link attributes to get distance, then groupby
    chunk['a'], chunk['b'] = chunk['a'].astype(int), chunk['b'].astype(int)
    link_attributes['a'], link_attributes['b'] = link_attributes['a'].astype(int), link_attributes['b'].astype(int)
    chunk = chunk.merge(link_attributes, on=['a', 'b'], how='left')
    chunk = chunk.groupby(['o', 'd', 'LTB_id', 'route', 'movement']).agg({'abs_demand': 'sum', 'Distance_m': 'sum'})
    chunk = chunk.rename(columns={'abs_demand': 'Trips', 'Distance_m': 'Distance'})
    return chunk


def main():
    current_directory = os.getcwd()
    lta = "Lancashire"
    years = [2018]
    user_classes = ['UC1', 'UC2', 'UC3', 'UC4', 'UC5']
    time_period = ['TS1']

    for year in years:
        if year == 2018:
            scenarios = ['base']
        else:
            scenarios = ['core', 'high', 'low']
        for scenario in scenarios:
            for user_class in user_classes:
                for time in time_period:

                    link_oa = pd.read_csv(r"Y:\Carbon\QCR_Assignments\07.Noham_to_NoCarb\Link_OA_lookups\2018\link_LTB_spatial.csv").drop(['Unnamed: 0'],axis=1)
                    if year == 2018:
                        route_table = pd.DataFrame(pd.read_hdf(fr"G:\raw_data\4019 - road OD flows\Satpig\QCR\2018\RotherhamBase_i8c_2018_{time}_v107_SatPig_{user_class}.h5")).reset_index()
                    else:
                        route_table = pd.DataFrame(pd.read_hdf(fr"G:\raw_data\4019 - road OD flows\Satpig\QCR\{year}\{scenario}\NoHAM_QCR_DM_{scenario}_{year}_{time}_v107_SatPig_{user_class}.h5")).reset_index()

                    link_attributes = pd.read_csv(r"Y:\Carbon\QCR_Assignments\07.Noham_to_NoCarb\P1XDump\Link_Data_2018_TS1.csv", usecols=['A', 'B', 'Distance_m']).rename(columns={'A': 'a', 'B':'b'})

                    link_oa_selected = select_oa(lta, link_oa)
                    route_table = assign_movement(route_table, link_oa_selected, lta, link_attributes)
                    route_table.to_csv(os.path.join(current_directory, f'satpig_processing_output\{scenario}_{year}_{time}_{user_class}.csv'))

if __name__ == '__main__':
    main()
