import pandas as pd
import pytest


# # # FIXTURES # # #
@pytest.fixture(scope="module")
def mock_basic_clean():
    ba_cl_data = pd.read_csv(r"E:\GitHub\caf.carbon\tests\data_basic_clean\audit\fleet_archive.csv")
    return ba_cl_data

@pytest.fixture(scope="module")
def mock_advance_clean():
    ad_cl_data = pd.read_csv(r"E:\GitHub\caf.carbon\tests\data_advance_clean\audit\fleet_archive.csv")
    return ad_cl_data

@pytest.fixture(scope="module")
def mock_split_tables():
    split_data = pd.read_csv(r"E:\GitHub\caf.carbon\tests\data_split_tables\audit\index_fleet.csv")
    return split_data