import configparser as cf
from caf.carbon import rail_calculations

# %% Load config file

config = cf.ConfigParser(interpolation=cf.ExtendedInterpolation())

rail_outpath = r"E:\GitHub\caf.carbon\NoCarb\Outputs\NPR_Bradford_DS/"

rail_outputs = True

if rail_outputs:
    rail = rail_calculations.Rail(rail_outpath)
