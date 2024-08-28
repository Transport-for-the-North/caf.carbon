import pandas as pd


fleet_archive = pd.read_csv(r'A:\caf.carbon\CAFCarb data inputs\fleet\2023 fleet data\TFNScan_Feb24.csv')
pd.set_option('display.max_columns', None)
import numpy as np
print(np.quantile([1200,1500],0.25))
print(fleet_archive.head(20))