import pandas as pd
import h5py

on_off = pd.read_csv(r'E:\GitHub\caf.carbon\NoCarb\Inputs\NoRMS\UCT_2052\UCT_OP_On_Offs_WithCrdData.csv')
norms_lookup = pd.read_csv(r'A:\caf.carbon\CAFCarb data inputs\NoRMS\emission_profiles\NoRMSVehTypesLookup.csv',encoding='latin-1')
veh_dict = pd.read_csv(r'A:\caf.carbon\CAFCarb data inputs\NoRMS\emission_profiles\vehicles_dict.csv')

combined = on_off.merge(norms_lookup, how='left', left_on='VEHTYPEID', right_on='NoRMS_vehtype')
combined.rename(columns={'Total carriages':'VCARRIAGES'}, inplace=True)
combined.drop(columns=['power', 'VEHTYPEID', 'NoRMS_vehtype'], inplace=True)

final = combined.merge(veh_dict, how='left', left_on=['VEHCLASS'], right_on=['VCLASS'])
final.rename(columns={'vehicle type':'VTYPE'}, inplace=True)
final.drop(columns=['VEHCLASS'], inplace=True)

print(final.columns)
print(final.head(10))
final.to_csv('UCT_2052_OP_On_Offs_NoCarb.csv')




