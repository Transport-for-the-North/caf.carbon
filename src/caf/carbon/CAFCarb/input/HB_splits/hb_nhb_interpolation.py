import pandas as pd

def lininterpol(df1, df2, year1, year2, targetyear):
   df1year = year1
   df2year = year2
   year = targetyear
   target = df1.copy()
   for column in target:
      target[column] = -1 * ((df2year - year) * df1[column] + (year - df1year) * df2[column]) / (df1year - df2year)
   return target


#interpolate nb/nhb matrices for skim data input years for the vehicle emissions tool
# HB FROM
for time_hb in ['TP1', 'TP2', 'TP3']:
    for p in ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8']:
       p2018 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_from_yr2018_{p}_m3_{time_hb}.csv")
       p2033 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_from_yr2033_{p}_m3_{time_hb}.csv")
       p2040 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_from_yr2040_{p}_m3_{time_hb}.csv")
       p2050 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_from_yr2050_{p}_m3_{time_hb}.csv")

       p2028 = lininterpol(p2018, p2033, 2018, 2033, 2028)
       p2038 = lininterpol(p2033, p2040, 2033, 2040, 2038)
       p2043 = lininterpol(p2040, p2050, 2040, 2050, 2043)
       p2048 = lininterpol(p2040, p2050, 2040, 2050, 2048)

       p2018.to_csv(fr'F:\VKM_emissions_tool\hb_od_from_yr2018_{p}_m3_{time_hb}.csv')
       p2028.to_csv(fr'F:\VKM_emissions_tool\hb_od_from_yr2028_{p}_m3_{time_hb}.csv')
       p2038.to_csv(fr'F:\VKM_emissions_tool\hb_od_from_yr2038_{p}_m3_{time_hb}.csv')
       p2043.to_csv(fr'F:\VKM_emissions_tool\hb_od_from_yr2043_{p}_m3_{time_hb}.csv')
       p2048.to_csv(fr'F:\VKM_emissions_tool\hb_od_from_yr2048_{p}_m3_{time_hb}.csv')

# HB TO
for time_hb in ['TP1', 'TP2', 'TP3']:
    for p in ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8']:
       p2018 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_to_yr2018_{p}_m3_{time_hb}.csv")
       p2033 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_to_yr2033_{p}_m3_{time_hb}.csv")
       p2040 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_to_yr2040_{p}_m3_{time_hb}.csv")
       p2050 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\hb_od_to_yr2050_{p}_m3_{time_hb}.csv")

       p2028 = lininterpol(p2018, p2033, 2018, 2033, 2028)
       p2038 = lininterpol(p2033, p2040, 2033, 2040, 2038)
       p2043 = lininterpol(p2040, p2050, 2040, 2050, 2043)
       p2048 = lininterpol(p2040, p2050, 2040, 2050, 2048)

       p2018.to_csv(fr'F:\VKM_emissions_tool\hb_od_to_yr2018_{p}_m3_{time_hb}.csv')
       p2028.to_csv(fr'F:\VKM_emissions_tool\hb_od_to_yr2028_{p}_m3_{time_hb}.csv')
       p2038.to_csv(fr'F:\VKM_emissions_tool\hb_od_to_yr2038_{p}_m3_{time_hb}.csv')
       p2043.to_csv(fr'F:\VKM_emissions_tool\hb_od_to_yr2043_{p}_m3_{time_hb}.csv')
       p2048.to_csv(fr'F:\VKM_emissions_tool\hb_od_to_yr2048_{p}_m3_{time_hb}.csv')

# NHB
for time_hb in ['TP1', 'TP2', 'TP3']:
    for p in [ 'p12', 'p13', 'p14', 'p15', 'p16', 'p18']:
       p2018 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\nhb_od_yr2018_{p}_m3_{time_hb}.csv")
       p2033 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\nhb_od_yr2033_{p}_m3_{time_hb}.csv")
       p2040 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\nhb_od_yr2040_{p}_m3_{time_hb}.csv")
       p2050 = pd.read_csv(fr"I:\NorMITs Demand\noham\EFS\iter3i\NTEM\Matrices\OD Matrices\nhb_od_yr2050_{p}_m3_{time_hb}.csv")

       p2028 = lininterpol(p2018, p2033, 2018, 2033, 2028)
       p2038 = lininterpol(p2033, p2040, 2033, 2040, 2038)
       p2043 = lininterpol(p2040, p2050, 2040, 2050, 2043)
       p2048 = lininterpol(p2040, p2050, 2040, 2050, 2048)

       p2018.to_csv(fr'F:\VKM_emissions_tool\nhb_od_yr2018_{p}_m3_{time_hb}.csv')
       p2028.to_csv(fr'F:\VKM_emissions_tool\nhb_od_yr2028_{p}_m3_{time_hb}.csv')
       p2038.to_csv(fr'F:\VKM_emissions_tool\nhb_od_yr2038_{p}_m3_{time_hb}.csv')
       p2043.to_csv(fr'F:\VKM_emissions_tool\nhb_od_yr2043_{p}_m3_{time_hb}.csv')
       p2048.to_csv(fr'F:\VKM_emissions_tool\nhb_od_yr2048_{p}_m3_{time_hb}.csv')




