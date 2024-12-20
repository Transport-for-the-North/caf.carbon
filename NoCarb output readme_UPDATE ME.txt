NoCarb output ReadMe file - last amended 26 April 2021

This file provides relevant information to support the use and understanding of 
NoCarb output files. The first section provides a description of each output
file, and the second provides a glossary of key terms.

########################

2. Glossary:

- Chainage: Vehicle kilometres travelled
- Cohort: The year that a vehicle was registered (year - CYA)
- CYA: The age of a vehicle (year - cohort)
- MTCO2: Megatonnes of carbon dioxide emissions 
- NELUM area type: The area type of a given NELUM zone:
		----> 1: Urban
		----> 2: Sub-urban
		----> 3: Rural
- North: Whether a zone is internal or external of TfN's Northern boundary
- Scenario: One of TfN's Future Travel Scenarios:
		----> SC01: Just About Managing
		----> SC02: Prioritised Places
		----> SC03: Digitally Distributed
		----> SC04: Urban Zero Carbon
- Scenario years: The year milestones used to derive and predict emissions:
2018, 2020, 2025, 2030, 2035, 2040, 2045, 2050. Car demand from NELUM is produced for these 
years and most of the outputs only relate to these years.
- Segment: Vehicle type/size specific to a vehicle type (e.g. 'mini' car) 
- Tally: Number of registered vehicles (cars, vans and HGVs)
- Decarbonisation trajectory: TfN's set target for future CO2 emissions
- Vehicle type: Whether a vehicle is a car, LGV (van) or HGV (heavy goods vehicle)
- Zone: The zones currently relate to TfN's modelling zones, as used by the Northern
Economy and Land Use Model (NELUM).

########################

2. Description of the output files:

- {}_fleet_emissions: Provides disaggregated figures (by year, zone, cohort, 
vehicle type, segment and fuel) for gCo2, vehicle kms and number of vehicles 
under each scenario. This only relates to cars, vans and HGVs (and does not
include bus and rail).

- fuel_share: The share of fuels among each vehicle type by scenario and year.
This only relates to cars, vans and HGVs (and does not include bus and rail).

- carbon_scenario_attributes: A summary output file to support GIS mapping. Includes info
for all scenarios, broken down by zone and year. This only relates to cars, vans 
and HGVs (and does not include bus and rail).The variables include:
----> NELUM area type: Whether a zone is urban, sub-urban or rural
----> North: Whether a zone is internal or external of TfN's northern boundary
----> Total Co2 emissions in megatonnes
----> Co2 percentage change compared to the equivalent zone in 2018
----> Total Co2 emissions from cars (megatonnes)
----> Total Co2 emissions from vans and HGVs (megatonnes)
----> Emissions per head
----> Emissions intensity (gCo2 / vehicle kms)
----> Total kms travelled
----> Vehicle km percentage change compared to the equivalent zone in 2018
----> Vehicle kms per head
----> Number of cars

- pt_emissions: Emissions estimate (megatonnes of Co2) for bus and rail by year
and scenario. Estimates are pan-northern.

- scenarios_vs_target plot: A summary plot of emissions under each scenario,
which includes bus and rail, as well as cars, vans and HGVs. The dotted line 
represents the decarbonisation trajectory, which outlines TfN's current targets for future 
emissions. 

- Audit folder: Files used to sense check outputs. If run_fresh == False in 
NoCarb, these files will be called on directly (apart from the scrappage curve), else overwritten on each run.
-----> 	characteristics: Used to calculate the baseline speed emissions curve
		and includes info related to average Co2, average mass and average engine size
		by cohort, vehicle type, segment and fuel.
----->	fleet_archive: Historical vehicle registration data for cars, vans and 
		HGVs (dating back to 2003) which has undergone basic pre-processing.
----->	index_fleet: Fully pre-processed baseline (2018) fleet information, 
		derived from the fleet_archive but mapped to TfN's NELUM zones and with
		more robust pre-processing. Breaks down the number of vehicles by zone,
		cohort, vehicle type, segment and fuel. Excludes bus and rail.
----->	projected_fleet: The fully projected fleet of Just About Managing, used
		to support sense checking only. Includes the same information as 
		index_fleet and covers all scenario years (2018, 2020 and every 5 years
		through to 2050. Excludes bus and rail.
----->	scrappage_curve: Relates to the proportion of a vehicle type that will
		survive in the fleet in future years. Excludes bus and rail.

- Figures folder: Includes the following plots:
-----> Total chainage (vehicle kms) by scenario
-----> Total MTCO2 by scenario
-----> Cumulative MTCO2 by scenario (derived through linear interpolation of intermediary years)
-----> Total electricity gird MTCO2 by scenario
-----> Scenario specific plots:
		----> Fuel share of sales (cars and vans)
		----> Fuel share of sales (HGVs)
		----> Fuel share of fleet (cars and vans)
		----> Fuel share of fleet (HGVs)
		----> Vehicle kms by NELUM area type
		----> Vehicle kms by vehicle type
		----> MTCO2 by NELUM area type
		----> MTCO2 by vehicle type
		
######################################

