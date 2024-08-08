"""
Specify input and output folder locations
"""
# Built-Ins
import os
from pathlib import Path

# Third Party
from dotenv import load_dotenv

# load .env file
load_dotenv()

# default if not provided path locations
DEFAULT = "[NOT PROVIDED]"

LOOKUP_DIR: Path = Path(os.getenv("LOOKUP_DIR", DEFAULT))
INPUTS_DIR: Path = Path(os.getenv("INPUTS_DIR", DEFAULT))
OUT_PATH: Path = Path(os.getenv("OUT_PATH", DEFAULT))
FLEET_DIR: Path = Path(os.getenv("FLEET_DIR", DEFAULT))

# fleet emissions tool data
REGION_FILTER: Path = Path(os.getenv("REGION_FILTER", DEFAULT))
MSOA_BODY: Path = Path(os.getenv("MSOA_BODY", DEFAULT))
MSOA_LAD: Path = Path(os.getenv("MSOA_LAD", DEFAULT))
MSOA_AREA_TYPE: Path = Path(os.getenv("MSOA_AREA_TYPE", DEFAULT))
TARGET_AREA_TYPE: Path = Path(os.getenv("TARGET_AREA_TYPE", DEFAULT))
ANPR_DATA: Path = Path(os.getenv("ANPR_DATA", DEFAULT))
NOHAM_AREA_TYPE: Path = Path(os.getenv("NOHAM_AREA_TYPE", DEFAULT))
TRAVELLER_DATA: Path = Path(os.getenv("TRAVELLER_DATA", DEFAULT))
LOG_PATH: Path = Path(os.getenv("LOG_PATH", DEFAULT))
NOHAM_TO_MSOA: Path = Path(os.getenv("NOHAM_TO_MSOA", DEFAULT))
DEMOGRAPHICS_DATA: Path = Path(os.getenv("DEMOGRAPHICS_DATA", DEFAULT))
SEG_SHARE: Path = Path(os.getenv("SEG_SHARE", DEFAULT))
FUEL_SHARE: Path = Path(os.getenv("FUEL_SHARE", DEFAULT))
DEMAND_PATH: Path = Path(os.getenv("DEMAND_PATH", DEFAULT))
CAR_PATH: Path = Path(os.getenv("CAR_PATH", DEFAULT))
LGV_PATH: Path = Path(os.getenv("LGV_PATH", DEFAULT))
HGV_PATH: Path = Path(os.getenv("HGV_PATH", DEFAULT))

# fleet emissions tool preprocessing data
DEMAND_FACTORS: Path = Path(os.getenv("DEMAND_FACTORS", DEFAULT))
MSOA_RTM_TRANSLATION: Path = Path(os.getenv("MSOA_RTM_TRANSLATION", DEFAULT))
LINK_ROAD_TYPES: Path = Path(os.getenv("LINK_ROAD_TYPES", DEFAULT))
DEMAND_DATA: Path = Path(os.getenv("DEMAND_DATA", DEFAULT))
LINK_DATA: Path = Path(os.getenv("LINK_DATA", DEFAULT))
DEMAND_OUT_PATH: Path = Path(os.getenv("DEMAND_OUT_PATH", DEFAULT))

# vkm emissions tool data
BATCHED_PROCESSING_FILE: Path = Path(os.getenv("BATCHED_PROCESSING_FILE", DEFAULT))

# new DVLA data
VEHICLE_PATH: Path = Path(os.getenv("VEHICLE_PATH", DEFAULT))
POSTCODE_MSOA: Path = Path(os.getenv("POSTCODE_MSOA", DEFAULT))
DVLA_BODY: Path = Path(os.getenv("DVLA_BODY", DEFAULT))
SEGMENT_PATH: Path = Path(os.getenv("SEGMENT_CORRECTION", DEFAULT))

# tables
GENERAL_TABLES_PATH: Path = Path(os.getenv("GENERAL_TABLES_PATH", DEFAULT))
SCENARIO_TABLES_PATH: Path = Path(os.getenv("SCENARIO_TABLES_PATH", DEFAULT))
#
# # fleet emissions tool data
# REGION_FILTER = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "msoa_region.csv"
#
# POSTCODE_MSOA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "postcodes.csv"
#
# DVLA_BODY = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "DVLA_body_type_look_up.csv"
#
# MSOA_BODY = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "Bodytype by MSOA - 200507.csv"
#
# MSOA_LAD = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "MSOA_LAD_lookup.csv"
#
# MSOA_AREA_TYPE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "areaTypesMSOA.csv"
#
# TARGET_AREA_TYPE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "New Area Types.csv"
#
# ANPR_DATA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "ANPR_data_North.xlsx"
#
# NOHAM_AREA_TYPE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "noham_area_types.csv"
#
# TRAVELLER_DATA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "tfn_traveller_type.csv"
#
# LOG_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "outputs" / "cafcarbon.log"
#
# OUT_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "outputs"
#
# NOHAM_TO_MSOA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "noham_msoa_pop_weighted_lookup.csv"
#
# DEMOGRAPHICS_DATA = (
#     Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "output_6_resi_gb_msoa_tfn_tt_prt_2018_pop.pbz2"
# )
#
# SEG_SHARE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "seg_prop_vtype_2015.csv"
#
# FUEL_SHARE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "fuel_prop_segment_2015.csv"
#
# DEMAND_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "demand" / "TfN"
#
# CAR_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "fleet" / "Cars by LAD from 2003 - 200507.csv"
#
# LGV_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "fleet" / "LGVs by LAD from 2003 - 200507.csv"
#
# HGV_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "fleet" / "Goods by LAD from 2003 - 200507.csv"
#
# VEHICLE_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "fleet" / "TFN Data Feb 24" / "TFNScan_Feb24.csv"
#
# # fleet emissions tool preprocessing data
# DEMAND_FACTORS = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "demand_factors.csv"
#
# MSOA_RTM_TRANSLATION = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "MC" / "MRTM2v14_to_MSOA_spatial.csv"
# # Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "SERTM_to_msoa11_spatial.csv"
#
# LINK_ROAD_TYPES = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "MC" / "MiHAM_Links_msoaio.csv"
# # Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "SE_link.csv"
#
# DEMAND_DATA = Path("T:/") / "Adam Adamson" / "NoCarb" / "MC CafCarb Runs Inputs"
# # Path("O:/") / "26.RTM2" / "SERTM2" / "Assignment"
#
# LINK_DATA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "MC"
#
# DEMAND_OUT_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "demand" / "MC" / "SC01"
#
# # vkm emissions tool data
# BATCHED_PROCESSING_FILE = Path("E:/") / "GitHub" / "caf.carbon" / "Model input data" / "batch_run_set_up.csv"
