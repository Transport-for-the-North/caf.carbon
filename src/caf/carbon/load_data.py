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
