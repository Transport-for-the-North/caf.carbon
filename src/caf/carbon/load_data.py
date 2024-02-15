"""
Specify input and output folder locations
"""
from pathlib import Path

data_path = Path.cwd()

# fleet emissions tool data
REGION_FILTER = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "msoa_region.csv"

MSOA_BODY = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "Bodytype by MSOA - 200507.csv"

MSOA_LAD = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "MSOA_LAD_lookup.csv"

MSOA_AREA_TYPE = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "areaTypesMSOA.csv"

TARGET_AREA_TYPE = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "New Area Types.csv"

ANPR_DATA = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "ANPR_data_North.xlsx"

NOHAM_AREA_TYPE = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "noham_area_types.csv"

TRAVELLER_DATA = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "tfn_traveller_type.csv"

LOG_PATH = \
    Path.cwd() / \
    "CAFCarb" / "outputs" / "cafcarbon.log"

OUT_PATH = \
    Path.cwd() / \
    "CAFCarb" / "outputs"

NOHAM_TO_MSOA = \
    Path.cwd() / \
    "CAFCarb" / "input" / "noham_msoa_pop_weighted_lookup.csv"

DEMOGRAPHICS_DATA = \
    Path.cwd() / \
    "CAFCarb" / "input" / "output_6_resi_gb_msoa_tfn_tt_prt_2018_pop.pbz2"

SEG_SHARE = \
    Path.cwd() / \
    "CAFCarb" / "input" / "seg_prop_vtype_2015.csv"

FUEL_SHARE = \
    Path.cwd() / \
    "CAFCarb" / "input" / "fuel_prop_segment_2015.csv"

DEMAND_PATH = \
    Path.cwd() / \
    "src" / "caf" / "carbon""CAFCarb" / "input" / "demand" / "TfSE"

CAR_PATH = \
    Path.cwd() / \
    "CAFCarb" / "fleet" / "Cars by LAD from 2003 - 200507.csv"

LGV_PATH = \
    Path.cwd() / \
    "CAFCarb" / "fleet" / "LGVs by LAD from 2003 - 200507.csv"

HGV_PATH = \
    Path.cwd() / \
    "CAFCarb" / "fleet" / "Goods by LAD from 2003 - 200507.csv"

# fleet emissions tool preprocessing data
DEMAND_FACTORS = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "demand_factors.csv"

MSOA_RTM_TRANSLATION = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "SERTM_to_msoa11_spatial.csv"

LINK_ROAD_TYPES = \
    Path.cwd() / \
    "CAFCarb" / "lookup" / "SE_link.csv"

DEMAND_DATA = Path("O:/") / "26.RTM2" / "SERTM2" / "Assignment"

LINK_DATA = Path("E:/") / "RTMs" / "SERTM2" / "Assignment"

# vkm emissions tool data
BATCHED_PROCESSING_FILE = \
    Path.cwd() / \
    "Model input data" / "batch_run_set_up.csv"
