"""
Specify input and output folder locations
"""

from pathlib import Path

data_path = Path.cwd()

# fleet emissions tool data
REGION_FILTER = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "msoa_region.csv"

MSOA_BODY = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "Bodytype by MSOA - 200507.csv"

MSOA_LAD = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "MSOA_LAD_lookup.csv"

MSOA_AREA_TYPE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "areaTypesMSOA.csv"

TARGET_AREA_TYPE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "New Area Types.csv"

ANPR_DATA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "ANPR_data_North.xlsx"

NOHAM_AREA_TYPE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "noham_area_types.csv"

TRAVELLER_DATA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "tfn_traveller_type.csv"

LOG_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "outputs" / "cafcarbon.log"

OUT_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "outputs"

NOHAM_TO_MSOA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "noham_msoa_pop_weighted_lookup.csv"

DEMOGRAPHICS_DATA = (
    Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "output_6_resi_gb_msoa_tfn_tt_prt_2018_pop.pbz2"
)

SEG_SHARE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "seg_prop_vtype_2015.csv"

FUEL_SHARE = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "fuel_prop_segment_2015.csv"

DEMAND_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "demand" / "MC"

CAR_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "fleet" / "Cars by LAD from 2003 - 200507.csv"

LGV_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "fleet" / "LGVs by LAD from 2003 - 200507.csv"

HGV_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "fleet" / "Goods by LAD from 2003 - 200507.csv"

# fleet emissions tool preprocessing data
DEMAND_FACTORS = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "demand_factors.csv"

MSOA_RTM_TRANSLATION = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "MC" / "MRTM2v14_to_MSOA_spatial.csv"
# Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "SERTM_to_msoa11_spatial.csv"

LINK_ROAD_TYPES = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "MC" / "MiHAM_Links_msoaio.csv"
# Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "lookup" / "SE_link.csv"

DEMAND_DATA = Path("T:/") / "Adam Adamson" / "NoCarb" / "MC CafCarb Runs Inputs"
# Path("O:/") / "26.RTM2" / "SERTM2" / "Assignment"

LINK_DATA = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "MC"

DEMAND_OUT_PATH = Path("E:/") / "GitHub" / "caf.carbon" / "CAFCarb" / "input" / "demand" / "MC" / "SC01"

# vkm emissions tool data
BATCHED_PROCESSING_FILE = Path("E:/") / "GitHub" / "caf.carbon" / "Model input data" / "batch_run_set_up.csv"
