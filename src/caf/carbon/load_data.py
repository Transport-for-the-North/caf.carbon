"""
Specify input and output folder locations
"""

from pathlib import Path

data_path = Path.cwd()

# fleet emissions tool data D:\caf.carbon-EVCI\CAFCarb
REGION_FILTER = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "msoa_region.csv"

INPUT_PATH = Path("D:/") / "caf.carbon-EVCI"

POSTCODE_MSOA = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "postcodes.csv"

DVLA_BODY = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "DVLA_body_type_look_up.csv"
)

MSOA_BODY = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "Bodytype by MSOA - 200507.csv"
)

MSOA_LAD = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "MSOA_LAD_lookup.csv"

MSOA_AREA_TYPE = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "areaTypesMSOA.csv"

TARGET_AREA_TYPE = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "New Area Types.csv"
)

ANPR_DATA = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "ANPR_data_North.xlsx"

NOHAM_AREA_TYPE = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "noham_area_types.csv"
)

TRAVELLER_DATA = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "tfn_traveller_type.csv"
)

LOG_PATH = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "outputs" / "cafcarbon.log"

OUT_PATH = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "outputs"

NOHAM_TO_MSOA = (
    Path("D:/")
    / "caf.carbon-EVCI"
    / "CAFCarb"
    / "input"
    / "noham_msoa_pop_weighted_lookup.csv"
)

VKM_BENCHMARK = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "input" / "region_traffic_benchmark.csv"
)

DEMOGRAPHICS_DATA = (
    Path("D:/")
    / "caf.carbon-EVCI"
    / "CAFCarb"
    / "input"
    / "output_6_resi_gb_msoa_tfn_tt_prt_2018_pop.pbz2"
)

SEG_SHARE = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "input" / "seg_prop_vtype_2015.csv"

FUEL_SHARE = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "input" / "fuel_prop_segment_2015.csv"
)

DEMAND_PATH = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "input" / "demand" / "TfN"

CAR_PATH = (
    Path("D:/")
    / "caf.carbon-EVCI"
    / "CAFCarb"
    / "fleet"
    / "Cars by LAD from 2003 - 200507.csv"
)

LGV_PATH = (
    Path("D:/")
    / "caf.carbon-EVCI"
    / "CAFCarb"
    / "fleet"
    / "LGVs by LAD from 2003 - 200507.csv"
)

HGV_PATH = (
    Path("D:/")
    / "caf.carbon-EVCI"
    / "CAFCarb"
    / "fleet"
    / "Goods by LAD from 2003 - 200507.csv"
)

VEHICLE_PATH = (
    Path("D:/")
    / "caf.carbon-EVCI"
    / "CAFCarb"
    / "fleet"
    / "TFN Data Feb 24"
    / "TFNScan_Feb24.csv"
)

# fleet emissions tool preprocessing data
DEMAND_FACTORS = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "demand_factors.csv"

MSOA_RTM_TRANSLATION = (
    Path("D:/")
    / "caf.carbon-EVCI"
    / "CAFCarb"
    / "input"
    / "MC"
    / "MRTM2v14_to_MSOA_spatial.csv"
)
# Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "SERTM_to_msoa11_spatial.csv"

LINK_ROAD_TYPES = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "input" / "MC" / "MiHAM_Links_msoaio.csv"
)
# Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "lookup" / "SE_link.csv"

DEMAND_DATA = Path("T:/") / "Adam Adamson" / "NoCarb" / "MC CafCarb Runs Inputs"
# Path("O:/") / "26.RTM2" / "SERTM2" / "Assignment"

LINK_DATA = Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "input" / "MC"

DEMAND_OUT_PATH = (
    Path("D:/") / "caf.carbon-EVCI" / "CAFCarb" / "input" / "demand" / "MC" / "SC01"
)

# vkm emissions tool data
BATCHED_PROCESSING_FILE = (
    Path("D:/") / "caf.carbon-EVCI" / "Model input data" / "batch_run_set_up.csv"
)
