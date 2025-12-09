# Built-Ins
import configparser as cf
import re

# Third Party
import pandas as pd
from pandas import DataFrame
import math

# Local Imports
from caf.carbon import audit_tests as at


# %% Helper functions
def if_string_then_list(string_or_list):
    """If input is string, convert to one element list, else leave as list."""
    list_form = [string_or_list] if isinstance(string_or_list, str) else string_or_list
    return list_form


def camel_columns_to_snake(table_df):
    """Convert dataframe column names to snake case."""

    def camel_to_snake(string):
        # Special cases
        string = re.sub("NELUM", "Nelum", string)
        string = re.sub("CYA", "Cya", string)
        # Convert string to snake case
        string = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", string)
        string = re.sub("([a-z0-9])([A-Z])", r"\1_\2", string).lower()
        string = re.sub(" _", "_", string)
        string = re.sub(" ", "_", string)
        return string

    table_df.columns = [camel_to_snake(i) for i in table_df.columns]
    return table_df


def all_but(table_df, names):
    """List all column names except those provided as 'names'."""
    names = if_string_then_list(names)
    return [item for item in table_df.columns if item not in names]


def group_to_list_string(start, end):
    """Give interval [start,end] as a comma separated string."""
    cya_list = list(range(start, end + 1))
    return ",".join(map(str, cya_list))


def new_load_general_table(sheet_name: str, parameters) -> pd.DataFrame:
    """Load tables from Excel sheets. Remove the config.txt method and access the sheets into a df directly"""
    if sheet_name in ["gridConsumption", "gridCarbonIntensity"]:
        header_row = 2
    else:
        header_row = 0

    table = pd.read_excel(
        io=parameters.general_table_path, sheet_name=sheet_name, header=header_row
    ).dropna()

    # pylint: enable-all
    # Remove column suffixes (e.g. second 2018 column is called 2018.2)
    table = table.rename(columns=lambda x: re.sub(r"\.[0-9]$", "", str(x)))
    table = camel_columns_to_snake(table)
    return table


def new_load_scenario_tables(
    scenario: str, table_name: str, parameters, suffix
) -> pd.DataFrame:
    """Load scenario tables from inputs folder"""
    # TODO(JC): - update in future
    table_ranges = {
        "segSales_propOfTypeYear": "A:H",
        "fuelSales_propOfSegYear": "J:R",
        "fleetSize_totOfYear": "T:Z",
        "ptEmissionReduction": "AB:AH",
        "co2Reduction": "AJ:AL",
        "ChainageReduction": "AN:AU",
    }

    if suffix in ["none", ""]:
        suffix = ".xlsx"
    else:
        suffix = "_" + suffix + ".xlsx"
    use_cols = table_ranges[table_name]

    table = pd.read_excel(
        io="{}{}".format(parameters.scenario_table_path, suffix),
        sheet_name=scenario,
        usecols=use_cols,
        header=1,
    ).dropna()

    table = table.rename(columns=lambda x: re.sub(r"\.[0-9]$", "", str(x)))
    table = camel_columns_to_snake(table)

    return table


def load_scenario_tables(scenario: str, table_name: str, parameters, suffix) -> pd.DataFrame:
    """Load scenario tables from inputs folder"""
    # TODO(JC): - update in future
    table_ranges = {
        "segSales_propOfTypeYear": "A:I",
        "fuelSales_propOfSegYear": "K:T",
        "fleetSize_totOfYear": "V:AC",
        "ptEmissionReduction": "AE:AL",
        "co2Reduction": "AN:AP",
        "ChainageReduction": "AR:AZ",
    }

    if suffix in ["none", ""]:
        suffix = ".xlsx"
    else:
        suffix = "_" + suffix + ".xlsx"
    use_cols = table_ranges[table_name]

    table = pd.read_excel(
        io="{}{}".format(parameters.scenario_table_path, suffix),
        sheet_name=scenario,
        usecols=use_cols,
        header=1,
    ).dropna()

    table = table.rename(columns=lambda x: re.sub(r"\.[0-9]$", "", str(x)))
    table = camel_columns_to_snake(table)

    return table


def load_csv(self, table_name, parameters):
    """Load table from a csv with only one table.

    Path is loaded from the config.txt settings.
    """
    file_path = parameters.naei
    table = pd.read_csv(file_path)
    table = camel_columns_to_snake(table)
    at.describe_table(table_name, table, file_path)
    return table


# %% Recoding
def cya_group_to_list(table_df):
    """Convert CYA bins to CYA string lists.

    e.g. '15+' -> '15,16,17,18,19,20'.
    """
    table_df = table_df.copy()

    recode = {
        "00": 0,
        "01": 1,
        "02": 2,
        "03": 3,
        "04": 4,
        "05": 5,
        "06_08": group_to_list_string(6, 8),
        "09_11": group_to_list_string(9, 11),
        "12_14": group_to_list_string(12, 14),
        "15+": group_to_list_string(15, 20),
    }
    table_df["cya"] = table_df["cya"].replace(recode)
    return table_df


def determine_body_type(table_df):
    """Generate body type attribute from vehicle type (and segment for HGVs)."""
    table_df = table_df.copy()

    vehicle_type_to_body_type = {"car": "Car", "lgv": "Van", "hgv": "HGV"}
    table_df["body_type"] = table_df["vehicle_type"].replace(vehicle_type_to_body_type)
    table_df.loc[table_df["segment"].str.contains("artic") == True, "body_type"] = "Artic"
    table_df.loc[table_df["segment"].str.contains("rigid") == True, "body_type"] = "Rigid"
    return table_df


def determine_fuel_type(table_df):
    """Group engine types into fuels for use in combustion calculations."""
    table_df = table_df.copy()

    table_df["fuel_type"] = table_df["fuel"]
    table_df.loc[table_df["fuel"] == "phev", "fuel"] = "petrol hybrid"
    table_df.loc[table_df["fuel"].isin(["bev", "hydrogen"]), "fuel"] = "clean"
    return table_df


# %% Data operations
def weighted_mean(table_df, grouping_var_list, weight_var, mean_var_list):
    """Calculate the weighted mean of an attribute within a selected group.

    Parameters
    ----------
    table_df : DataFrame
        Longer dataframe with a single CYA value per row.

    grouping_var_list : list of str
        Set of attributes that will uniquely define the final output.

    weight_var : str
        Attribute defining the weighting of each row.

    mean_var_list : list of str
        Attribute (or set of) which will be averaged.

    Returns
    ----------
    table_df : DataFrame
        Reduced dataframe with only the listed attributes.
    """
    grouping_var_list = if_string_then_list(grouping_var_list)
    mean_var_list = if_string_then_list(mean_var_list)
    table_df = table_df.copy()
    mutable_columns = mean_var_list + [weight_var]
    table_df[mean_var_list] = table_df[mean_var_list].mul(table_df[weight_var], axis=0)
    table_df = table_df.groupby(grouping_var_list)[mutable_columns].sum().reset_index()
    table_df[mean_var_list] = table_df[mean_var_list].div(table_df[weight_var], axis=0)
    table_df = table_df.drop_duplicates()
    return table_df


def cya_column_to_group(table_df):
    """Bin discrete CYA values into the initial form."""
    table_df = table_df.copy()
    table_df["cya"] = table_df["cya"].astype(int)
    table_df.loc[table_df["cya"].isin([6, 7, 8]), "cya"] = "06_08"
    table_df.loc[table_df["cya"].isin([9, 10, 11]), "cya"] = "09_11"
    table_df.loc[table_df["cya"].isin([12, 13, 14]), "cya"] = "12_14"
    table_df.loc[table_df["cya"].isin([15, 16, 17, 18, 19, 20]), "cya"] = "15+"
    table_df["cya"] = table_df["cya"].astype(str)
    return table_df


def cya_list_to_column(table_df, shared_value=None):
    """Convert CYA string lists into a row for each entry in the list.

    Parameters
    ----------
    table_df : DataFrame
        Longer dataframe with a single CYA value per row.

    shared_value : str (or None)
        Extensive property to be shared.

    Returns
    ----------
    """
    table_df = table_df.copy()
    table_df["cya"] = table_df["cya"].astype("str").str.split(",")
    table_df["cya"] = table_df["cya"].apply(lambda x: [int(i) for i in x])
    if shared_value is not None:
        pre_count = table_df[shared_value].sum()
        table_df[shared_value] = table_df[shared_value].div(table_df["cya"].map(len), axis=0)

    table_df = table_df.explode("cya")
    table_df["cya"] = table_df["cya"].astype("int64")

    if shared_value is not None:
        post_count = table_df[shared_value].sum()
        at.change_in_count("cya", shared_value, pre_count, post_count)
    return table_df


def determine_from_similar(table_df, shared_qualities, missing_quality, value_to_distribute):
    """Imputes unknown values based on the distribution of known values for a set of shared attributes.

    Parameters
    ----------
    table_df : DataFrame
        Longer dataframe with a single CYA value per row.

    shared_qualities : list of str
        Set of attributes which correlate with the missing attribute.

    missing_quality : str
        Attribute which is being imputed.

    value_to_distribute : str
        Extensive properties to be shared.


    Returns
    ----------
    table_df : DataFrame
        Dataframe with unknown values mapped to known values.
    """
    pre_count = table_df[value_to_distribute].sum()

    table_df = table_df.copy()
    table_df[missing_quality] = table_df[missing_quality].replace({"Unknown": "unknown"})
    reduced_table = (
        table_df.groupby(shared_qualities + [missing_quality])[value_to_distribute]
        .sum()
        .reset_index()
    )
    missing_table = reduced_table.loc[(reduced_table[missing_quality] == "unknown")].drop(
        columns=missing_quality
    )

    # Define distribution of the imputed attribute in the data when the missing values are removed.
    reduced_table = reduced_table.loc[~(reduced_table[missing_quality] == "unknown")]
    reduced_table["share"] = reduced_table[value_to_distribute] / reduced_table.groupby(
        shared_qualities
    )[value_to_distribute].transform("sum")
    reduced_table = reduced_table.drop(columns=value_to_distribute)

    # Assign missing values according to the distribution
    missing_table = missing_table.merge(reduced_table, how="left", on=shared_qualities)
    missing_table["distributed_value"] = (
        missing_table[value_to_distribute] * missing_table["share"]
    )
    missing_table = missing_table.drop(columns=[value_to_distribute, "share"])

    # Distribute the imputed values across the non-grouped subset of missing values
    unkn_table_df = table_df.loc[(table_df[missing_quality] == "unknown")]
    unkn_table_df = unkn_table_df.drop(columns=missing_quality)
    unkn_table_df["share"] = unkn_table_df[value_to_distribute] / unkn_table_df.groupby(
        shared_qualities
    )[value_to_distribute].transform("sum")
    unkn_table_df = unkn_table_df.merge(missing_table, how="left", on=shared_qualities)
    unkn_table_df[value_to_distribute] = (
        unkn_table_df["distributed_value"] * unkn_table_df["share"]
    )
    unkn_table_df = unkn_table_df.drop(columns=["share", "distributed_value"])

    # Append imputed subset to the original dataframe after removing the missing values
    kn_table_df = table_df.loc[~(table_df[missing_quality] == "unknown")]
    table_df = kn_table_df._append(unkn_table_df, ignore_index=True)

    # Check that the value to distribute still sums to the same total as before the imputation
    post_count = table_df[value_to_distribute].sum()
    at.change_in_count(missing_quality, value_to_distribute, pre_count, post_count)
    return table_df


def interpolate_timeline(table_df, grouping_vars, value_var, melt=True):
    """Interpolate values on a yearly basis using the year attribute.

    Parameters
    ----------
    table_df : DataFrame
        Longer dataframe with a single CYA value per row.
    grouping_vars : list of str
        Set of attributes which uniquely define the imputation groups.
    value_var : str
        Attribute which is being imputed.
    melt : boolean
        Should the table first be pivoted from wide year to long year.

    Returns
    ----------
    table_df : DataFrame
        Dataframe with full set of years.
    """
    if melt:
        if value_var == "segment_sales_distribution":
            table_df.rename(columns={"Segment": "segment"}, inplace=True)  # Rename column
        if value_var == "segment_fuel_sales_distribution":
            table_df.rename(
                columns={"Segment": "segment", "Fuel": "fuel"}, inplace=True
            )  # Rename column

        table_df = pd.melt(
            table_df, id_vars=grouping_vars, var_name="year", value_name=value_var
        )

    table_df["year"] = pd.to_datetime(table_df["year"], format="%Y")
    table_df = table_df.set_index("year")
    table_df = (
        table_df.groupby(grouping_vars)
        .resample("Y")
        .first()
        .interpolate(axis=0)[value_var]
        .reset_index()
    )
    table_df["year"] = table_df["year"].dt.year
    return table_df


def s_curve_value(a, k, x0, x):
    value = a / (1 + math.exp(-k * (x - x0)))
    return value


def load_table(self, table_name, parameters, table_type=None, suffix="File"):
    """Load table from an excel sheet containing multiple tables.

    Path, sheet and position are loaded from the config.txt settings.
    """
    if table_type is None:
        table_type = self.type
        header_row = 1
    elif table_type == "gridCo2":
        header_row = 2
    if suffix in ["None", ""]:
        suffix = "File"
    file_path = parameters.general_table_path
    if table_name in ["gridConsumption", "gridCarbonIntensity"]:
        header_row = 2
    else:
        header_row = 0

    table = pd.read_excel(
        io=parameters.general_table_path, sheet_name=table_name, header=header_row
    ).dropna()

    # Remove column suffixes (eg. second 2018 column is called 2018.2)
    table = table.rename(columns=lambda x: re.sub("\.[0-9]$", "", str(x)))
    table = camel_columns_to_snake(table)

    at.describe_table(table_name, table, file_path)
    return table
