# -*- coding: utf-8 -*-
# from pathlib import Path
import itertools
from datetime import datetime, timezone
from os import getenv
from typing import Optional, Sequence

import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

# from makefile:install
EPA_CEMS_DATA_PATH = getenv("EPA_CEMS_DATA_PATH")

PUDL_EPA_EIA_CROSSWALK_URL = (
    "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/"
    "core_epa__assn_eia_epacamd.parquet"
)
PUDL_EIA860M_CHANGELOG_GENERATORS_URL = (
    "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/"
    "core_eia860m__changelog_generators.parquet"
)
CAMD_EIA_CROSSWALK_CSV_URL = (
    "https://raw.githubusercontent.com/catalyst-cooperative/"
    "camd-eia-crosswalk-latest/refs/heads/main/epa_eia_crosswalk.csv"
)

ALL_STATES = (  # includes territories and DC
    "AK",
    "AL",
    "AR",
    "AS",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "GU",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MP",
    "MS",
    "MT",
    "NA",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "PR",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VI",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
)

ALL_CEMS_YEARS = range(1995, datetime.now(timezone.utc).year + 1)


def _available_epacems_columns() -> set[str]:
    """Return available columns in the configured EPA CEMS parquet dataset."""
    if not EPA_CEMS_DATA_PATH:
        return set()
    try:
        return set(pq.read_schema(EPA_CEMS_DATA_PATH).names)
    except Exception:
        return set()


def _build_unit_id_epa(cems: pd.DataFrame) -> pd.Series:
    """Create a stable surrogate unit ID if unit_id_epa is not provided."""
    if "unitid" in cems.columns:
        unit_col = "unitid"
    elif "emissions_unit_id_epa" in cems.columns:
        unit_col = "emissions_unit_id_epa"
    else:
        raise KeyError("Cannot derive unit_id_epa: no unit identifier column found.")

    if "plant_id_eia" in cems.columns:
        plant_col = "plant_id_eia"
    elif "plant_id_epa" in cems.columns:
        plant_col = "plant_id_epa"
    else:
        raise KeyError("Cannot derive unit_id_epa: no plant identifier column found.")

    key = (
        cems[[plant_col, unit_col]]
        .astype("string")
        .fillna("<NA>")
        .agg("||".join, axis=1)
    )
    return pd.factorize(key, sort=False)[0].astype("int64")


def year_state_filter(years=(), states=()):
    """
    Create filters to read given years and states from partitioned parquet dataset.

    This function was the only dependency on the pudl repo, so I copied it from
    pudl.outputs.epacems.py:year_state_filter

    A subset of an Apache Parquet dataset can be read in more efficiently if files
    which don't need to be queried are avoideed. Some datasets are partitioned based
    on the values of columns to make this easier. The EPA CEMS dataset which we
    publish is partitioned by state and report year.

    This function takes a set of years, and a set of states, and returns a list of lists
    of tuples, appropriate for use with the read_parquet() methods of pandas and dask
    dataframes.

    Args:
        years (iterable): 4-digit integers indicating the years of data you would like
            to read. By default it includes all years.
        states (iterable): 2-letter state abbreviations indicating what states you would
            like to include. By default it includes all states.

    Returns:
        list: A list of lists of tuples, suitable for use as a filter in the
        read_parquet method of pandas and dask dataframes.

    """
    year_filters = [("year", "=", year) for year in years]
    state_filters = [("state", "=", state.upper()) for state in states]

    if states and not years:
        filters = [
            [
                tuple(x),
            ]
            for x in state_filters
        ]
    elif years and not states:
        filters = [
            [
                tuple(x),
            ]
            for x in year_filters
        ]
    elif years and states:
        filters = [list(x) for x in itertools.product(year_filters, state_filters)]
    else:
        filters = None

    return filters


def load_epacems(
    states: Optional[Sequence[str]] = ("CO",),
    years: Optional[Sequence[int]] = (2019,),
    columns: Optional[Sequence[str]] = (
        "plant_id_eia",
        "unitid",
        "operating_datetime_utc",
        # "operating_time_hours",
        "gross_load_mw",
        # "steam_load_1000_lbs",
        # "so2_mass_lbs",
        # "so2_mass_measurement_code",
        # "nox_rate_lbs_mmbtu",
        # "nox_rate_measurement_code",
        # "nox_mass_lbs",
        # "nox_mass_measurement_code",
        # "co2_mass_tons",
        # "co2_mass_measurement_code",
        # "heat_content_mmbtu",
        # "facility_id",
        "unit_id_epa",
        # "year",
        # "state",
    ),
    engine: Optional[str] = "pandas",
) -> pd.DataFrame:
    """load EPA CEMS data from PUDL with optional subsetting

    Args:
        states (Optional[Sequence[str]], optional): subset by state abbreviation. Pass None to get all states. Defaults to ("CO",).
        years (Optional[Sequence[int]], optional): subset by year. Pass None to get all years. Defaults to (2019,).
        columns (Optional[Sequence[str]], optional): subset by column. Pass None to get all columns. Defaults to ( "plant_id_eia", "unitid", "operating_datetime_utc", "operating_time_hours", "gross_load_mw", "state", ).
        engine (Optional[str], optional): choose 'pandas' or 'dask'. Defaults to 'pandas'

    Returns:
        pd.DataFrame: epacems data
    """
    if states is None:
        states = list(ALL_STATES)
        # states = pudl.constants.us_states.keys()  # all states
    else:
        states = list(states)
    if years is None:
        years = list(ALL_CEMS_YEARS)
        # years = pudl.constants.data_years["epacems"]  # all years
    else:
        years = list(years)
    if columns is not None:
        # columns=None is handled by pd.read_parquet, gives all columns
        columns = list(columns)
    if engine != "pandas":
        raise NotImplementedError("dask engine not yet implemented. Only pandas")

    # pudl_settings = pudl.workspace.setup.get_defaults()
    # cems_path = Path(pudl_settings["parquet_dir"]) / "epacems"
    available = _available_epacems_columns()
    read_columns = columns
    if columns is not None and available:
        aliases = {
            "unitid": "emissions_unit_id_epa",
            "steam_load_1000_lbs": "steam_load_lbs",
        }
        read_columns = []
        for col in columns:
            if col in available:
                read_columns.append(col)
            elif col in aliases and aliases[col] in available:
                read_columns.append(aliases[col])
            elif col == "unit_id_epa":
                # May be synthesized after read if absent in source schema.
                continue
            else:
                read_columns.append(col)
        read_columns = list(dict.fromkeys(read_columns))

    cems = pd.read_parquet(
        # cems_path,
        EPA_CEMS_DATA_PATH,
        # use_nullable_dtypes=True,
        columns=read_columns,
        # filters=pudl.output.epacems.year_state_filter(
        filters=year_state_filter(
            states=states,
            years=years,
        ),
    )

    # Backward compatibility: mirror legacy CEMS column names when only new names exist.
    if "unitid" not in cems.columns and "emissions_unit_id_epa" in cems.columns:
        cems["unitid"] = cems["emissions_unit_id_epa"]
    if "steam_load_1000_lbs" not in cems.columns and "steam_load_lbs" in cems.columns:
        cems["steam_load_1000_lbs"] = cems["steam_load_lbs"] / 1000

    if "unit_id_epa" not in cems.columns:
        cems["unit_id_epa"] = _build_unit_id_epa(cems)

    if columns is not None:
        # Preserve requested legacy column set/order after compatibility transformations.
        missing = [col for col in columns if col not in cems.columns]
        if missing:
            raise KeyError(f"Requested columns are missing after load: {missing}")
        cems = cems.loc[:, columns]

    return cems


def load_eia860m_changelog_generators() -> pd.DataFrame:
    """Load EIA860m generator changelog data for month-aware capacity joins."""
    cols = [
        "report_date",
        "valid_until_date",
        "plant_id_eia",
        "generator_id",
        "capacity_mw",
    ]
    out = pd.read_parquet(PUDL_EIA860M_CHANGELOG_GENERATORS_URL, columns=cols)
    out["generator_id"] = out["generator_id"].astype("string")
    out["report_date"] = pd.to_datetime(out["report_date"]).dt.tz_localize(None)
    out["valid_until_date"] = pd.to_datetime(out["valid_until_date"]).dt.tz_localize(
        None
    )
    return out


def load_epa_crosswalk() -> pd.DataFrame:
    """Load EPA/EIA crosswalk from PUDL parquet and enrich with CAMD CSV fields.

    The PUDL parquet provides annual CAMD<->EIA associations. It does not include
    many legacy CAMD/EIA metadata fields used downstream (e.g. fuel and capacity).
    Those fields are sourced from the latest CAMD/EIA crosswalk CSV and merged in.
    """
    pudl_cols = [
        "report_year",
        "plant_id_epa",
        "emissions_unit_id_epa",
        "plant_id_eia",
        "generator_id",
    ]
    pudl_xwalk = pd.read_parquet(PUDL_EPA_EIA_CROSSWALK_URL, columns=pudl_cols)
    pudl_xwalk = pudl_xwalk.rename(
        columns={
            "plant_id_epa": "CAMD_PLANT_ID",
            "emissions_unit_id_epa": "CAMD_UNIT_ID",
            "plant_id_eia": "EIA_PLANT_ID",
            "generator_id": "EIA_GENERATOR_ID",
        }
    )
    pudl_xwalk["CAMD_UNIT_ID"] = pudl_xwalk["CAMD_UNIT_ID"].astype("string")
    pudl_xwalk["EIA_GENERATOR_ID"] = pudl_xwalk["EIA_GENERATOR_ID"].astype("string")

    camd_xwalk = pd.read_csv(CAMD_EIA_CROSSWALK_CSV_URL)
    camd_xwalk["CAMD_UNIT_ID"] = camd_xwalk["CAMD_UNIT_ID"].astype("string")
    camd_xwalk["EIA_GENERATOR_ID"] = camd_xwalk["EIA_GENERATOR_ID"].astype("string")

    merge_keys = [
        "CAMD_PLANT_ID",
        "CAMD_UNIT_ID",
        "EIA_PLANT_ID",
        "EIA_GENERATOR_ID",
    ]
    camd_meta = camd_xwalk.drop_duplicates(subset=merge_keys)

    merged = pudl_xwalk.merge(
        camd_meta,
        on=merge_keys,
        how="left",
    )
    return merged
