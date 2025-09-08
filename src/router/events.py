import json
import numpy as np
import pandas as pd
from datetime import datetime
from functools import lru_cache
from utils.logger import log_execution_time
from utils.common import asset2df, network_asset2df
from concurrent.futures import ThreadPoolExecutor
from contants.color import get_color
from memory.atmiv import memory_atm_iv
from memory.vol import memory_vol
from memory.correlation import memory_correlation
from memory.skew import memory_skew
from memory.skew_benchmark import memory_skew_benchmark
from memory.fwd_scan import memory_fwd_scan_iv
from memory.price_up_down import memory_price_change
from memory.strike_ls import memory_strike_ls
from memory.calendars import memory_calendars
from memory.bcrs_scan import memory_bcrs
from memory.intra_long_short import memory_intra_long_short
from memory.long_short import memory_ls_iv
from memory.atr_scan import memory_atr
from memory.iv_surface import memory_surface_scan_iv


@lru_cache(maxsize=None)
def initialize_metadata():
    expected_df = asset2df("expected_results.csv")
    confirmed_df = network_asset2df("confirm_results.csv")

    merged_df = (
        pd.merge(
            expected_df,
            confirmed_df,
            on="symbol",
            how="outer",
            suffixes=("_expected", "_confirmed"),
        )
        .rename(columns={"symbol": "ticker"})
        .rename(columns={"date_expected": "expected", "date_confirmed": "confirmed"})
        .assign(
            color=lambda x: x.apply(
                lambda row: get_color(
                    confirmed_date=(
                        datetime.strptime(row.confirmed, "%Y-%m-%d").date()
                        if pd.notnull(row.confirmed)
                        else None
                    ),
                    expected_date=(
                        datetime.strptime(row.expected, "%Y-%m-%d").date()
                        if pd.notnull(row.expected)
                        else None
                    ),
                ),
                axis=1,
            )
        )
        .reindex(columns=["ticker", "expected", "confirmed", "color"])
    ).replace({np.nan: None})

    # return merged_df.to_dict(orient='records')
    return json.dumps(merged_df.to_dict(orient="records"))


@lru_cache(maxsize=None)
def initialize_atm_iv():
    memory_atm_iv.initialize()


@lru_cache(maxsize=None)
def initialize_vol():
    memory_vol.initialize()


@lru_cache(maxsize=None)
def initialize_correlation():
    memory_correlation.initialize()


@lru_cache(maxsize=None)
def initialize_fwd():
    memory_fwd_scan_iv.initialize()


@lru_cache(maxsize=None)
def initialize_skew_benchmark():
    memory_skew_benchmark.initialize()


@lru_cache(maxsize=None)
def initialize_strike_ls():
    pass
    # memory_strike_ls.initialize()


@lru_cache(maxsize=None)
def initialize_intra_long_short():
    memory_intra_long_short.initialize()


@lru_cache(maxsize=None)
def initialize_long_short():

    memory_ls_iv.initialize()


@lru_cache(maxsize=None)
def initialize_atr():
    memory_atr.initialize()

@lru_cache(maxsize=None)
def initialize_surface_iv():
    memory_surface_scan_iv.initialize()


@log_execution_time
def update_atm_iv_data():
    with ThreadPoolExecutor() as executor:
        tasks = [
            executor.submit(memory_atm_iv.update),
            executor.submit(memory_vol.update),
            executor.submit(memory_correlation.update),
            executor.submit(memory_skew.update),
            executor.submit(memory_skew_benchmark.update),
            executor.submit(memory_fwd_scan_iv.update),
            executor.submit(memory_price_change.update),
            executor.submit(memory_strike_ls.update),
            executor.submit(memory_calendars.update),
            executor.submit(memory_bcrs.update),
            executor.submit(memory_intra_long_short.update),
            executor.submit(memory_ls_iv.update),
            executor.submit(memory_atr.update),
            executor.submit(memory_surface_scan_iv.update),

        ]

        for task in tasks:
            task.result()
