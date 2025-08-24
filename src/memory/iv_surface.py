# src/memory/surface_iv.py
from __future__ import annotations

import os
import warnings
import pandas as pd
from typing import Literal, Optional

from contants.dates import EXPIRY  # kept for compatibility if you reference it elsewhere
from utils.api import get_all_token_set
from utils.logger import logger
from utils.common import asset2df, convert_filter_token_set, ASSET_DIR

warnings.filterwarnings("ignore")


EXCLUDE_SYMBOLS = {
    'NIFTY', 'BANKNIFTY', 'AXISBANK', 'RELIANCE', 'LT', 'ICICIBANK',
    'TORNTPHARM', 'HDFCBANK', 'SUNPHARMA', 'AUROPHARMA', 'DIVISLAB',
    'LUPIN', 'GRANULES', 'SYNGENE', 'DRREDDY', 'DIXON', 'KOTAKBANK',
    'DELHIVERY', 'TATAPOWER', 'MCX', 'LICHSGFIN', 'ITC', 'GODREJPROP', 'EICHERMOT'
}


def _safe_to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df





class Surface_IV:
    def __init__(self):
        # live outputs
        self.intraday_short: pd.DataFrame = pd.DataFrame()
        self.intraday_long: pd.DataFrame = pd.DataFrame()
        self.eod_short: pd.DataFrame = pd.DataFrame()
        self.eod_long: pd.DataFrame = pd.DataFrame()

        self.intraday_short_avg: pd.DataFrame = pd.DataFrame()
        self.intraday_long_avg: pd.DataFrame = pd.DataFrame()
        self.eod_short_avg: pd.DataFrame = pd.DataFrame()
        self.eod_long_avg: pd.DataFrame = pd.DataFrame()

        # refs and config
        self.intraday_ref: pd.DataFrame = pd.DataFrame()
        self.eod_iv_ref: pd.DataFrame = pd.DataFrame()
        self.strike_diff_df: pd.DataFrame = pd.DataFrame()

        self.expiry_list: list[str] = []      # used for fetching current token-set
        self.target_expiry: Optional[str] = None  # used for filtering options for comparison

    def initialize(self):
        """
        Load reference CSVs and derive expiry shortlist automatically.
        """
        self.intraday_ref = asset2df("intraday_iv_surface.csv")
        self.eod_iv_ref = asset2df("eod_iv_surface.csv")
        self.strike_diff_df = asset2df("strike_diff.csv")

        # Derive 2–3 most recent expiries present in reference to use for current fetch.
        if "expiry" in self.intraday_ref.columns and not self.intraday_ref.empty:
            exps = (
                pd.Series(self.intraday_ref["expiry"], dtype="string")
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            # keep last 3 when sorted
            self.expiry_list = sorted(exps)[-3:]
            # choose the max as the working monthly target (matches typical “monthly” max)
            self.target_expiry = None
        else:
            # fallback: keep empty (fetch all), and don’t filter target_expiry
            self.expiry_list = []
            self.target_expiry = None

        logger.info(f"[Surface_IV] initialized. expiry_list={self.expiry_list} target_expiry={self.target_expiry}")

    def _process_surface_iv(self):
        # Always reload refs, in case assets updated on disk
        self.intraday_ref = asset2df("intraday_iv_surface.csv")
        self.eod_iv_ref = asset2df("eod_iv_surface.csv")

        # Build filter for token-set fetch
        delta_filter = {"pe": ["-0.5", "-0.2"], "ce": ["0.2", "0.5"]}
        kwargs = {"delta": delta_filter}
        if self.expiry_list:
            kwargs["expiry"] = self.expiry_list

        filtered_data = get_all_token_set(**kwargs)
        current_df = convert_filter_token_set(filtered_data)

        return self.compare_current_with_reference(
            current_df=current_df,
            intraday_reference_df=self.intraday_ref,
            eod_reference_df=self.eod_iv_ref,
            strike_diff_df=self.strike_diff_df,
        )

    def compare_current_with_reference(
        self,
        current_df: pd.DataFrame,
        intraday_reference_df: pd.DataFrame,
        eod_reference_df: pd.DataFrame,
        strike_diff_df: pd.DataFrame,
    ):
        if current_df.empty:
            logger.warning("[Surface_IV] current_df empty from token-set")
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty, empty, empty, empty

        # FUTURES → spot proxy
        fut_df = current_df[current_df["asset_type"] == "Future"].copy()
        if fut_df.empty:
            logger.warning("[Surface_IV] No futures rows in token-set; cannot compute strike_offset.")
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty, empty, empty, empty

        # mergeable spot cols
        spot_cols = ["symbol", "expiry", "ltp"]
        if "percent_change" in fut_df.columns:
            spot_cols.append("percent_change")
        elif "pct_change" in fut_df.columns:
            spot_cols.append("pct_change")

        spot_df = fut_df[spot_cols].rename(columns={"ltp": "fut_ltp"}).copy()

        # OPTIONS
        opt_df = current_df[current_df["asset_type"].isin(["CallOption", "PutOption"])].copy()
        if opt_df.empty:
            logger.warning("[Surface_IV] No options rows in token-set.")
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty, empty, empty, empty

        # # quality filters (project-wide convention)
        # opt_df = opt_df[(opt_df.get("ask_iv", 0) > 0) & (opt_df.get("bid_iv", 0) > 0)]
        # # extra 5% spread filter when both present
        # if "ask_iv" in opt_df.columns and "bid_iv" in opt_df.columns:
        #     spread = (opt_df["ask_iv"] - opt_df["bid_iv"]) / opt_df["ask_iv"].replace(0, pd.NA)
        #     opt_df = opt_df[spread > 0.05]

        # keep only working monthly expiry if available
        if self.target_expiry is not None:
            
            opt_df = opt_df[opt_df["expiry"].astype(str) == str(self.target_expiry)]

        # symbol exclusions
        opt_df = opt_df[~opt_df["symbol"].isin(EXCLUDE_SYMBOLS)].copy()

        # tidy columns for merge
        if "pct_change" in opt_df.columns:
            opt_df = opt_df.drop(columns="pct_change", errors="ignore")

        opt_df = opt_df.merge(spot_df, on=["symbol", "expiry"], how="left")
        opt_df = opt_df.merge(strike_diff_df, on="symbol", how="left")
        opt_df = opt_df.dropna(subset=["fut_ltp", "strike_diff"])

        _safe_to_numeric(opt_df, ["strike_price", "fut_ltp", "strike_diff"])

        opt_df = opt_df.dropna(subset=["fut_ltp", "strike_price", "strike_diff"]).copy()
        opt_df["strike_offset"] = ((opt_df["strike_price"] - opt_df["fut_ltp"]) / opt_df["strike_diff"]).round().astype(int)

        # Save a debug join for inspection
        intraday_merged_df = opt_df.merge(
            intraday_reference_df,
            on=["symbol", "expiry", "asset_type", "strike_offset"],
            how="inner",
            suffixes=("_live", "_ref"),
        )
        try:
            intraday_merged_df.to_csv(os.path.join(ASSET_DIR, "intraday_merged.csv"), index=False)
        except Exception as e:
            logger.warning(f"[Surface_IV] could not write intraday_merged.csv: {e}")

        eod_merged_df = opt_df.merge(
            eod_reference_df,
            on=["symbol", "expiry", "asset_type", "strike_offset"],
            how="inner",
            suffixes=("_live", "_ref"),
        )

        # EMA logic
        intraday_short_df = intraday_merged_df[intraday_merged_df["bid_iv"] > intraday_merged_df["ema_ask_iv"]].copy()
        intraday_long_df  = intraday_merged_df[intraday_merged_df["ask_iv"] < intraday_merged_df["ema_bid_iv"]].copy()
        eod_short_df      = eod_merged_df[eod_merged_df["bid_iv"] > eod_merged_df["ema_ask_iv"]].copy()
        eod_long_df       = eod_merged_df[eod_merged_df["ask_iv"] < eod_merged_df["ema_bid_iv"]].copy()

        # AVG logic
        intraday_short_avg = intraday_merged_df[intraday_merged_df["bid_iv"] > intraday_merged_df["avg_ask_iv"]].copy()
        intraday_long_avg  = intraday_merged_df[intraday_merged_df["ask_iv"] < intraday_merged_df["avg_bid_iv"]].copy()
        eod_short_avg      = eod_merged_df[eod_merged_df["bid_iv"] > eod_merged_df["avg_ask_iv"]].copy()
        eod_long_avg       = eod_merged_df[eod_merged_df["ask_iv"] < eod_merged_df["avg_bid_iv"]].copy()

        # % edges
        def pct_edge(n, d): 
            return (n - d) / d.replace(0, pd.NA)

        intraday_short_df["iv_edge"]     = pct_edge(intraday_short_df["bid_iv"],    intraday_short_df["ema_ask_iv"])
        intraday_long_df["iv_edge"]      = pct_edge(intraday_long_df["ema_bid_iv"], intraday_long_df["ask_iv"])
        eod_short_df["iv_edge"]          = pct_edge(eod_short_df["bid_iv"],         eod_short_df["ema_ask_iv"])
        eod_long_df["iv_edge"]           = pct_edge(eod_long_df["ema_bid_iv"],      eod_long_df["ask_iv"])

        intraday_short_avg["iv_edge"]    = pct_edge(intraday_short_avg["bid_iv"],   intraday_short_avg["avg_ask_iv"])
        intraday_long_avg["iv_edge"]     = pct_edge(intraday_long_avg["avg_bid_iv"],intraday_long_avg["ask_iv"])
        eod_short_avg["iv_edge"]         = pct_edge(eod_short_avg["bid_iv"],        eod_short_avg["avg_ask_iv"])
        eod_long_avg["iv_edge"]          = pct_edge(eod_long_avg["avg_bid_iv"],     eod_long_avg["ask_iv"])

        # keep a light-weight debug CSV
        try:
            intraday_short_df.to_csv(os.path.join(ASSET_DIR, "intraday_short.csv"), index=False)
        except Exception as e:
            logger.warning(f"[Surface_IV] could not write intraday_short.csv: {e}")

        sort = lambda df: df.sort_values("iv_edge", ascending=False)

        return (
            sort(intraday_short_df),
            sort(intraday_long_df),
            sort(eod_short_df),
            sort(eod_long_df),
            sort(intraday_short_avg),
            sort(intraday_long_avg),
            sort(eod_short_avg),
            sort(eod_long_avg),
        )

    def update(self):
        (
            self.intraday_short,
            self.intraday_long,
            self.eod_short,
            self.eod_long,
            self.intraday_short_avg,
            self.intraday_long_avg,
            self.eod_short_avg,
            self.eod_long_avg,
        ) = self._process_surface_iv()
        logger.info(
            f"[Surface_IV] updated. sizes="
            f"IS:{len(self.intraday_short)} IL:{len(self.intraday_long)} "
            f"ES:{len(self.eod_short)} EL:{len(self.eod_long)} "
            f"ISa:{len(self.intraday_short_avg)} ILa:{len(self.intraday_long_avg)} "
            f"ESa:{len(self.eod_short_avg)} ELa:{len(self.eod_long_avg)}"
        )

    def expiry(self, num: Literal[-1, 1, 2, 3, 4]):
        if num == -1:
            return (
                self.intraday_short,
                self.intraday_long,
                self.eod_short,
                self.eod_long,
                self.intraday_short_avg,
                self.intraday_long_avg,
                self.eod_short_avg,
                self.eod_long_avg,
            )
        elif num == 1:
            return self.intraday_short, self.intraday_long
        elif num == 2:
            return self.eod_short, self.eod_long
        elif num == 3:
            return self.strike_diff_df
        elif num == 4:
            return self.intraday_short_avg, self.intraday_long_avg, self.eod_short_avg, self.eod_long_avg
        else:
            return None


memory_surface_scan_iv = Surface_IV()
