import os
from typing import Literal, Optional
import pandas as pd
from contants.dates import EXPIRY
from .skew import memory_skew
from utils.common import asset2df, ASSET_DIR


class Skew_Benchmark:
    def __init__(self):
        self.expiry_1: pd.DataFrame = pd.DataFrame()
        self.expiry_1_with_skew: pd.DataFrame = pd.DataFrame()
        self.expiry_2: pd.DataFrame = pd.DataFrame()
        self.expiry_2_with_skew: pd.DataFrame = pd.DataFrame()

    def _initialize_df(self, filename: str) -> pd.DataFrame:
        df = asset2df(filename)
        df = df.drop(df.columns[0], axis=1)
        return df

    def initialize(self):
        self.expiry_1 = self._initialize_df("skew_expiry_1.csv")
        self.expiry_2 = self._initialize_df("skew_expiry_2.csv")

    def update_ticker_benchmark(
        self,
        expiry: Literal[1, 2],
        ticker: str,
        ppf: Optional[float],
        pcf: Optional[float],
        ccb: Optional[float],
        fourL_F: Optional[float],
    ):
        # Select the DataFrame based on expiry
        if expiry == 1:
            df = self.expiry_1
        elif expiry == 2:
            df = self.expiry_2

        # Filter the DataFrame for the given ticker
        df_ticker = df[df["symbol"] == ticker].copy()

        if df_ticker.empty:
            raise ValueError(f"Ticker {ticker} not found in expiry {expiry}.")

        # Update benchmarks if provided
        if ppf is not None:
            df_ticker.loc[:, "ppf"] = ppf
        if pcf is not None:
            df_ticker.loc[:, "pcf"] = pcf
        if ccb is not None:
            df_ticker.loc[:, "ccb"] = ccb
        if fourL_F is not None:
            df_ticker.loc[:, "4l_f"] = fourL_F

        df.update(df_ticker)

        filename = f"skew_expiry_{expiry}.csv"
        file_path = os.path.join(ASSET_DIR, filename)
        df.to_csv(file_path)
        

        # Update the benchmark and timestamp in the merged DataFrame
        self.update()

    def update(self):
        _df = memory_skew.get_dump().copy()
        cols = [
            "symbol",
            "expiry",
            "pe_pe.skew",
            "pe_ce.skew",
            "ce_ce.skew",
            "four_leg.skew",
        ]
        master_df = _df[cols]

        exp1_df = master_df[master_df["expiry"] == EXPIRY[0]]
        exp2_df = master_df[master_df["expiry"] == EXPIRY[1]]

        self.expiry_1_with_skew = pd.merge(
            self.expiry_1,
            exp1_df,
            left_on="symbol",
            right_on="symbol",
        )

        self.expiry_2_with_skew = pd.merge(
            self.expiry_2,
            exp2_df,
            left_on="symbol",
            right_on="symbol",
        )

    def expiry(self, num: Literal[-1, 1, 2]):
        if num == -1:
            return self.expiry_1, self.expiry_2
        elif num == 1:
            return self.expiry_1
        elif num == 2:
            return self.expiry_2

    def expiry_with_skew(self, num: Literal[-1, 1, 2]):
        if num == -1:
            return self.expiry_1_with_skew, self.expiry_2_with_skew
        elif num == 1:
            return self.expiry_1_with_skew
        elif num == 2:
            return self.expiry_2_with_skew


memory_skew_benchmark = Skew_Benchmark()
