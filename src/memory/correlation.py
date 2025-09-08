import pandas as pd
import numpy as np
from typing import Literal
from .atmiv import memory_atm_iv
from utils.common import network_asset2df
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings("ignore")

class Correlation:
    def __init__(self):
        self.expiry_1: pd.DataFrame = pd.DataFrame()
        self.expiry_1_with_atm: pd.DataFrame = pd.DataFrame()
        self.expiry_2: pd.DataFrame = pd.DataFrame()
        self.expiry_2_with_atm: pd.DataFrame = pd.DataFrame()

    def _initialize_df(self, filename: str) -> pd.DataFrame:
        df = network_asset2df(filename)
        columns_to_keep = ["stock_1", "stock_2", "avg_ratio"]
        return df[columns_to_keep]
    
    def _add_reverse_pairs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add reverse pairs to the DataFrame with inverted ratio and avg_ratio.
        """
        # Create a reversed DataFrame
        reversed_df = df.rename(columns={"stock_1": "stock_2", "stock_2": "stock_1"}).copy()
        
        # Invert avg_ratio
        reversed_df["avg_ratio"] = 1 / df["avg_ratio"]
        
        # Concatenate original and reversed DataFrames
        full_df = pd.concat([df, reversed_df], ignore_index=True)
        
        return full_df

    def initialize(self):
        self.expiry_1 = self._initialize_df("correlation.csv")
        self.expiry_2 = self._initialize_df("correlation.csv")

        self.expiry_1['avg_ratio'] = 1/ self.expiry_1['avg_ratio']

        self.expiry_2['avg_ratio'] = 1/self.expiry_2['avg_ratio']
        
        # Generate reverse pairs for both expiries during initialization
        self.expiry_1 = self._add_reverse_pairs(self.expiry_1).copy()
        self.expiry_2 = self._add_reverse_pairs(self.expiry_2).copy()
    
    
    def _get_atm_df(self, num: Literal[1, 2]) -> pd.DataFrame:
        columns_to_keep_atm_df = ["symbol", "atm_iv"]
        atm_df = memory_atm_iv.expiry(num).copy()
        return atm_df[columns_to_keep_atm_df]

    def _update_ratio(self, correlation_df: pd.DataFrame, atm_df: pd.DataFrame) -> pd.DataFrame:
        merged_df1 = (
            pd.merge(correlation_df, atm_df, left_on="stock_1", right_on="symbol", how="left")
            .drop(columns="symbol")
            .rename(columns={"atm_iv": "atm_iv_1"})
        ).drop_duplicates()

        merged_df2 = (
            pd.merge(correlation_df, atm_df, left_on="stock_2", right_on="symbol", how="left")
            .drop(columns="symbol")
            .rename(columns={"atm_iv": "atm_iv_2"})
        ).drop_duplicates()

        result = pd.concat([merged_df1, merged_df2[["atm_iv_2"]]], axis=1)
        result['ratio'] = result['atm_iv_1'] / result['atm_iv_2'].replace(0, np.nan)
        result['sort_ratio'] =  result['ratio'] / result['avg_ratio']
        result = result.drop_duplicates(subset=["stock_1", "stock_2"]).sort_values(by="sort_ratio", ascending=False).reset_index(drop=True)
    
        # print(result[result["atm_iv_2"] != 0])
        return result

    def _update_expiry(self, expiry_num: int) -> pd.DataFrame:
        correlation_df = getattr(self, f"expiry_{expiry_num}")
        atm_df = self._get_atm_df(expiry_num)
        return self._update_ratio(correlation_df, atm_df)

    def update(self):
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._update_expiry, 1): "expiry_1_with_atm",
                executor.submit(self._update_expiry, 2): "expiry_2_with_atm",
            }
            for future in futures:
                setattr(self, futures[future], future.result())

    def expiry(self, num: Literal[-1, 1, 2]):
        if num == -1:
            return self.expiry_1, self.expiry_2
        elif num == 1:
            return self.expiry_1
        elif num == 2:
            return self.expiry_2

    def expiry_with_atm(self, num: Literal[-1, 1, 2]):
        if num == -1:
            return self.expiry_1_with_atm, self.expiry_2_with_atm
        elif num == 1:
            return self.expiry_1_with_atm
        elif num == 2:
            return self.expiry_2_with_atm


memory_correlation = Correlation()
