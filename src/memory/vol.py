from typing import Literal, Optional
import pandas as pd
import json
from utils.common import network_asset2df, CONFIG
from datetime import datetime
from .atmiv import memory_atm_iv

VOL_TRACKER_PERC = CONFIG["VOL_TRACKER_PERC"]

class Vol:
    def __init__(self):
        self.expiry_1: pd.DataFrame = pd.DataFrame()
        self.expiry_1_with_atm: pd.DataFrame = pd.DataFrame()
        self.expiry_2: pd.DataFrame = pd.DataFrame()
        self.expiry_2_with_atm: pd.DataFrame = pd.DataFrame()

    def _initialize_df(self, filename: str) -> pd.DataFrame:
        df = network_asset2df(filename)
        df = df.drop(df.columns[0], axis=1)
        df["vol_benchmark"] = df["forward_vol"].astype(float).round(1)
        df["vol_benchmark_prev"] = df["forward_vol"].astype(float).round(1)
        df["forward_vol"] = df["forward_vol"].astype(float).round(1)
        df["vol_threshold_percentage"] = VOL_TRACKER_PERC/100
        df['vol_threshold_val'] = df["forward_vol"] * df["vol_threshold_percentage"]
        df["vol_up_updated_at"] = str(datetime.now())
        df["vol_down_updated_at"] = str(datetime.now())
        return df

    def initialize(self):
        self.expiry_1 = self._initialize_df("forward_vol_expiry_1.csv")
        self.expiry_2 = self._initialize_df("forward_vol_expiry_2.csv")
        self.expiry_1_with_atm = self.expiry_1.copy()
        self.expiry_2_with_atm = self.expiry_2.copy()
        self.expiry_1_up_display_df = pd.DataFrame(columns=self.expiry_1.columns)
        self.expiry_1_down_display_df = pd.DataFrame(columns=self.expiry_1.columns)
        self.expiry_2_up_display_df = pd.DataFrame(columns=self.expiry_2.columns)
        self.expiry_2_down_display_df = pd.DataFrame(columns=self.expiry_2.columns)

    def _update_benchmark(self, df, number: pd.DataFrame):
        current_time = datetime.now()
        step_1 = df.copy()
        # print(number, df[df["atm_iv"] == 0])
        # df = df[df['atm_iv'] != 0]
        # df = df[df['vol_benchmark'] != 0]
        df = df[df['forward_vol'] != 0]
        # Update vol_up_benchmark and vol_up_updated_at
        df.loc[
            (df["atm_iv"] != 0) & (df["atm_iv"] - df["vol_benchmark"]
            >= df['vol_threshold_val']),
            "vol_up_updated_at",
        ] = current_time
        
        df.loc[
            (df["atm_iv"] != 0) & (df["atm_iv"] - df["vol_benchmark"]
            >= df['vol_threshold_val']),
            "vol_benchmark_prev",
        ] = df['vol_benchmark']
        
        df.loc[
            (df["atm_iv"] != 0) & (df["atm_iv"] - df["vol_benchmark"]
            >= df['vol_threshold_val']),
            "vol_benchmark",
        ] = df['vol_benchmark'] + df["vol_threshold_val"]

        # Update vol_down_benchmark and vol_down_updated_at
        df.loc[
            (df["atm_iv"] != 0) & (df["vol_benchmark"] - df["atm_iv"]
            > df['vol_threshold_val']),
            "vol_down_updated_at",
        ] = current_time
        
        df.loc[
            (df["atm_iv"] != 0) & (df["vol_benchmark"] - df["atm_iv"]
            > df['vol_threshold_val']),
            "vol_benchmark_prev",
        ] = df['vol_benchmark']
        
        df.loc[
            (df["atm_iv"] != 0) & (df["vol_benchmark"] - df["atm_iv"]
            > df['vol_threshold_val']),
            "vol_benchmark",
        ] = df['vol_benchmark'] - df['vol_threshold_val']
        
        # Store for disply
        
        vol_up_df = df[df['vol_up_updated_at'] == current_time]
        vol_down_df = df[df['vol_down_updated_at'] == current_time]
        
        df["vol_up_updated_at"] = df["vol_up_updated_at"].astype(str)
        df["vol_down_updated_at"] = df["vol_down_updated_at"].astype(str)
        
        vol_up_df["vol_up_updated_at"] = vol_up_df["vol_up_updated_at"].astype(str)
        vol_down_df["vol_down_updated_at"] = vol_down_df["vol_down_updated_at"].astype(str)
        
        return df, vol_up_df, vol_down_df

    def update(self):
        temp_1 = memory_atm_iv.expiry(1)[['symbol', 'atm_iv', 'fwd_iv', 'pct_change']].copy()
        # temp_1["pct_change"] = (-1)*temp_1["pct_change"]
        # print("######################------temp1-------", len(temp_1["symbol"].unique()))
        temp_exp1_with_atm = pd.merge(
            self.expiry_1_with_atm[self.expiry_1.columns],
            temp_1,
            on="symbol",
            how="left",
        )
        # print("################### 1 \n", self.expiry_1_with_atm)
        self.expiry_1_with_atm, temp_up_1, temp_down_1 = self._update_benchmark(temp_exp1_with_atm, 1)
        self.expiry_1_up_display_df = pd.concat([temp_up_1, self.expiry_1_up_display_df], ignore_index=True)
        self.expiry_1_down_display_df = pd.concat([temp_down_1, self.expiry_1_down_display_df], ignore_index=True)

        final_store_1_up = self.expiry_1_up_display_df.drop("atm_iv", axis=1)
        final_store_1_down = self.expiry_1_down_display_df.drop("atm_iv", axis=1)

        self.expiry_1_up_display_df = pd.merge(final_store_1_up, temp_1[["symbol", "atm_iv"]], on="symbol", how="left")
        self.expiry_1_down_display_df = pd.merge(final_store_1_down, temp_1[["symbol", "atm_iv"]], on="symbol", how="left")

        temp_2 = memory_atm_iv.expiry(2)[['symbol', 'atm_iv', 'fwd_iv', 'pct_change']].copy()

        temp_exp2_with_atm = pd.merge(

            self.expiry_2_with_atm[self.expiry_2.columns],
            temp_2,
            on="symbol",
            how="left",
        )

        self.expiry_2_with_atm, temp_up_2, temp_down_2 = self._update_benchmark(temp_exp2_with_atm, 2)
        self.expiry_2_up_display_df = pd.concat([temp_up_2, self.expiry_2_up_display_df], ignore_index=True)
        self.expiry_2_down_display_df = pd.concat([temp_down_2, self.expiry_2_down_display_df], ignore_index=True)

        final_store_2_up = self.expiry_2_up_display_df.drop("atm_iv", axis=1)
        final_store_2_down = self.expiry_2_down_display_df.drop("atm_iv", axis=1)

        self.expiry_2_up_display_df = pd.merge(final_store_2_up, temp_2[["symbol", "atm_iv"]], on="symbol", how="left")
        self.expiry_2_down_display_df = pd.merge(final_store_2_down, temp_2[["symbol", "atm_iv"]], on="symbol", how="left")


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
    
    def expiry_with_atm_display(self, num: Literal[-1, 1, 2]):
        if num == -1:
            return (self.expiry_1_up_display_df, 
                    self.expiry_1_down_display_df, 
                    self.expiry_2_up_display_df, 
                    self.expiry_2_down_display_df,
                    )
        elif num == 1:
            return self.expiry_1_up_display_df, self.expiry_1_down_display_df
        elif num == 2:
            return self.expiry_2_up_display_df, self.expiry_2_down_display_df

memory_vol = Vol()
