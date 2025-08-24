import os
from typing import Literal
import pandas as pd
import numpy as np
import json
from datetime import datetime
from contants.dates import EXPIRY
from utils.common import asset2df, ASSET_DIR, cols_2, CONFIG
from utils.api import get_atm_iv_from_api, get_z_score_from_api
from utils.logger import logger

date_today = datetime.now().date()
idv_file_path_exists = os.path.join(ASSET_DIR, "idv_cal", f"{date_today}.csv")
idv_file_path = os.path.join("idv_cal", f"{date_today}.csv")

MOVE_TRACKER_VAL = CONFIG["MOVE_TRACKER_VAL"]

class ATM_IV:
    def __init__(self):
        self.expiry_1: pd.DataFrame = pd.DataFrame()
        self.expiry_2: pd.DataFrame = pd.DataFrame()
        self.expiry_3: pd.DataFrame = pd.DataFrame()
        
        self.expiry_1_store: pd.DataFrame = pd.DataFrame(columns=cols_2)
        self.expiry_2_store: pd.DataFrame = pd.DataFrame(columns=cols_2)
        self.expiry_3_store: pd.DataFrame = pd.DataFrame(columns=cols_2)
        self.all_expiry_store: pd.DataFrame = pd.DataFrame(columns=cols_2)

        self.move_tracker: pd.DataFrame = pd.DataFrame()
    
    def _initialize_df(self, filename: str) -> pd.DataFrame:
        df = asset2df(filename)
        df = df[["symbol", "long_move", "full_move", "fut_close"]]
        df.rename(columns={"long_move": "long_move_val", "fut_close": "fut_benchmark"}, inplace=True)
        df["fut_benchmark"] = df["fut_benchmark"].astype(float)
        df["fut_benchmark_track"] = df["fut_benchmark"].astype(float)
        df[["long_moves", "long_moves_track", "days_theta", "idv_updated_time", "idv_updated_time_track"]] = 0,0,0,str(datetime.now()),str(datetime.now())
        df["long_move_val"] = df["long_move_val"].fillna(9999999)
        df["long_move_val_track"] = df["full_move"].fillna(9999999)
        df["long_move_val_track"] = df["long_move_val_track"]*MOVE_TRACKER_VAL
        # print(df)
        return df
    
    def initialize(self):
        if os.path.exists(idv_file_path_exists):
            self.idv_cal: pd.DataFrame = asset2df(idv_file_path)
            if not self.idv_cal.empty:

                self.idv_cal = self.idv_cal[["symbol", "long_move_val", "long_move_val_track", "fut_benchmark", "fut_benchmark_track", "long_moves", 
                                             "long_moves_track", "days_theta", "idv_updated_time", "idv_updated_time_track"]]
            else:
                self.idv_cal: pd.DataFrame = self._initialize_df("iv_stats.csv")

        else:
            self.idv_cal: pd.DataFrame = self._initialize_df("iv_stats.csv")
        
        print(self.idv_cal)
    

    def calc_idv(self, df):
        # df = df[df["expiry"] == (df["expiry"]).unique()[0]]
        current_time = datetime.now()

        condition_check = (abs(df["ltp"] - df["fut_benchmark"]) > df["long_move_val"]) & (df["pct_change"] > -800)
        
        df.loc[
            condition_check,
            "idv_updated_time",
        ] = str(current_time)
        
        df.loc[
            condition_check,
            "long_moves",
        ] = df["long_moves"] + np.round(abs(df["ltp"] - df["fut_benchmark"])/df["long_move_val"])
        
        df["days_theta"] = df["long_moves"]/2
        
        check_df = df[condition_check]
        
        df.loc[
            condition_check,
            "fut_benchmark",
        ] = df["ltp"]

        ############################################################################################################

        condition_check_1 = (abs(df["ltp"] - df["fut_benchmark_track"]) > df["long_move_val_track"]) & (df["pct_change"] > -800)
        
        df.loc[
            condition_check_1,
            "long_moves_track",
        ] = df["long_moves_track"] + round(abs(df["ltp"] - df["fut_benchmark_track"])/df["long_move_val_track"])
        
        check_df_1 = df[condition_check_1][["symbol", "ltp", "pct_change", "fut_benchmark_track", "long_moves_track", "idv_updated_time_track"]]
        check_df_1["current_time"] = str(current_time)
        # print(check_df_1[["symbol", "ltp", "fut_benchmark_track", "long_moves_track"]])
        
        df.loc[
            condition_check_1,
            "fut_benchmark_track",
        ] = df["ltp"]

        df.loc[
            condition_check_1,
            "idv_updated_time_track",
        ] = str(current_time)

        # print(df[condition_check][["ltp", "fut_benchmark"]])
        
        if (len(check_df) > 0):
            # print(check_df[["symbol", "ltp", "fut_benchmark", "idv_updated_time", "long_moves", "days_theta"]].sort_values(by="long_moves"))
            new_df = df[self.idv_cal.columns]
            filename = idv_file_path
            file_path = os.path.join(ASSET_DIR, filename)
            new_df.to_csv(file_path)
        
        return df, check_df_1

    def _process_atm_iv(self):
        atm_iv_data = get_atm_iv_from_api().copy()
        
        # z_score_data = get_z_score_from_api().copy()
        if atm_iv_data is not None : #and z_score_data is not None:
            df = pd.DataFrame(atm_iv_data)

            columns_to_drop = ["pk", "atm_iv_pk", "errors"]
            df = df.drop(columns=columns_to_drop)
            df = df.explode(
                ["expiry", "atm_strike", "type", "delta", "ltp", "atm_iv", "iv_stats", "percent_change", "ivp"]
            )
            # print(df[df["symbol"]=="NIFTY"])
            # Convert numerical columns to appropriate data types
            df["pct_change"] = df["percent_change"].apply(lambda x: x['fut'])
            df["delta"] = df["delta"].astype(float)
            df["atm_iv"] = df["atm_iv"].astype(float)
            df["ltp"] = df["ltp"].astype(float)

            # df2 = pd.DataFrame(z_score_data)
            # df2 = df2.explode(
            #     [
            #         "expiry",
            #         "pe_pe_z_score",
            #         "ce_ce_z_score",
            #         "pe_ce_z_score",
            #         "four_leg_z_score",
            #         "pe_pe_ivp",
            #         "ce_ce_ivp",
            #         "pe_ce_ivp",
            #         "four_leg_ivp",
            #     ]
            # )

            # merged_df = pd.merge(df, df2, on=["symbol", "expiry"], how="inner")
            merged_df = df

            expiry_groups = {
                expiry: merged_df[
                    merged_df["expiry"] == expiry] for expiry in merged_df["expiry"].unique()
            }
            
            expiry_1_api_df = expiry_groups.get(EXPIRY[0])
            # print(expiry_1_api_df[expiry_1_api_df["symbol"]=="NIFTY"])
            expiry_2_api_df = expiry_groups.get(EXPIRY[1])
            expiry_3_api_df = expiry_groups.get(EXPIRY[2])
            
            self.expiry_1_store = pd.concat([self.expiry_1_store, expiry_1_api_df], ignore_index=True)
            self.expiry_2_store = pd.concat([self.expiry_2_store, expiry_2_api_df], ignore_index=True)
            self.expiry_3_store = pd.concat([self.expiry_3_store, expiry_3_api_df], ignore_index=True)
            
            self.expiry_1_store = self.expiry_1_store.drop_duplicates(subset="symbol", keep="last").reset_index(drop=True)
            self.expiry_2_store = self.expiry_2_store.drop_duplicates(subset="symbol", keep="last").reset_index(drop=True)
            self.expiry_3_store = self.expiry_3_store.drop_duplicates(subset="symbol", keep="last").reset_index(drop=True)

            # print("atm", self.expiry_1_store[cols_2].columns)

            idv_cal_df = pd.merge(self.expiry_1_store[cols_2], self.idv_cal, on="symbol", how="left")
            idv_cal_df, move_tracker_store = self.calc_idv(idv_cal_df)
            
            self.move_tracker = pd.concat([move_tracker_store, self.move_tracker], ignore_index=True)
            idv_cal_df.fillna(0, inplace=True)
            
            # print(idv_cal_df[self.idv_cal.columns].sort_values(by="idv_updated_time"))
            self.idv_cal = idv_cal_df[self.idv_cal.columns]

            self.expiry_1_store = pd.merge(self.expiry_1_store[cols_2], idv_cal_df[["symbol", "days_theta"]], on="symbol", how="left")
            self.expiry_2_store = pd.merge(self.expiry_2_store[cols_2], idv_cal_df[["symbol", "days_theta"]], on="symbol", how="left")
            self.expiry_3_store = pd.merge(self.expiry_3_store[cols_2], idv_cal_df[["symbol", "days_theta"]], on="symbol", how="left")

            self.all_expiry_store = pd.concat([self.expiry_1_store, self.expiry_2_store, self.expiry_3_store], ignore_index=True)
            
            # print(len(self.expiry_1_store["symbol"].unique()))
            # print(self.expiry_1_store[~self.expiry_1_store["symbol"].isin(self.expiry_2_store["symbol"].unique())])
            # print(self.expiry_1_store[~self.expiry_1_store["symbol"].isin(self.expiry_3_store["symbol"].unique())])
            
            # print(self.expiry_1_store[self.expiry_1_store["symbol"] == "NIFTY"])
            # print(len(self.expiry_1_store), len(self.expiry_2_store), len(self.expiry_3_store))
            
            return (
                self.expiry_1_store,
                self.expiry_2_store,
                self.expiry_3_store,
                self.move_tracker,
            )
        else:
            logger.warning("Error in processing ATM IV")
            return None, None, None

    def update(self):
        processed_expiry_1, processed_expiry_2, processed_expiry_3, processed_move_tracker = (
            self._process_atm_iv()
        )
        if processed_expiry_1 is not None:
            self.expiry_1 = processed_expiry_1
        if processed_expiry_2 is not None:
            self.expiry_2 = processed_expiry_2
        if processed_expiry_3 is not None:
            self.expiry_3 = processed_expiry_3
        if processed_move_tracker is not None:
            self.move_tracker = processed_move_tracker

    def expiry(self, num: Literal[0, -1, 1, 2, 3]):
        if num == 0:
            return self.all_expiry_store
        elif num == -1:
            return self.expiry_1, self.expiry_2, self.expiry_3
        elif num == 1:
            return self.expiry_1
        elif num == 2:
            return self.expiry_2
        else:
            return self.expiry_3
    
    def move_tracker_func(self):
        return self.move_tracker


memory_atm_iv = ATM_IV()
