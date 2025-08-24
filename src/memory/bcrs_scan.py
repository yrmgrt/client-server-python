import json
from typing import Literal
import pandas as pd
from contants.dates import EXPIRY
from utils.api import get_all_token_set
from utils.logger import logger
from .atmiv import memory_atm_iv
from .calendars import memory_calendars
from datetime import datetime
import os
from utils.common import ASSET_DIR, savedf, CONFIG

## BCRS INIT ##
BCRS_STRIKE_1_DELTA_LOW = CONFIG["BCRS_CALL_STRIKE_1_DELTA_LOW"]
BCRS_STRIKE_1_DELTA_HIGH = CONFIG["BCRS_CALL_STRIKE_1_DELTA_HIGH"]
BCRS_STRIKE_2_DELTA_DIFF = CONFIG["BCRS_CALL_STRIKE_2_MIN_DELTA_DIFF"]
BCRS_STRIKE_RATIO = CONFIG["BCRS_CALL_RATIO"]

bcrs_strike_1_delta = (BCRS_STRIKE_1_DELTA_LOW + BCRS_STRIKE_1_DELTA_HIGH)/2

BPRS_STRIKE_1_DELTA_LOW = CONFIG["BPRS_PUT_STRIKE_1_DELTA_LOW"]
BPRS_STRIKE_1_DELTA_HIGH = CONFIG["BPRS_PUT_STRIKE_1_DELTA_HIGH"]
BPRS_STRIKE_2_DELTA_DIFF = CONFIG["BPRS_PUT_STRIKE_2_MIN_DELTA_DIFF"]
BPRS_STRIKE_RATIO = CONFIG["BPRS_PUT_RATIO"]

bprs_strike_1_delta = (BPRS_STRIKE_1_DELTA_LOW + BPRS_STRIKE_1_DELTA_HIGH)/2

CVS_SAVE_TIME = CONFIG["BCRS_CSV_SAVE_TIME"]

BCRS_LIQUIDITY_CHECK = CONFIG["BCRS_LIQUIDITY_CHECK"]/100
BCRS_LIQUIDITY_CHECK = 999 if BCRS_LIQUIDITY_CHECK == 0 else BCRS_LIQUIDITY_CHECK

date_today = datetime.now().date()
date_today_pd = pd.to_datetime('today').date()


bcrs_file_path = os.path.join(ASSET_DIR, "ratio_spread_data", f"{date_today}_bull_call_ratio_spread.csv")
bprs_file_path = os.path.join(ASSET_DIR, "ratio_spread_data", f"{date_today}_bear_put_ratio_spread.csv")

## INDEX STRADDLE INIT ##


class BCRS:
    def __init__(self):
        self.bcrs_df: pd.DataFrame = pd.DataFrame()
        self.bprs_df: pd.DataFrame = pd.DataFrame()
        self.index_straddle_df: pd.DataFrame = pd.DataFrame()    
    def find_min_delta_diff(self, row, net_df, strike_diff_delta, pe_or_ce):

        filtered_df = net_df[net_df['symbol'] == row['symbol']]
        # if row["symbol"] == "AMBUJACEM":
        #     print(filtered_df)
        
        filtered_df['delta_diff'] = row['params.delta'] - filtered_df['params.delta']
        if pe_or_ce == "PE":
            filtered_df["delta_diff"] = filtered_df["delta_diff"]*(-1)
        filtered_df = filtered_df[filtered_df["delta_diff"] >= strike_diff_delta]
        filtered_df = filtered_df.sort_values(by="delta_diff", ascending=True)

        # if row["symbol"] == "AMBUJACEM":
        #     print(filtered_df)

        if filtered_df.empty:
            return 0, 0, 0

        closest_row = filtered_df.head(1)
    
        return closest_row['strike_price'].values[0], closest_row["opt_ltp"].values[0], closest_row['params.delta'].values[0]
    
    def _process_ratio_df(self, raw_df, strike_1_delta, strike_2_diff, strike_ratio, pe_ce):
        
        pe_or_ce_df = raw_df[raw_df["params.liquidity"]<=BCRS_LIQUIDITY_CHECK].copy()
        
        pct_change_df = memory_atm_iv.expiry(1).copy()
        pct_change_df = pct_change_df[["symbol", "pct_change", "ivp", "ltp"]]
        pct_change_df["ivp"] = (100)*pct_change_df["ivp"]
        
        merged_df = pd.merge(pe_or_ce_df, pct_change_df[["symbol", "pct_change", "ivp", "ltp"]], on="symbol", how="left")
        merged_df["strike_price"] = merged_df["strike_price"].astype(float)
        merged_df["opt_ltp"] = (merged_df["params.bid"].astype(float) + merged_df["params.ask"].astype(float))/2
        merged_df["delta_diff_near"] = abs(merged_df["params.delta"] - strike_1_delta)
        # merged_df["delta_diff_far"] = abs(merged_df["params.delta"] - strike_2_delta)

        merged_df = merged_df[["symbol", "pct_change", "ivp", "strike_price", "params.delta", "ltp", "delta_diff_near", "opt_ltp"]]

        bcrs_strikes_all = merged_df.sort_values(by=["symbol","delta_diff_near"], ascending=[True,True])
        # print(bcrs_strikes_all[bcrs_strikes_all["symbol"] == "AMBUJACEM"])
        bcrs_strikes = bcrs_strikes_all[abs(bcrs_strikes_all["params.delta"]-strike_1_delta)<=0.05]
        bcrs_strikes = bcrs_strikes_all.drop_duplicates(subset="symbol", keep="first").reset_index(drop=True)
        bcrs_strikes = bcrs_strikes[["symbol", "pct_change", "ivp", "strike_price", "opt_ltp", "params.delta", "ltp"]]
        # print(near_strikes)

        bcrs_strikes[['strike_price_far', 'opt_ltp_far', 'params.delta_far']] = bcrs_strikes.apply(
            lambda row: pd.Series(self.find_min_delta_diff(row, bcrs_strikes_all, strike_2_diff, pe_ce)), axis=1
        )
        
        bcrs_strikes["bcrs_val"] = bcrs_strikes["opt_ltp_far"]*strike_ratio - bcrs_strikes["opt_ltp"]
        bcrs_strikes["sort_val"] = bcrs_strikes["bcrs_val"]/bcrs_strikes["ltp"]
        # print(bcrs_strikes[bcrs_strikes["strike_price_far"] == 0])
        bcrs_strikes.sort_values(by="sort_val", ascending=False, inplace=True)
        bcrs_strikes = bcrs_strikes[bcrs_strikes["strike_price_far"] != 0]

        return bcrs_strikes
    
    def _process_straddle_index(self, index_data, index_atm_memory):

        merged_df = pd.merge(index_data, index_atm_memory[['symbol', 'expiry', 'ltp']], on=['symbol', 'expiry'], how='left')
        merged_df['abs_diff'] = (merged_df['strike_price'].astype(float) - merged_df['ltp']).abs()
        closest_strike = merged_df.loc[merged_df.groupby(['symbol', 'expiry'])['abs_diff'].idxmin()]
        selected_columns = closest_strike[['symbol', 'expiry', 'strike_price']]

        final_result = pd.merge(index_data, selected_columns, on=['symbol', 'expiry', 'strike_price'], how='inner')

        final_result["ltp"] = (final_result["params.bid"].astype(float) + final_result["params.ask"].astype(float))/2 

        straddle_df_ = final_result.groupby(['symbol', 'expiry', 'strike_price'])['ltp'].sum().reset_index()
        straddle_df_.sort_values(by=["symbol", "expiry"], inplace=True)

        straddle_df_['expiry_date'] = pd.to_datetime(straddle_df_['expiry']).dt.date

        straddle_df_['dte'] = (straddle_df_['expiry_date'] - date_today_pd).apply(lambda x: x.days) + 1
        straddle_df_['expiry'] = straddle_df_['expiry_date'].apply(lambda x: x.strftime('%d-%b-%Y'))
        straddle_df_["expiry"] = straddle_df_["expiry"].astype(str)
        # print(straddle_df_)
        return straddle_df_

    def _process_bcrs(self):
        pe_df = memory_calendars.get_token_dump_pe().copy()
        pe_df_current = pe_df[pe_df["markers.expiry"] == EXPIRY[0]]
        ce_df = memory_calendars.get_token_dump_ce().copy()
        ce_df_current = ce_df[ce_df["markers.expiry"] == EXPIRY[0]]

        atm_memory_df_1, atm_memory_df_2, atm_memory_df_3 = memory_atm_iv.expiry(1).copy(), memory_atm_iv.expiry(2).copy(), memory_atm_iv.expiry(3).copy()
        atm_memory_df_1 = atm_memory_df_1[atm_memory_df_1["symbol"].isin(["NIFTY", "BANKNIFTY"])]
        atm_memory_df_2 = atm_memory_df_2[atm_memory_df_2["symbol"].isin(["NIFTY", "BANKNIFTY"])]
        atm_memory_df_3 = atm_memory_df_3[atm_memory_df_3["symbol"].isin(["NIFTY", "BANKNIFTY"])]
        index_memory_df = pd.concat([atm_memory_df_1, atm_memory_df_2, atm_memory_df_3], ignore_index=True)

        net_token_df = pd.concat([pe_df, ce_df], ignore_index=True)
        index_token_df = net_token_df[(net_token_df["symbol"]).isin(["NIFTY", "BANKNIFTY"])]
        index_token_df["expiry"] = index_token_df["pk.expiry"]

        straddle_df = self._process_straddle_index(index_token_df, index_memory_df)
        
        bcrs_df = self._process_ratio_df(ce_df_current, bcrs_strike_1_delta, BCRS_STRIKE_2_DELTA_DIFF, BCRS_STRIKE_RATIO, "CE")
        bprs_df = self._process_ratio_df(pe_df_current, bprs_strike_1_delta, BPRS_STRIKE_2_DELTA_DIFF, BPRS_STRIKE_RATIO, "PE")

        if str(datetime.now().time())>=CVS_SAVE_TIME and not os.path.exists(bcrs_file_path):
            savedf("ratio_spread_data", f"{date_today}_bull_call_ratio_spread.csv", bcrs_df)
        
        if str(datetime.now().time())>=CVS_SAVE_TIME and not os.path.exists(bprs_file_path):
            savedf("ratio_spread_data", f"{date_today}_bear_put_ratio_spread.csv", bprs_df)
        
        return bcrs_df, bprs_df, straddle_df

    def update(self):
        processed_bcrs_df, processed_bprs_df, processed_straddle_df = (
            self._process_bcrs()
        )

        if processed_bcrs_df is not None:
            self.bcrs_df = processed_bcrs_df
        
        if processed_bprs_df is not None:
            self.bprs_df = processed_bprs_df

        if processed_straddle_df is not None:
            self.index_straddle_df = processed_straddle_df

    def get_data(self, num: Literal[-1, 1, 2, 3]):
        if num == 1:
            return (self.bcrs_df, self.bprs_df, self.index_straddle_df)
        else:
            return None


memory_bcrs = BCRS()
