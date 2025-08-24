from typing import Literal
import pandas as pd
import numpy as np
import json
from contants.dates import EXPIRY
from utils.api import get_all_token_set
from utils.logger import logger
from .atmiv import memory_atm_iv
from datetime import datetime
from utils.common import CONFIG


CAL_CALL_UPPER_DELTA = CONFIG["CAL_CALL_UPPER_DELTA"]
CAL_CALL_LOWER_DELTA = CONFIG["CAL_CALL_LOWER_DELTA"]

CAL_PUT_UPPER_DELTA = CONFIG["CAL_PUT_UPPER_DELTA"]
CAL_PUT_LOWER_DELTA = CONFIG["CAL_PUT_LOWER_DELTA"]

CAL_LIQUIDITY_CHECK_CURRENT = CONFIG["CAL_LIQUIDITY_CHECK_CURRENT"]
CAL_LIQUIDITY_CHECK_CURRENT = 999 if CAL_LIQUIDITY_CHECK_CURRENT == 0 else 0.05

CAL_LOW_IVP_LIMIT = CONFIG["CAL_LOW_IVP_LIMIT"]
CAL_HIGH_IVP_LIMIT = CONFIG["CAL_HIGH_IVP_LIMIT"]

# CONFIG["CAL_LOW_IVP_LIMIT"] = 25

# CONFIG_FILE = open("scan_config.json", "w")
# json.dump(CONFIG, CONFIG_FILE, indent=2)
# CONFIG_FILE.close()

date_today = datetime.now().date()

class CALENDARS:
    def __init__(self):
        self.get_token_data_pe_dump: pd.DataFrame = pd.DataFrame()
        self.get_token_data_ce_dump: pd.DataFrame = pd.DataFrame()
        self.calendars_1: pd.DataFrame = pd.DataFrame()
        self.calendars_2: pd.DataFrame = pd.DataFrame()
        self.calendars_1_low_ivp: pd.DataFrame = pd.DataFrame()
        self.calendars_1_high_ivp: pd.DataFrame = pd.DataFrame()
        self.calendars_2_low_ivp: pd.DataFrame = pd.DataFrame()
        self.calendars_2_high_ivp: pd.DataFrame = pd.DataFrame()
        self.display_df_1: pd.DataFrame = pd.DataFrame()
        self.display_df_2: pd.DataFrame = pd.DataFrame()
        self.last_trade_time_1: str = f"{date_today} 00:00:00"
        self.last_trade_time_2: str = f"{date_today} 00:00:00"
    
    def update_calendar_settings(
        self,
        low_ivp: float,
        high_ivp: float,
    ):
        global CAL_LOW_IVP_LIMIT, CAL_HIGH_IVP_LIMIT
        CAL_LOW_IVP_LIMIT = low_ivp
        CAL_HIGH_IVP_LIMIT = high_ivp

        CONFIG["CAL_LOW_IVP_LIMIT"] = low_ivp
        CONFIG["CAL_HIGH_IVP_LIMIT"] = high_ivp

        CONFIG_FILE = open("scan_config.json", "w")
        json.dump(CONFIG, CONFIG_FILE, indent=2)
        CONFIG_FILE.close()
        
        # Update the benchmark and timestamp in the merged DataFrame
        self.update()

    def find_closest_delta(self, row, net_df_current):

        filtered_df = net_df_current[net_df_current['symbol'] == row['symbol']]
        if filtered_df.empty:
            return 0, "NA", 0, 0
        
        filtered_df['delta_diff'] = abs(filtered_df['params.delta'] - row['params.delta'])
        idx_min_delta = filtered_df['delta_diff'].idxmin()
        closest_row = filtered_df.loc[idx_min_delta]
    
        return closest_row['strike_price'], closest_row["pk.asset_type"], closest_row['params.delta'], closest_row['params.last_iv']
    
    def calendar_init_cleaner(self, call_df, put_df, last_trade_time, expiry_current, expiry_next):

        ce_df_current = call_df[call_df["markers.expiry"] == EXPIRY[expiry_current]]
        ce_df_current = ce_df_current[ce_df_current["params.liquidity"]<=CAL_LIQUIDITY_CHECK_CURRENT]
        pe_df_current = put_df[put_df["markers.expiry"] == EXPIRY[expiry_current]]
        pe_df_current = pe_df_current[pe_df_current["params.liquidity"]<=CAL_LIQUIDITY_CHECK_CURRENT]

        net_df_current = pd.concat([ce_df_current, pe_df_current], ignore_index=True)
        net_df_current["params.delta"] = round(net_df_current["params.delta"],2)

        ce_df_next = call_df[call_df["markers.expiry"] == EXPIRY[expiry_next]]
        pe_df_next = put_df[put_df["markers.expiry"] == EXPIRY[expiry_next]]
        ce_df_next['params.ltt'] = pd.to_datetime(ce_df_next['params.ltt']).astype(str)
        pe_df_next['params.ltt'] = pd.to_datetime(pe_df_next['params.ltt']).astype(str)       

        net_df = pd.concat([ce_df_next, pe_df_next], ignore_index=True)
        net_df.sort_values(by="params.ltt", inplace=True, ascending=False)
        net_df = net_df[(net_df["symbol"] != "NIFTY")&(net_df["symbol"] != "BANKNIFTY")]

        df_init_processed = net_df[net_df["params.ltt"]>=last_trade_time]

        df_init_processed["sort_time"] = pd.Timestamp.now()
        df_init_processed = df_init_processed[["symbol", "strike_price", "pk.asset_type", "params.ltt", "params.delta", "params.last_iv", "sort_time"]]

        df_init_processed["params.delta"] = round(df_init_processed["params.delta"],2)

        # print(df_init_processed)
        # print(df_init_processed.apply(
        #     lambda row: pd.Series(self.find_closest_delta(row, net_df_current)), axis=1
        # ))
        
        df_init_processed[['strike_price_current', 'current_opt_type', 'closest_delta', 'closest_iv']] = df_init_processed.apply(
            lambda row: pd.Series(self.find_closest_delta(row, net_df_current)), axis=1
        )

        return df_init_processed
    
    def calendar_fin_cleaner(self, display_df, expiry):

        pct_change_df = memory_atm_iv.expiry(expiry).copy()
        pct_change_df = pct_change_df[["symbol", "pct_change", "ivp"]]

        merged_cal_df = pd.merge(display_df, pct_change_df, on="symbol", how="left")
        merged_cal_df = merged_cal_df[(merged_cal_df["symbol"]!="NIFTY")&(merged_cal_df["symbol"]!="BANKNIFTY")]
        merged_cal_df["ivp"] = merged_cal_df["ivp"]*100
        merged_cal_df["params.last_iv"] = merged_cal_df["params.last_iv"]*100
        merged_cal_df["closest_iv"] = merged_cal_df["closest_iv"]*100
        merged_cal_df["diff"] = merged_cal_df["params.last_iv"] - merged_cal_df["closest_iv"]

        merged_cal_df.rename(columns = {"closest_iv": "iv_current", "closest_delta": "current_delta", 
        "strike_price": "strike_price_next", "params.last_iv": "iv_next", "params.delta": "next_delta",
        "params.ltt": "ltt", "pk.asset_type": "opt_type"}, inplace=True)

        merged_cal_df['opt_type'] = merged_cal_df['opt_type'].replace({'CallOption': 'CE', 'PutOption': 'PE'})
        merged_cal_df['current_opt_type'] = merged_cal_df['current_opt_type'].replace({'CallOption': 'CE', 'PutOption': 'PE'})

        # merged_cal_df["diff"] = merged_cal_df["iv_next"] - merged_cal_df["iv_current"]

        # merged_cal_df.sort_values(by="diff", ascending=True, inplace=True)
        
        # merged_cal_df = pd.DataFrame(columns=["pct_change", "symbol", "strike_price_current", "iv_current", "strike_price_next"
        #                      "iv_next", "ivp"])
        # print(CAL_LOW_IVP_LIMIT, CAL_HIGH_IVP_LIMIT)
        low_ivp_df = merged_cal_df[merged_cal_df["ivp"]<=CAL_LOW_IVP_LIMIT]
        high_ivp_df = merged_cal_df[merged_cal_df["ivp"]>=CAL_HIGH_IVP_LIMIT]

        low_ivp_df_pos = low_ivp_df[low_ivp_df["diff"]>=0]
        low_ivp_df_neg = low_ivp_df[low_ivp_df["diff"]<0]
        high_ivp_df_pos = high_ivp_df[high_ivp_df["diff"]>=0]
        high_ivp_df_neg = high_ivp_df[high_ivp_df["diff"]<0]

        return merged_cal_df, low_ivp_df_pos, low_ivp_df_neg, high_ivp_df_pos, high_ivp_df_neg

    def _process_calendar(self):

        merged_cal_df = pd.DataFrame()
        data = get_all_token_set(delta={"pe": ["-0.6","-0.05"], "ce": ["0.05","0.6"]}, expiry=[EXPIRY[0], EXPIRY[1], EXPIRY[2]]).copy()

        self.get_token_pe_dump = pd.json_normalize(data, record_path=['markers', 'pe_data'], meta=[['symbol'], ['markers', 'expiry']])
        pe_df = self.get_token_pe_dump[(self.get_token_pe_dump["params.delta"]<=CAL_PUT_UPPER_DELTA)&(self.get_token_pe_dump["params.delta"]>=CAL_PUT_LOWER_DELTA)]
        self.get_token_ce_dump = pd.json_normalize(data, record_path=['markers', 'ce_data'], meta=[['symbol'], ['markers', 'expiry']])
        ce_df = self.get_token_ce_dump[(self.get_token_ce_dump["params.delta"]<=CAL_CALL_UPPER_DELTA)&(self.get_token_ce_dump["params.delta"]>=CAL_CALL_LOWER_DELTA)]
        # ce_df = ce_df[ce_df["params.liquidity"]<=0.05]     

        new_df = self.calendar_init_cleaner(ce_df, pe_df, self.last_trade_time_1, 0, 1)
        self.display_df_1 = pd.concat([new_df, self.display_df_1], ignore_index=True)
        self.last_trade_time_1 = max(new_df["params.ltt"]) if not new_df.empty else self.last_trade_time_1
        self.display_df_1.drop_duplicates(subset=["symbol", "strike_price", "pk.asset_type", "params.ltt"], inplace=True)
        self.display_df_1 = self.display_df_1.head(500)
        self.display_df_1.sort_values(by=["sort_time", "params.ltt", "symbol", "strike_price"], inplace=True, ascending=[False, False, False, False])

        new_df_2 = self.calendar_init_cleaner(ce_df, pe_df, self.last_trade_time_2, 1, 2)
        self.display_df_2 = pd.concat([new_df_2, self.display_df_2], ignore_index=True)
        self.last_trade_time_2 = max(new_df_2["params.ltt"]) if not new_df_2.empty else self.last_trade_time_2
        self.display_df_2.drop_duplicates(subset=["symbol", "strike_price", "pk.asset_type", "params.ltt"], inplace=True)
        self.display_df_2 = self.display_df_2.head(500)
        self.display_df_2.sort_values(by=["sort_time", "params.ltt", "symbol", "strike_price"], inplace=True, ascending=[False, False, False, False])
        
        merged_cal_df, low_ivp_df_pos, low_ivp_df_neg, high_ivp_df_pos, high_ivp_df_neg = self.calendar_fin_cleaner(self.display_df_1, 1)
        merged_cal_df_2, low_ivp_df_pos_2, low_ivp_df_neg_2, high_ivp_df_pos_2, high_ivp_df_neg_2 = self.calendar_fin_cleaner(self.display_df_2, 2)

        return (merged_cal_df, low_ivp_df_pos, low_ivp_df_neg, high_ivp_df_pos, high_ivp_df_neg,
                merged_cal_df_2, low_ivp_df_pos_2, low_ivp_df_neg_2, high_ivp_df_pos_2, high_ivp_df_neg_2)

    def update(self):
        (processed_cal_df, processed_cal_df_low_ivp_pos, processed_cal_df_low_ivp_neg, processed_cal_df_high_ivp_pos, processed_cal_df_high_ivp_neg,
        processed_cal_df_2, processed_cal_df_low_ivp_2_pos, processed_cal_df_low_ivp_2_neg, processed_cal_df_high_ivp_2_pos, processed_cal_df_high_ivp_2_neg) = (
            self._process_calendar()
        )
        if processed_cal_df is not None:
            self.calendars_1 = processed_cal_df
        if processed_cal_df_low_ivp_pos is not None:
            self.calendars_1_low_ivp_pos = processed_cal_df_low_ivp_pos
        if processed_cal_df_low_ivp_neg is not None:
            self.calendars_1_low_ivp_neg = processed_cal_df_low_ivp_neg
        if processed_cal_df_high_ivp_pos is not None:
            self.calendars_1_high_ivp_pos = processed_cal_df_high_ivp_pos
        if processed_cal_df_high_ivp_neg is not None:
            self.calendars_1_high_ivp_neg = processed_cal_df_high_ivp_neg
        if processed_cal_df_2 is not None:
            self.calendars_2 = processed_cal_df_2
        if processed_cal_df_low_ivp_2_pos is not None:
            self.calendars_2_low_ivp_pos = processed_cal_df_low_ivp_2_pos
        if processed_cal_df_low_ivp_2_neg is not None:
            self.calendars_2_low_ivp_neg = processed_cal_df_low_ivp_2_neg
        if processed_cal_df_high_ivp_2_pos is not None:
            self.calendars_2_high_ivp_pos = processed_cal_df_high_ivp_2_pos
        if processed_cal_df_high_ivp_2_neg is not None:
            self.calendars_2_high_ivp_neg = processed_cal_df_high_ivp_2_neg

    def get_data(self, num: Literal[-1, 1, 2, 3]):
        if num == 1:
            return (self.calendars_1, self.calendars_1_low_ivp, self.calendars_1_high_ivp)
        elif num == 2:
            return (self.calendars_2, self.calendars_2_low_ivp, self.calendars_2_high_ivp)
        elif num == 3:
            return (self.calendars_1, self.calendars_1_low_ivp_pos, self.calendars_1_low_ivp_neg, self.calendars_1_high_ivp_pos, self.calendars_1_high_ivp_neg,
            self.calendars_2, self.calendars_2_low_ivp_pos, self.calendars_2_low_ivp_neg, self.calendars_2_high_ivp_pos, self.calendars_2_high_ivp_pos)
        else:
            return None
    
    def get_token_dump_pe(self):
        return self.get_token_pe_dump
    
    def get_token_dump_ce(self):
        return self.get_token_ce_dump


memory_calendars = CALENDARS()
