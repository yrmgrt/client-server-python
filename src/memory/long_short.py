from typing import Literal
import pandas as pd
from contants.dates import EXPIRY
from utils.api import get_all_token_set
from utils.logger import logger
from .atmiv import memory_atm_iv
import warnings
import json
from utils.common import asset2df, network_asset2df, CONFIG
from datetime import datetime, timedelta
warnings.filterwarnings("ignore")

NO_OF_DAYS_TO_RESULT = CONFIG["NO_OF_DAYS_TO_RESULT"]

class LS_IV:
    def __init__(self):
        self.expiry_1_short: pd.DataFrame = pd.DataFrame()
        self.expiry_1_long: pd.DataFrame = pd.DataFrame()
        self.expiry_2_short: pd.DataFrame = pd.DataFrame()
        self.expiry_2_long: pd.DataFrame = pd.DataFrame()
        self.expiry_1_short_result: pd.DataFrame = pd.DataFrame()
        self.expiry_1_long_result: pd.DataFrame = pd.DataFrame()
        self.expiry_2_short_result: pd.DataFrame = pd.DataFrame()
        self.expiry_2_long_result: pd.DataFrame = pd.DataFrame()

    def process_result_dates(self, df, date_1, date_2):
        filtered_df = df[(df['date'] >= date_1) & (df['date'] <= date_2)]
        symbols_list = filtered_df['symbol'].tolist()
        return symbols_list

    def initialize(self):
        self.expiry_1 = network_asset2df("avg_risk_prem.csv")
        today = datetime.today().date()
        future_date = today + timedelta(days=NO_OF_DAYS_TO_RESULT)
        future_date = str(future_date)
        today = str(today)
        
        confirmed_df = network_asset2df("confirm_results.csv")
        expected_df = asset2df("expected_results.csv")
        expected_df = expected_df[~expected_df["symbol"].isin(confirmed_df["symbol"].tolist())]

        expected_df_current = expected_df[(expected_df["date"]>=today)&(expected_df["date"]<EXPIRY[0])]
        expected_df_next = expected_df[(expected_df["date"]>=EXPIRY[0])&(expected_df["date"]<EXPIRY[1])]
        confirmed_df_current = confirmed_df[(confirmed_df["date"]>=today)&(confirmed_df["date"]<EXPIRY[0])]
        confirmed_df_next = confirmed_df[(confirmed_df["date"]>=EXPIRY[0])&(confirmed_df["date"]<EXPIRY[1])]

        self.expiry_1_fwd = network_asset2df("forward_vol_expiry_1.csv")
        self.expiry_2_fwd = network_asset2df("forward_vol_expiry_2.csv")

        self.result_confirmed_current = confirmed_df_current["symbol"].tolist()
        self.result_confirmed_next = confirmed_df_next["symbol"].tolist()
        self.result_expected_current = expected_df_current["symbol"].tolist()
        self.result_expected_next = expected_df_next["symbol"].tolist()
        
        self.near_result_confirmed_current = self.process_result_dates(confirmed_df_current, today, future_date)
        self.near_result_confirmed_next = self.process_result_dates(confirmed_df_next, today, future_date)
        self.near_result_expected_current = self.process_result_dates(expected_df_current, today, future_date)
        self.near_result_expected_next = self.process_result_dates(expected_df_next, today, future_date)

    def _process_hv(self):

        exp_df1_atm_memory = memory_atm_iv.expiry(1).copy()
        exp_df1 = exp_df1_atm_memory[exp_df1_atm_memory["atm_iv"] != 0]
        exp_df1 = pd.merge(exp_df1, self.expiry_1, on='symbol', how='left')
        exp_df1 = pd.merge(exp_df1, self.expiry_1_fwd, on='symbol', how='left')
        exp_df1['fwd_iv'] = exp_df1['forward_vol']
        exp_df1["ivp"] = exp_df1["ivp"]*100
        exp_df1["diff"] = (exp_df1["atm_iv"] - exp_df1["hv"] - exp_df1['avg_risk_prem'])
        exp_df1_short = exp_df1.sort_values(by="diff", ascending=False)
        exp_df1_long = exp_df1.sort_values(by="diff", ascending=True)

        exp_df2_atm_memory = memory_atm_iv.expiry(2).copy()
        exp_df2 = exp_df2_atm_memory[exp_df2_atm_memory["atm_iv"] != 0]
        exp_df2 = pd.merge(exp_df2, self.expiry_1, on='symbol', how='left')
        exp_df2 = pd.merge(exp_df2, self.expiry_2_fwd, on='symbol', how='left')
        exp_df2['fwd_iv'] = exp_df2['forward_vol']
        exp_df2["ivp"] = exp_df2["ivp"]*100
        exp_df2["diff"] = (exp_df2["atm_iv"] - exp_df2["hv"] - exp_df2['avg_risk_prem'])
        exp_df2_short = exp_df2.sort_values(by="diff", ascending=False)
        exp_df2_long = exp_df2.sort_values(by="diff", ascending=True)

        current_long_condition = (exp_df1["symbol"].isin(self.near_result_confirmed_current) | exp_df1["symbol"].isin(self.near_result_expected_current))
        next_long_condition = (exp_df2["symbol"].isin(self.near_result_confirmed_next) | exp_df2["symbol"].isin(self.near_result_expected_next))
        # current_short_condition = (exp_df1["symbol"].isin(self.result_confirmed_current) | exp_df1["symbol"].isin(self.result_confirmed_next)
        #                            & ~current_long_condition)
        # current_short_condition = (~current_long_condition & 
        #                            (exp_df1["symbol"].isin(self.result_confirmed_current) | exp_df1["symbol"].isin(self.result_confirmed_next) |
        #                             exp_df1["symbol"].isin(self.result_expected_current) | exp_df1["symbol"].isin(self.result_expected_next)) &
        #                             ~(exp_df1["symbol"].isin(self.near_result_confirmed_next) | exp_df1["symbol"].isin(self.near_result_confirmed_next)))
        # next_short_condition = (~next_long_condition & 
        #                            (exp_df2["symbol"].isin(self.result_confirmed_current) | exp_df2["symbol"].isin(self.result_confirmed_next) |
        #                             exp_df2["symbol"].isin(self.result_expected_current) | exp_df2["symbol"].isin(self.result_expected_next)) &
        #                             ~(exp_df2["symbol"].isin(self.near_result_confirmed_current) | exp_df2["symbol"].isin(self.near_result_confirmed_current)))
        current_short_condition = ~current_long_condition
        next_short_condition = ~next_long_condition

        exp_df1_short_result = exp_df1[current_short_condition].sort_values(by="ivp", ascending=False)
        exp_df1_long_result = exp_df1[current_long_condition].sort_values(by="ivp", ascending=True)
        exp_df2_short_result = exp_df2[next_short_condition].sort_values(by="ivp", ascending=False)
        exp_df2_long_result = exp_df2[next_long_condition].sort_values(by="ivp", ascending=True)

        return (
            exp_df1_short,
            exp_df1_long,
            exp_df2_short,
            exp_df2_long,
            exp_df1_short_result,
            exp_df1_long_result,
            exp_df2_short_result,
            exp_df2_long_result,
        )

    def update(self):
        (processed_short_1, processed_long_1, processed_short_2, processed_long_2,
         processed_short_1_result, processed_long_1_result, processed_short_2_result, processed_long_2_result) = (
            self._process_hv()
        )
        if processed_short_1 is not None:
            self.expiry_1_short = processed_short_1
        if processed_long_1 is not None:
            self.expiry_1_long = processed_long_1
        if processed_short_2 is not None:
            self.expiry_2_short = processed_short_2
        if processed_long_2 is not None:
            self.expiry_2_long = processed_long_2
        if processed_short_1_result is not None:
            self.expiry_1_short_result = processed_short_1_result
        if processed_long_1_result is not None:
            self.expiry_1_long_result = processed_long_1_result
        if processed_short_2_result is not None:
            self.expiry_2_short_result = processed_short_2_result
        if processed_long_2_result is not None:
            self.expiry_2_long_result = processed_long_2_result

    def expiry(self, num: Literal[-1, 1, 2, 3]):
        if num == -1:
            return (self.expiry_1_short,
                    self.expiry_1_long,
                    self.expiry_2_short,
                    self.expiry_2_long,
                    self.expiry_1_short_result,
                    self.expiry_1_long_result,
                    self.expiry_2_short_result,
                    self.expiry_2_long_result,)
        elif num == 1:
            return (self.expiry_1_short,
                    self.expiry_1_long,)
        elif num == 2:
            return (self.expiry_1_short,
                    self.expiry_1_long)
        else:
            return None


memory_ls_iv = LS_IV()
