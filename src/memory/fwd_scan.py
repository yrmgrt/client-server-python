from typing import Literal
import pandas as pd
from contants.dates import EXPIRY
from utils.api import get_all_token_set
from utils.logger import logger
from .atmiv import memory_atm_iv
import warnings
from utils.common import network_asset2df
warnings.filterwarnings("ignore")

class FWD_IV:
    def __init__(self):
        self.expiry_1_abv_fwd: pd.DataFrame = pd.DataFrame()
        self.expiry_1_blw_fwd: pd.DataFrame = pd.DataFrame()
        self.expiry_2_abv_fwd: pd.DataFrame = pd.DataFrame()
        self.expiry_2_blw_fwd: pd.DataFrame = pd.DataFrame()

    def initialize(self):
        self.expiry_1 = network_asset2df("forward_vol_expiry_1.csv")
        self.expiry_2 = network_asset2df("forward_vol_expiry_2.csv")


    def _process_fwd_iv(self):

        # test_df = get_all_token_set(delta={"pe":["-0.5", "-0.3"],"ce":["0.3","0.5"]}, expiry=["2024-10-31"], symbols=["HDFCBANK", "AXISBANK"])
        # print(test_df)

        exp_df1_atm_memory = memory_atm_iv.expiry(1).copy()

        exp_df1 = exp_df1_atm_memory[exp_df1_atm_memory["atm_iv"] != 0]
        exp_df1 = pd.merge(exp_df1, self.expiry_1, on='symbol', how='inner')
        exp_df1["ivp"] = exp_df1["ivp"]*100
        exp_df1['fwd_iv'] = exp_df1['forward_vol']
        exp_df1["diff"] = (exp_df1["atm_iv"] - exp_df1["fwd_iv"])/exp_df1["atm_iv"]
        exp_df1_abv_fwd = exp_df1[exp_df1['atm_iv'] >= exp_df1['fwd_iv']]
        exp_df1_abv_fwd.sort_values(by="diff", ascending=False, inplace=True)
        exp_df1_blw_fwd = exp_df1[exp_df1['atm_iv'] < exp_df1['fwd_iv']]
        exp_df1_blw_fwd.sort_values(by="diff", ascending=True, inplace=True)

        # exp_df1_abv_fwd.to_csv("temp1.csv")
        # exp_df1_blw_fwd.to_csv("temp2.csv")

        exp_df2_atm_memory = memory_atm_iv.expiry(2).copy()

        exp_df2 = exp_df2_atm_memory[exp_df2_atm_memory["atm_iv"] != 0]
        exp_df2 = pd.merge(exp_df2, self.expiry_2, on='symbol', how='inner')
        exp_df2["ivp"] = exp_df2["ivp"]*100
        exp_df2['fwd_iv'] = exp_df2['forward_vol']
        exp_df2["diff"] = (exp_df2["atm_iv"] - exp_df2["fwd_iv"])/exp_df2["atm_iv"]
        exp_df2_abv_fwd = exp_df2[exp_df2['atm_iv'] >= exp_df2['fwd_iv']]
        exp_df2_abv_fwd.sort_values(by="diff", ascending=False, inplace=True)
        exp_df2_blw_fwd = exp_df2[exp_df2['atm_iv'] < exp_df2['fwd_iv']]
        exp_df2_blw_fwd.sort_values(by="diff", ascending=True, inplace=True)

        # exp_df2_abv_fwd.to_csv("temp1_feb.csv")
        # exp_df2_blw_fwd.to_csv("temp2_feb.csv")

        exp_df1 = exp_df1.drop('forward_vol', axis=1)
        exp_df2 = exp_df2.drop('forward_vol', axis=1)

        return (
            exp_df1_abv_fwd,
            exp_df1_blw_fwd,
            exp_df2_abv_fwd,
            exp_df2_blw_fwd,
        )

    def update(self):
        processed_fwd_abv_1, processed_fwd_blw_1, processed_fwd_abv_2, processed_fwd_blw_2 = (
            self._process_fwd_iv()
        )
        if processed_fwd_abv_1 is not None:
            self.expiry_1_abv_fwd = processed_fwd_abv_1
        if processed_fwd_blw_1 is not None:
            self.expiry_1_blw_fwd = processed_fwd_blw_1
        if processed_fwd_abv_2 is not None:
            self.expiry_2_abv_fwd = processed_fwd_abv_2
        if processed_fwd_blw_2 is not None:
            self.expiry_2_blw_fwd = processed_fwd_blw_2

    def expiry(self, num: Literal[-1, 1, 2, 3]):
        if num == -1:
            return (self.expiry_1_abv_fwd,
                    self.expiry_1_blw_fwd,
                    self.expiry_2_abv_fwd,
                    self.expiry_2_blw_fwd,)
        elif num == 1:
            return (self.expiry_1_abv_fwd,
                    self.expiry_1_blw_fwd,)
        elif num == 2:
            return (self.expiry_2_abv_fwd,
                    self.expiry_2_blw_fwd,)
        else:
            return None


memory_fwd_scan_iv = FWD_IV()
