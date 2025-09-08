from typing import Literal
import pandas as pd
from contants.dates import EXPIRY
from utils.api import get_all_token_set
from utils.logger import logger
from .atmiv import memory_atm_iv
import warnings
from utils.common import network_asset2df
warnings.filterwarnings("ignore")

class ATR_IV:
    def __init__(self):
        self.expiry_1_short: pd.DataFrame = pd.DataFrame()
        self.expiry_1_long: pd.DataFrame = pd.DataFrame()
        self.expiry_2_short: pd.DataFrame = pd.DataFrame()
        self.expiry_2_long: pd.DataFrame = pd.DataFrame()

    def initialize(self):
        self.expiry_1 = network_asset2df("latest_atr_per_symbol.csv")
        self.expiry_2 = network_asset2df("latest_atr_per_symbol.csv")
        self.iv_stats = network_asset2df('iv_stats.csv')
        self.expiry_1['long_hv'] = self.expiry_1['ATR_pct'] * 0.6 * 15
        self.expiry_1['short_hv'] = self.expiry_1['ATR_pct'] * 1 * 20 * 0.6  

        self.expiry_2['long_hv'] = self.expiry_2['ATR_pct'] * 0.6 * 15
        self.expiry_2['short_hv'] = self.expiry_2['ATR_pct'] * 1 * 20 * 0.6

        self.expiry_1['highest_iv'] = self.iv_stats['higest_normal_iv']
        self.expiry_1['lowest_iv'] = self.iv_stats['lowest_normal_iv']

        self.expiry_2['highest_iv'] = self.iv_stats['higest_normal_iv']
        self.expiry_2['lowest_iv'] = self.iv_stats['lowest_normal_iv']




        #move_iv

    def _process_hv(self):

        # test_df = get_all_token_set(delta={"pe":["-0.5", "-0.3"],"ce":["0.3","0.5"]}, expiry=["2024-10-31"], symbols=["HDFCBANK", "AXISBANK"])
        # print(test_df)

        exp_df1_atm_memory = memory_atm_iv.expiry(1).copy()


        exp_df1 = exp_df1_atm_memory[exp_df1_atm_memory["atm_iv"] != 0]
        
        exp_df1 = pd.merge(exp_df1, self.expiry_1, on='symbol', how='inner')
        # exp_df1['highest_iv'] = exp_df1['iv_stats'].str['highest_iv']
        # exp_df1['lowest_iv'] = exp_df1['iv_stats'].str['highest_iv']
        exp_df1['fair_iv'] = exp_df1['iv_stats'].str['avg_normal_iv']
        
        exp_df1 = exp_df1[exp_df1['lowest_iv'] > 0]
        
        # exp_df1["pct_change"] = exp_df1["pct_change"]
        # exp_df1['hv'] = exp_df1['move_iv']
        
        exp_df1["short_diff"] = (2 * exp_df1['atm_iv']) - exp_df1['fair_iv'] - exp_df1['short_hv']
        exp_df1["long_diff"] =  exp_df1['lowest_iv'] + exp_df1['long_hv'] - 2*exp_df1['atm_iv']
        
        exp_df1['ivp'] = exp_df1['ivp'] * 100
        # exp_df1_short = exp_df1
        exp_df1_short = exp_df1.sort_values(by="short_diff", ascending=False)
        # exp_df1_long = exp_df1
        exp_df1_long = exp_df1.sort_values(by="long_diff", ascending=False)


       

    
        exp_df2_atm_memory = memory_atm_iv.expiry(2).copy()

        exp_df2 = exp_df2_atm_memory[exp_df2_atm_memory["atm_iv"] != 0]
        exp_df2 = pd.merge(exp_df2, self.expiry_2, on='symbol', how='inner')
        # exp_df2['highest_iv'] = exp_df2['iv_stats'].str['higest_normal_iv']
        # exp_df2['lowest_iv'] = exp_df2['iv_stats'].str['lowest_normal_iv']
        exp_df2['fair_iv'] = exp_df2['iv_stats'].str['avg_normal_iv']
        exp_df2 = exp_df2[exp_df2['lowest_iv'] > 0]
        
        # exp_df1["pct_change"] = exp_df1["pct_change"]
        # exp_df1['hv'] = exp_df1['move_iv']
        
        exp_df2["short_diff"] = (2 * exp_df2['atm_iv']) - exp_df2['fair_iv'] - exp_df2['short_hv']
        exp_df2["long_diff"] =  exp_df2['lowest_iv'] + exp_df2['long_hv'] - 2*exp_df2['atm_iv']


        # exp_df2["pct_change"] = exp_df2["pct_change"]
        # exp_df2['hv'] = exp_df2['move_iv']
        exp_df2['ivp'] = exp_df2['ivp'] * 100
        


        
      
        exp_df2_short = exp_df2.sort_values(by="short_diff", ascending=False)
       
        exp_df2_long = exp_df2.sort_values(by="long_diff", ascending=False)

        
        # exp_df1 = exp_df1.drop(['forward_vol','move_iv'], axis=1)
        # exp_df2 = exp_df2.drop(['forward_vol','move_iv'], axis=1)
        # print(exp_df1_short.columns)
        return (
            exp_df1_short,
            exp_df1_long,
            exp_df2_short,

            exp_df2_long,
        )

    def update(self):
        processed_short_1, processed_long_1, processed_short_2, processed_long_2 = (
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

    def expiry(self, num: Literal[-1, 1, 2, 3]):
        if num == -1:
            return (self.expiry_1_short,
                    self.expiry_1_long,
                    self.expiry_2_short,
                    self.expiry_2_long,)
        elif num == 1:
            return (self.expiry_1_short,
                    self.expiry_1_long,)
        elif num == 2:
            return (self.expiry_1_short,
                    self.expiry_1_long)
        else:
            return None


memory_atr = ATR_IV()
