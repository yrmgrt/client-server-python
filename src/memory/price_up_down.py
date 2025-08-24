from typing import Literal
import pandas as pd
from contants.dates import EXPIRY
from utils.api import get_atm_iv_from_api
from utils.logger import logger
from .atmiv import memory_atm_iv
from utils.api import get_all_token_set

class PRICE_CHNG:
    def __init__(self):
        self.expiry_1_abv_price: pd.DataFrame = pd.DataFrame()
        self.expiry_1_blw_price: pd.DataFrame = pd.DataFrame()
        self.expiry_2_abv_price: pd.DataFrame = pd.DataFrame()
        self.expiry_2_blw_price: pd.DataFrame = pd.DataFrame()
        self.move_tracker: pd.DataFrame = pd.DataFrame()

    def _process_price_change(self):

        exp_df1 = memory_atm_iv.expiry(1).copy()
        # exp_df1["pct_change"] = (-1)*exp_df1["pct_change"]
        exp_df1_abv_price = exp_df1[exp_df1["pct_change"] >= 0]
        exp_df1_abv_price.sort_values(by="pct_change", ascending=False, inplace=True)
        
        exp_df1_blw_price = exp_df1[(exp_df1["pct_change"] < 0) & (exp_df1["pct_change"] > -800)]
        exp_df1_blw_price.sort_values(by="pct_change", ascending=True, inplace=True)
        
        exp_df2 = memory_atm_iv.expiry(2).copy()
        # exp_df2["pct_change"] = (-1)*exp_df2["pct_change"]
        exp_df2_abv_price = exp_df2[exp_df2["pct_change"] >= 0]
        exp_df2_abv_price.sort_values(by="pct_change", ascending=False, inplace=True)
        
        exp_df2_blw_price = exp_df2[(exp_df2["pct_change"] < 0) & (exp_df2["pct_change"] > -800)]
        exp_df2_blw_price.sort_values(by="pct_change", ascending=True, inplace=True)

        return (
            exp_df1_abv_price,
            exp_df1_blw_price,
            exp_df2_abv_price,
            exp_df2_blw_price,
        )

    def update(self):
        # self.calendar_df = self._process_calendar()

        processed_price_abv_1, processed_price_blw_1, processed_price_abv_2, processed_price_blw_2 = (
            self._process_price_change()
        )
        self.move_tracker = memory_atm_iv.move_tracker_func().copy()

        if processed_price_abv_1 is not None:
            self.expiry_1_abv_price = processed_price_abv_1
        if processed_price_blw_1 is not None:
            self.expiry_1_blw_price = processed_price_blw_1
        if processed_price_abv_2 is not None:
            self.expiry_2_abv_price = processed_price_abv_2
        if processed_price_blw_2 is not None:
            self.expiry_2_blw_price = processed_price_blw_2

    def expiry(self, num: Literal[-1, 1, 2, 3]):
        if num == -1:
            return (self.expiry_1_abv_price,
                    self.expiry_1_blw_price,
                    self.expiry_2_abv_price,
                    self.expiry_2_blw_price,
                    self.move_tracker,)
        elif num == 1:
            return (self.expiry_1_abv_price,
                    self.expiry_1_blw_price,)
        elif num == 2:
            return (self.expiry_2_abv_price,
                    self.expiry_2_blw_price,)
        else:
            return None
    
    def calendar(self):
        return self.calendar_df


memory_price_change = PRICE_CHNG()
