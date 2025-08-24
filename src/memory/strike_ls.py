# import ast
# import json
from typing import Literal
import pandas as pd
# from contants.dates import EXPIRY
# from utils.common import asset2df
# from .atmiv import memory_atm_iv
# from .calendars import memory_calendars
# from .skew import memory_skew
# import time
# from scipy.interpolate import interp1d


class STRIKE_LS:
    def __init__(self):
        self.display_df = pd.DataFrame()

    def _process_strike_ls(self):
        master_df_1 = pd.DataFrame()
        compare_df_2 = pd.DataFrame()
        vol_morn = pd.DataFrame()
        skew_morn = pd.DataFrame()
        skew_morn_fair = pd.DataFrame()
        master_df_1_long_sort = pd.DataFrame()
        master_df_1_short_sort = pd.DataFrame()
        master_df_1_long_sort_intra = pd.DataFrame()
        master_df_1_short_sort_intra = pd.DataFrame()
        
        return (
            master_df_1, compare_df_2, vol_morn, skew_morn, skew_morn_fair, master_df_1_long_sort,
            master_df_1_short_sort, master_df_1_long_sort_intra, master_df_1_short_sort_intra,
        )

    def update(self):
        (self.compare_df_1, self.compare_df_2, self.vol_morn, self.skew_morn, self.skew_morn_fair, self.long_df_1, self.short_df_1,
         self.intra_long_df_1, self.intra_short_df_1) = (
            self._process_strike_ls()
        )
        

    def expiry(self, num: Literal[-1, 1, 2, 3]):
        if num == -1:
            return (self.compare_df_1,
                    self.compare_df_2,
                    self.display_df,
                    self.vol_morn,
                    self.skew_morn,
                    self.skew_morn_fair,
                    self.long_df_1,
                    self.short_df_1,
                    self.intra_long_df_1,
                    self.intra_short_df_1,)
        elif num == 1:
            return (self.compare_df_1,
                    self.compare_df_2,
                    self.display_df,
                    self.vol_morn,
                    self.skew_morn,
                    self.skew_morn_fair,
                    self.long_df_1,
                    self.short_df_1,
                    self.intra_long_df_1,
                    self.intra_short_df_1,)
        else:
            return None


memory_strike_ls = STRIKE_LS()