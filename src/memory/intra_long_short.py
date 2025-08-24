from typing import Literal
from datetime import datetime
import os
import pandas as pd
from contants.dates import EXPIRY
from utils.api import get_all_token_set
from utils.logger import logger
from .atmiv import memory_atm_iv
import warnings
from utils.common import asset2df, ASSET_DIR
warnings.filterwarnings("ignore")

date_today = datetime.now().date()

class INTRA_L_S:
    def __init__(self):
        self.expiry_1_long: pd.DataFrame = pd.DataFrame()
        self.expiry_1_short: pd.DataFrame = pd.DataFrame()
    
    def select_latest_idv(self):
        idv_files_path = os.path.join(ASSET_DIR, "idv_cal")
        dates_content = [datetime.strptime(file[:-4], '%Y-%m-%d').date() for file in os.listdir(idv_files_path) if file.endswith(".csv")]
        dates = [date for date in dates_content if date < date_today]
        dates.sort()
        date_required = dates[-1]
        filename = date_required.strftime('%Y-%m-%d') + '.csv'
        yest_file_path = os.path.join("idv_cal", filename)
        yest_df = asset2df(yest_file_path)
        yest_df["long_moves_yest"] = yest_df["long_moves"]
        return (yest_df[["symbol", "long_moves_yest"]], yest_file_path)

    def initialize(self):
        self.expiry_1_fwd = asset2df("forward_vol_expiry_1.csv")
        self.expiry_2_fwd = asset2df("forward_vol_expiry_2.csv")
        self.iv_stats_df = asset2df("iv_stats.csv")
        self.yest_idv_df, temp = self.select_latest_idv()

    def _process_intra_long_short(self):

        atm_memory_long_short = memory_atm_iv.expiry(1).copy()

        atm_memory_long_short = atm_memory_long_short[atm_memory_long_short["atm_iv"] != 0]
        exp_df1_long_short = pd.merge(atm_memory_long_short, self.expiry_1_fwd, on='symbol', how='inner')
        exp_df1_long_short = pd.merge(exp_df1_long_short, self.iv_stats_df[["symbol", "bench_mark_iv"]], on='symbol', how='inner')
        exp_df1_long_short = pd.merge(exp_df1_long_short, self.yest_idv_df, on='symbol', how='inner')

        exp_df1_long_short["ivp"] = exp_df1_long_short["ivp"]*100
        exp_df1_long_short['fwd_iv'] = exp_df1_long_short['forward_vol']

        # temp, printer = self.select_latest_idv()
        # print(printer)
        # print(exp_df1_long_short[["symbol", "ivp", "atm_iv", "fwd_iv", "days_theta", "hv", "long_moves_yest", "bench_mark_iv"]])
        
        short_condition_0 = ((exp_df1_long_short["days_theta"]<=0.5)
                             &(exp_df1_long_short["hv"]<=0.9*exp_df1_long_short["atm_iv"])
                             &(exp_df1_long_short["long_moves_yest"]<=2))
        
        short_condition_1 = ((exp_df1_long_short["ivp"]>=30)&(exp_df1_long_short["ivp"]<=60)
                             &(exp_df1_long_short["atm_iv"]>exp_df1_long_short["fwd_iv"]))
        
        short_condition_2_1 = ((exp_df1_long_short["ivp"]>=60)&(exp_df1_long_short["ivp"]<=90))
        if str(datetime.now().time()) < "10:30:00":
            short_condition_2_2 = (exp_df1_long_short["atm_iv"]>exp_df1_long_short["fwd_iv"])
        else:
            short_condition_2_2 = (exp_df1_long_short["atm_iv"]>(exp_df1_long_short["fwd_iv"]+exp_df1_long_short["bench_mark_iv"])/2)
        
        short_condition_3 = (exp_df1_long_short["ivp"]>90
                             &(exp_df1_long_short["atm_iv"]>exp_df1_long_short["bench_mark_iv"]))
        
        if str(datetime.now().time()) < "10:30:00":
            long_condition_0_1 = (exp_df1_long_short["days_theta"]>=0)
        else:
            long_condition_0_1 = (exp_df1_long_short["days_theta"]>=0.5)
        
        long_condition_0_2 = ((exp_df1_long_short["hv"]>=0.9*exp_df1_long_short["atm_iv"])
                             &(exp_df1_long_short["long_moves_yest"]>=3))
        
        long_condition_1 = ((exp_df1_long_short["ivp"]>=30)&(exp_df1_long_short["ivp"]<=60)
                             &(exp_df1_long_short["atm_iv"]<exp_df1_long_short["fwd_iv"]))
        
        long_condition_2_1 = ((exp_df1_long_short["ivp"]>=60)&(exp_df1_long_short["ivp"]<=90))
        if str(datetime.now().time()) < "10:30:00":
            long_condition_2_2 = (exp_df1_long_short["atm_iv"]<exp_df1_long_short["fwd_iv"])
        else:
            long_condition_2_2 = (exp_df1_long_short["atm_iv"]<(exp_df1_long_short["fwd_iv"]+exp_df1_long_short["bench_mark_iv"])/2)
        
        long_condition_3 = (exp_df1_long_short["ivp"]>90
                             &(exp_df1_long_short["atm_iv"]<exp_df1_long_short["bench_mark_iv"]))

        short_df = exp_df1_long_short[short_condition_0 & (short_condition_1 | (short_condition_2_1 & short_condition_2_2) | short_condition_3)]
        long_df = exp_df1_long_short[(long_condition_0_1 & long_condition_0_2) & (long_condition_1 | (long_condition_2_1 & long_condition_2_2) | long_condition_3)]

        short_df = short_df[["symbol", "pct_change", "ivp", "atm_iv", "fwd_iv", "days_theta", "hv", "long_moves_yest", "bench_mark_iv"]]
        long_df = long_df[["symbol", "pct_change", "ivp", "atm_iv", "fwd_iv", "days_theta", "hv", "long_moves_yest", "bench_mark_iv"]]
        # short_df.to_csv("short_df.csv")
        # long_df.to_csv("long_df.csv")

        return (
            short_df, 
            long_df,
        )

    def update(self):
        processed_short_df, processed_long_df = (
            self._process_intra_long_short()
        )
        if processed_short_df is not None:
            self.expiry_1_short = processed_short_df
        
        if processed_long_df is not None:
            self.expiry_1_long = processed_long_df

    def expiry(self, num: Literal[-1, 1, 2, 3]):
        if num == -1:
            return (None)
        elif num == 1:
            return (self.expiry_1_short,
                    self.expiry_1_long,)
        elif num == 2:
            return (None)
        else:
            return None

memory_intra_long_short = INTRA_L_S()
