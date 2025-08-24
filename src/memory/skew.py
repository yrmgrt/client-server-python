from typing import Any, Literal, Optional
import pandas as pd
from contants.dates import EXPIRY
from utils.api import get_skew_from_api
from memory.atmiv import memory_atm_iv


class Skew:
    def __init__(self):
        self.df: pd.DataFrame = pd.DataFrame()
        self.dump: pd.DataFrame = pd.DataFrame()
        self.sorted_data: Any = []
        self.flattened_dict = {}

    def update(self):
        data = get_skew_from_api().copy()

        if data is not None:
            # Normalize data into DataFrame
            df = pd.json_normalize(data, "individual", ["symbol"])

            # Filter out rows with empty expiry
            df = df.dropna(subset=["expiry"])

            self.dump = df

            # Initialize expiry_map structure with empty lists
            expiry_map = {
                expiry: {
                    "expiry": expiry,
                    "PPF": [],
                    "PPB": [],
                    "PCF": [],
                    "PCB": [],
                    "CCF": [],
                    "CCB": [],
                    "FourLeg_F": [],
                    "FourLeg_B": [],
                }
                for expiry in df["expiry"].unique()
            }

            exp_df1_pct_change = memory_atm_iv.expiry(1).copy()
            # exp_df1_pct_change["pct_change"] = (-1)*exp_df1_pct_change["pct_change"]
            self.pct_change_df = exp_df1_pct_change[["symbol", "pct_change"]]
            
            def add_pct_change(row, symbol, ubl_data):
                """ Add pct_change to the symbol's data """
            
                pct_change = self.pct_change_df.loc[
                    self.pct_change_df["symbol"] == symbol, "pct_change"
                ]
                if not pct_change.empty:
                    ubl_data["pct_change"] = pct_change.iloc[0]
                else:
                    ubl_data["pct_change"] = 0

            def process_pe_pe(row):
                z_score = row.get("pe_pe.z_score", 0)
                key = "PPF" if z_score > 0 else "PPB"
                symbol = row["symbol"]
                atm_data = {
                    "strike_price": row.get("atm_data.strike_price", 0),
                    "iv": row.get("atm_data.iv", 0),
                }

                skew_val = row.get("pe_pe.skew", 0) if key == "PPB" else row.get("pe_pe.skew", 0)*(-1)
                skew_avg_val = row.get("pe_pe.skew_avg", 0) if key == "PPB" else row.get("pe_pe.skew_avg", 0)*(-1)
                skew_avg_yest_val = row.get("pe_pe.skew_avg_yest", 0) if key == "PPB" else row.get("pe_pe.skew_avg_yest", 0)*(-1)

                # Create Ubl structure
                ubl_data = {
                    **atm_data,
                    "z_score": z_score,
                    "ivp": row.get("pe_pe.ivp", 0),
                    "skew": skew_val,
                    "skew_avg": skew_avg_val,
                    "skew_std": row.get("pe_pe.skew_std", 0),
                    "skew_avg_yest": skew_avg_yest_val,
                    "skew_diff": skew_val - skew_avg_yest_val,
                    "elements": [create_element(e) for e in row.get("pe_pe.elements", [])],
                }
                add_pct_change(row, symbol, ubl_data)

                expiry_map[row["expiry"]][key].append({row["symbol"]: ubl_data})

            def process_pe_ce(row):
                z_score = row.get("pe_ce.z_score", 0)
                key = "PCF" if z_score > 0 else "PCB"
                symbol = row["symbol"]
                atm_data = {
                    "strike_price": row.get("atm_data.strike_price", 0),
                    "iv": row.get("atm_data.iv", 0),
                }

                skew_val = row.get("pe_ce.skew", 0) if key == "PCF" else row.get("pe_ce.skew", 0)*(-1)
                skew_avg_val = row.get("pe_ce.skew_avg", 0) if key == "PCF" else row.get("pe_ce.skew_avg", 0)*(-1)
                skew_avg_yest_val = row.get("pe_ce.skew_avg_yest", 0) if key == "PCF" else row.get("pe_ce.skew_avg_yest", 0)*(-1)

                ubl_data = {
                    **atm_data,
                    "z_score": z_score,
                    "ivp": row.get("pe_ce.ivp", 0),
                    "skew": skew_val,
                    "skew_avg": skew_avg_val,
                    "skew_std": row.get("pe_ce.skew_std", 0),
                    "skew_avg_yest": skew_avg_yest_val,
                    "skew_diff": skew_val - skew_avg_yest_val,
                    "elements": [create_element(e) for e in row.get("pe_ce.elements", [])],
                }
                add_pct_change(row, symbol, ubl_data)

                expiry_map[row["expiry"]][key].append({row["symbol"]: ubl_data})

            def process_ce_ce(row):
                z_score = row.get("ce_ce.z_score", 0)
                key = "CCF" if z_score > 0 else "CCB"
                symbol = row["symbol"]
                atm_data = {
                    "strike_price": row.get("atm_data.strike_price", 0),
                    "iv": row.get("atm_data.iv", 0),
                }

                skew_val = row.get("ce_ce.skew", 0) if key == "CCF" else row.get("ce_ce.skew", 0)*(-1)
                skew_avg_val = row.get("ce_ce.skew_avg", 0) if key == "CCF" else row.get("ce_ce.skew_avg", 0)*(-1)
                skew_avg_yest_val = row.get("ce_ce.skew_avg_yest", 0) if key == "CCF" else row.get("ce_ce.skew_avg_yest", 0)*(-1)

                ubl_data = {
                    **atm_data,
                    "z_score": z_score,
                    "ivp": row.get("ce_ce.ivp", 0),
                    "skew": skew_val,
                    "skew_avg": skew_avg_val,
                    "skew_std": row.get("ce_ce.skew_std", 0),
                    "skew_avg_yest": skew_avg_yest_val,
                    "skew_diff": skew_val - skew_avg_yest_val,
                    "elements": [create_element(e) for e in row.get("ce_ce.elements", [])],
                }
                add_pct_change(row, symbol, ubl_data)

                expiry_map[row["expiry"]][key].append({row["symbol"]: ubl_data})

            def process_four_leg(row):
                symbol = row["symbol"]
                atm_data = {
                    "strike_price": row.get("atm_data.strike_price", 0),
                    "iv": row.get("atm_data.iv", 0),
                }

                # Create FourLeg structure
                four_leg_data = {
                    **atm_data,
                    "skew": row.get("four_leg.skew", 0)*(-1),
                    "skew_avg": row.get("four_leg.skew_avg", 0),
                    "skew_std": row.get("four_leg.skew_std", 0),
                    "skew_avg_yest": row.get("four_leg.skew_avg_yest", 0),
                    "skew_diff": row.get("four_leg.skew", 0)*(-1) - row.get("four_leg.skew_avg_yest", 0),
                    "z_score": row.get("four_leg.z_score", 0),
                    "ivp": row.get("four_leg.ivp", 0),
                    "elements": [create_element(e) for e in row.get("four_leg.elements", [])],
                }
                add_pct_change(row, symbol, four_leg_data)

                expiry_map[row["expiry"]]["FourLeg_F"].append({row["symbol"]: four_leg_data})
                expiry_map[row["expiry"]]["FourLeg_B"].append({row["symbol"]: four_leg_data})

            def create_element(element_row):
                return {
                    "delta": element_row.get("delta", 0),
                    "iv": element_row.get("iv", 0),
                    "strike_price": element_row.get("strike_price", ""),
                    "type_": element_row.get("type_", ""),
                    "atm_iv_diff": element_row.get("atm_iv_diff", 0),
                    "atm_strike_diff": element_row.get("atm_strike_diff", ""),
                    "liquidity": element_row.get("liquidity", 0),
                }

                # Apply processing functions

            df.apply(process_pe_pe, axis=1)
            df.apply(process_pe_ce, axis=1)
            df.apply(process_ce_ce, axis=1)
            df.apply(process_four_leg, axis=1)

            self.df = df
            
            df_dict = {}

            # Sort each list by z_score
            for expiry_data in expiry_map.values():
                expiry_date = expiry_data["expiry"]
                if expiry_date in [EXPIRY[0], EXPIRY[1], EXPIRY[2]]:
                    flattened_data = []
                    for key in expiry_data:
                        if key != "expiry":
                            reverse = key in ["PPF", "PCF", "CCF", "FourLeg_B"]
                            filtered_data = [
                                item for item in expiry_data[key]
                                if list(item.values())[0].get("z_score", 0) != -999
                                ]
                            expiry_data[key] = sorted(
                                filtered_data,
                                key=lambda x: list(x.values())[0].get("z_score", 0),
                                reverse=reverse,
                            )
                            # expiry_data[key] = sorted(
                            #     filtered_data,
                            #     key=lambda x: list(x.values())[0].get("skew_diff", 0),
                            #     reverse=True,
                            # )
                            for sym_data in expiry_data[key]:
                                for sym, sym_details in sym_data.items():
                                    flattened_data.append({
                                        'expiry': expiry_date,
                                        'type': key,
                                        'symbol': sym,
                                        'sym_details': sym_details,
                                    })
                    df_dict[expiry_date] = pd.DataFrame(flattened_data)
                        
            self.flattened_dict = df_dict

            final_data = list(expiry_map.values())

            self.sorted_data = final_data

    def get_dump(self):
        return self.dump

    def get_data(self):
        return self.sorted_data
    
    def get_flattened_dict(self, num: Literal[-1, 1, 2, 3]):
        if num == -1:
            return self.flattened_dict
        elif num == 1:
            df = self.flattened_dict[EXPIRY[0]].copy()
            df_expanded = pd.concat([df[['expiry', 'type', 'symbol']], pd.json_normalize(df['sym_details'])], axis=1)
            return df_expanded
        elif num == 2:
            df = self.flattened_dict[EXPIRY[1]].copy()
            df_expanded = pd.concat([df[['expiry', 'type', 'symbol']], pd.json_normalize(df['sym_details'])], axis=1)
            return df_expanded
        else:
            df = self.flattened_dict[EXPIRY[2]].copy()
            df_expanded = pd.concat([df[['expiry', 'type', 'symbol']], pd.json_normalize(df['sym_details'])], axis=1)
            return df_expanded


memory_skew = Skew()
