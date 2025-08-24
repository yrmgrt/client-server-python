import json
from typing import Optional
import pandas as pd
from pydantic import BaseModel
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from contants.dates import EXPIRY
from memory.metadata import metadata_map
from memory.skew import memory_skew
from memory.atmiv import memory_atm_iv
from memory.vol import memory_vol
from memory.correlation import memory_correlation
from memory.skew_benchmark import memory_skew_benchmark
from memory.fwd_scan import memory_fwd_scan_iv
from memory.price_up_down import memory_price_change
from memory.strike_ls import memory_strike_ls
from memory.calendars import memory_calendars
from memory.bcrs_scan import memory_bcrs
from memory.intra_long_short import memory_intra_long_short
from memory.long_short import memory_ls_iv
from memory.iv_surface import memory_surface_scan_iv
from memory.atr_scan import memory_atr
import concurrent.futures
import asyncio
from datetime import datetime

router = APIRouter(prefix="/screener", tags=["screener"])


@router.get(path="/metadata", name="screeners:metadata")
async def get_metadata():
    metadata_values = metadata_map.get_metadata_values()
    return JSONResponse(
        {"success": True, "msg": "Metadata", "data": metadata_values},
        status.HTTP_200_OK,
    )


@router.get(path="/atmiv", name="screeners:atmiv")
async def get_atm_iv():
    expiry_1, expiry_2, expiry_3 = memory_atm_iv.expiry(-1)
    combined_df = pd.concat([expiry_1, expiry_2, expiry_3])
    grouped_df = combined_df.groupby("symbol").agg(list).reset_index()
    data = json.loads(grouped_df.to_json(orient="records"))
    return JSONResponse(
        {"success": True, "msg": "ATM IV Data", "data": data},
        status.HTTP_200_OK,
    )


@router.get(path="/vol", name="screeners:vol")
async def get_vol():
    expiry_1_up, expiry_1_down, expiry_2_up, expiry_2_down = memory_vol.expiry_with_atm_display(-1)
    data = {
        "expiry_1_up": json.loads(expiry_1_up.to_json(orient="records")),
        "expiry_1_down": json.loads(expiry_1_down.to_json(orient="records")),
        "expiry_2_up": json.loads(expiry_2_up.to_json(orient="records")),
        "expiry_2_down": json.loads(expiry_2_down.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All Vol Data", "data": data},
        status.HTTP_200_OK,
    )


@router.get(path="/filtered-vol", name="screeners:filtered-vol")
async def get_filtered_vol():
    expiry_1, expiry_2 = memory_vol.expiry_with_atm(-1)

    # Use concurrent futures to parallelize filtering
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        tasks = [
            loop.run_in_executor(
                executor,
                lambda: json.loads(
                    expiry_1[
                        expiry_1["atm_iv"] >= expiry_1["vol_up_benchmark"]
                    ].to_json(orient="records")
                ),
            ),
            loop.run_in_executor(
                executor,
                lambda: json.loads(
                    expiry_1[
                        expiry_1["atm_iv"] <= expiry_1["vol_down_benchmark"]
                    ].to_json(orient="records")
                ),
            ),
            loop.run_in_executor(
                executor,
                lambda: json.loads(
                    expiry_2[
                        expiry_2["atm_iv"] >= expiry_2["vol_up_benchmark"]
                    ].to_json(orient="records")
                ),
            ),
            loop.run_in_executor(
                executor,
                lambda: json.loads(
                    expiry_2[
                        expiry_2["atm_iv"] <= expiry_2["vol_down_benchmark"]
                    ].to_json(orient="records")
                ),
            ),
        ]
        filter_exp1_up, filter_exp1_down, filter_exp2_up, filter_exp2_down = (
            await asyncio.gather(*tasks)
        )

    return JSONResponse(
        {
            "success": True,
            "msg": "Filtered Vol Data",
            "data": {
                "expiry1_up": filter_exp1_up,
                "expiry1_down": filter_exp1_down,
                "expiry2_up": filter_exp2_up,
                "expiry2_down": filter_exp2_down,
            },
        },
        status.HTTP_200_OK,
    )


class VolUpdateItem(BaseModel):
    expiry: str
    ticker: str
    benchmark: Optional[float]
    change: Optional[float]


@router.post(path="/vol-update", name="screeners:vol-update")
async def volUpdate(body: VolUpdateItem):
    # Parse the date string into a datetime object
    date_obj = datetime.strptime(body.expiry, "%a %b %d %Y")
    # Format the datetime object into the desired string format
    formatted_date_str = date_obj.strftime("%Y-%m-%d")
    if formatted_date_str == EXPIRY[0]:
        expiry = 1
    else:
        expiry = 2

    memory_vol.update_ticker_benchmark(
        expiry=expiry,
        ticker=body.ticker,
        vol_up_benchmark=body.benchmark,
        vol_down_benchmark=body.benchmark,
        vol_up_threshold_percentage=body.change,
        vol_down_threshold_percentage=body.change,
    )
    return JSONResponse(
        {
            "success": True,
            "msg": "Update Successful",
            "data": None,
        },
        status.HTTP_200_OK,
    )


@router.get(path="/correlation", name="screeners:correlation")
async def get_correlation():
    expiry_1, expiry_2 = memory_correlation.expiry_with_atm(-1)
    data = {
        "expiry_1": json.loads(expiry_1.to_json(orient="records")),
        "expiry_2": json.loads(expiry_2.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All Correlation Data", "data": data},
        status.HTTP_200_OK,
    )


@router.get(path="/filtered-correlation", name="screeners:filtered-correlation")
async def get_filtered_correlation():
    expiry_1, expiry_2 = memory_correlation.expiry_with_atm(-1)

    # Use concurrent futures to parallelize filtering
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        tasks = [
            loop.run_in_executor(
                executor,
                lambda: json.loads(
                    expiry_1[expiry_1["ratio"] > expiry_1["avg_ratio"]].to_json(
                        orient="records"
                    )
                ),
            ),
            loop.run_in_executor(
                executor,
                lambda: json.loads(
                    expiry_2[expiry_2["ratio"] > expiry_2["avg_ratio"]].to_json(
                        orient="records"
                    )
                ),
            ),
        ]
        filtered_data_ratio_expiry_1, filtered_data_ratio_expiry_2 = (
            await asyncio.gather(*tasks)
        )

    return JSONResponse(
        {
            "success": True,
            "msg": "Filtered Correlation Data",
            "data": {
                "expiry1": filtered_data_ratio_expiry_1,
                "expiry2": filtered_data_ratio_expiry_2,
            },
        },
        status.HTTP_200_OK,
    )


@router.get(path="/skew", name="screeners:skew")
async def get_skew_data():
    return JSONResponse(
        {"success": True, "msg": "Skew Data", "data": memory_skew.get_data()},
        status.HTTP_200_OK,
    )


@router.get(path="/skew-benchamrk", name="screeners:skew-benchamrk")
async def get_skew_benchmark_data():
    expiry_1, expiry_2 = memory_skew_benchmark.expiry_with_skew(-1)
    data = {
        "expiry_1": json.loads(expiry_1.to_json(orient="records")),
        "expiry_2": json.loads(expiry_2.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All Skew Benchmark Data", "data": data},
        status.HTTP_200_OK,
    )


class SkewUpdateItem(BaseModel):
    expiry: str
    ticker: str
    ppf: Optional[float]
    pcf: Optional[float]
    ccb: Optional[float]
    fourL_F: Optional[float]


@router.post(path="/skew-update", name="screeners:skew-update")
async def skewUpdate(body: SkewUpdateItem):
    # Parse the date string into a datetime object
    date_obj = datetime.strptime(body.expiry, "%a %b %d %Y")
    # Format the datetime object into the desired string format
    formatted_date_str = date_obj.strftime("%Y-%m-%d")
    if formatted_date_str == EXPIRY[0]:
        expiry = 1
    else:
        expiry = 2

    memory_skew_benchmark.update_ticker_benchmark(
        expiry=expiry,
        ticker=body.ticker,
        pcf=body.pcf,
        ppf=body.ppf,
        ccb=body.ccb,
        fourL_F=body.fourL_F,
    )
    return JSONResponse(
        {
            "success": True,
            "msg": "Update Successful",
            "data": None,
        },
        status.HTTP_200_OK,
    )

@router.get(path="/fwd_scan", name="screeners:fwd_scan")
async def get_fwd_scan():
    (expiry_1_abv_fwd, 
     expiry_1_blw_fwd, 
     expiry_2_abv_fwd, 
     expiry_2_blw_fwd) = memory_fwd_scan_iv.expiry(-1)

    # print(f"expiry_2_abv_fwd, {expiry_2_abv_fwd.isnull().sum()}")
    
    data = {
        "expiry_1_abv_fwd": json.loads(expiry_1_abv_fwd.to_json(orient="records")),
        "expiry_1_blw_fwd": json.loads(expiry_1_blw_fwd.to_json(orient="records")),
        "expiry_2_abv_fwd": json.loads(expiry_2_abv_fwd.to_json(orient="records")),
        "expiry_2_blw_fwd": json.loads(expiry_2_blw_fwd.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All Fwd Data", "data": data},
        status.HTTP_200_OK,
    )
    
@router.get(path="/price_scan", name="screeners:price_scan")
async def get_price_scan():
    (expiry_1_abv_price, 
     expiry_1_blw_price, 
     expiry_2_abv_price, 
     expiry_2_blw_price,
     move_track) = memory_price_change.expiry(-1)
    
    data = {
        "expiry_1_abv_price": json.loads(expiry_1_abv_price.to_json(orient="records")),
        "expiry_1_blw_price": json.loads(expiry_1_blw_price.to_json(orient="records")),
        "expiry_2_abv_price": json.loads(expiry_2_abv_price.to_json(orient="records")),
        "expiry_2_blw_price": json.loads(expiry_2_blw_price.to_json(orient="records")),
        "move_track": json.loads(move_track.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All Price Data", "data": data},
        status.HTTP_200_OK,
    )
    
@router.get(path="/strike_ls", name="screeners:strike_ls")
async def get_strike_ls_scan():
    (compare_df_1, 
     compare_df_2,
     display_df_1,
     vol_morn,
     skew_morn, skew_morn_fair, long_df_1, short_df_1,
     intra_long_df_1, intra_short_df_1) = memory_strike_ls.expiry(1)
    
    data = {
        "compare_df_1": json.loads(compare_df_1.to_json(orient="records")),
        "compare_df_2": json.loads(compare_df_2.to_json(orient="records")),
        "display_df": json.loads(display_df_1.to_json(orient="records")),
        "vol_morn": json.loads(vol_morn.to_json(orient="records")),
        "skew_morn": json.loads(skew_morn.to_json(orient="records")),
        "skew_morn_fair": json.loads(skew_morn_fair.to_json(orient="records")),
        "long_df": json.loads(long_df_1.to_json(orient="records")),
        "short_df": json.loads(short_df_1.to_json(orient="records")),
        "intra_long_df_1": json.loads(intra_long_df_1.to_json(orient="records")),
        "intra_short_df_1": json.loads(intra_short_df_1.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All strike ls Data", "data": data},
        status.HTTP_200_OK,
    )

@router.get(path="/calendars", name="screeners:calendars")
async def get_calendar_scan():
    (calendar_1, calendar_1_low_ivp_pos, calendar_1_low_ivp_neg, calendar_1_high_ivp_pos, calendar_1_high_ivp_neg,
    calendar_2, calendar_2_low_ivp_pos, calendar_2_low_ivp_neg, calendar_2_high_ivp_pos, calendar_2_high_ivp_neg) = memory_calendars.get_data(3)
    
    data = {
        "calendar_1": json.loads(calendar_1.to_json(orient="records")),
        "calendar_1_low_ivp_pos": json.loads(calendar_1_low_ivp_pos.to_json(orient="records")),
        "calendar_1_low_ivp_neg": json.loads(calendar_1_low_ivp_neg.to_json(orient="records")),
        "calendar_1_high_ivp_pos": json.loads(calendar_1_high_ivp_pos.to_json(orient="records")),
        "calendar_1_high_ivp_neg": json.loads(calendar_1_high_ivp_neg.to_json(orient="records")),
        "calendar_2": json.loads(calendar_2.to_json(orient="records")),
        "calendar_2_low_ivp_pos": json.loads(calendar_2_low_ivp_pos.to_json(orient="records")),
        "calendar_2_low_ivp_neg": json.loads(calendar_2_low_ivp_neg.to_json(orient="records")),
        "calendar_2_high_ivp_pos": json.loads(calendar_2_high_ivp_pos.to_json(orient="records")),
        "calendar_2_high_ivp_neg": json.loads(calendar_2_high_ivp_neg.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All Calendar Data", "data": data},
        status.HTTP_200_OK,
    )

class CalendarUpdateItem(BaseModel):
    lowIVP: float
    highIVP: float

@router.post(path="/cal-update", name="screeners:cal-update")
async def calendarUpdate(body: CalendarUpdateItem):

    memory_calendars.update_calendar_settings(
        low_ivp=body.lowIVP,
        high_ivp=body.highIVP,
    )
    return JSONResponse(
        {
            "success": True,
            "msg": "Update Successful",
            "data": None,
        },
        status.HTTP_200_OK,
    )

@router.get(path="/bcrs", name="screeners:bcrs")
async def get_bcrs_scan():
    (bcrs_df, bprs_df, strad_df) = memory_bcrs.get_data(1)
    
    data = {
        "bcrs_df": json.loads(bcrs_df.to_json(orient="records")),
        "bprs_df": json.loads(bprs_df.to_json(orient="records")),
        "strad_df": json.loads(strad_df.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All BCRS Data", "data": data},
        status.HTTP_200_OK,
    )

@router.get(path="/intra_long_short", name="screeners:intra_long_short")
async def get_intra_long_short_scan():
    (short_df, long_df) = memory_intra_long_short.expiry(1)
    
    data = {
        "short_df": json.loads(short_df.to_json(orient="records")),
        "long_df": json.loads(long_df.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All Intra Data", "data": data},
        status.HTTP_200_OK,
    )

@router.get(path="/lsiv_scan", name="screeners:lsiv_scan")
async def get_ls_iv():
    (expiry_1_short, 
     expiry_1_long, 
     expiry_2_short, 
     expiry_2_long,
     expiry_1_short_result, 
     expiry_1_long_result, 
     expiry_2_short_result, 
     expiry_2_long_result) = memory_ls_iv.expiry(-1)
    
    data = {
        "expiry_1_short": json.loads(expiry_1_short.to_json(orient="records")),
        "expiry_1_long": json.loads(expiry_1_long.to_json(orient="records")),
        "expiry_2_short": json.loads(expiry_2_short.to_json(orient="records")),
        "expiry_2_long": json.loads(expiry_2_long.to_json(orient="records")),
        "expiry_1_short_result": json.loads(expiry_1_short_result.to_json(orient="records")),
        "expiry_1_long_result": json.loads(expiry_1_long_result.to_json(orient="records")),
        "expiry_2_short_result": json.loads(expiry_2_short_result.to_json(orient="records")),
        "expiry_2_long_result": json.loads(expiry_2_long_result.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All ls_iv Data", "data": data},
        status.HTTP_200_OK,
    )



@router.get(path="/atr_scan", name="screeners:atr_scan")
async def get_ls_iv():
    (expiry_1_short, 
     expiry_1_long, 
     expiry_2_short, 
     expiry_2_long) = memory_atr.expiry(-1)
    
    data = {
        "expiry_1_short": json.loads(expiry_1_short.to_json(orient="records")),
        "expiry_1_long": json.loads(expiry_1_long.to_json(orient="records")),
        "expiry_2_short": json.loads(expiry_2_short.to_json(orient="records")),
        "expiry_2_long": json.loads(expiry_2_long.to_json(orient="records")),
    }
    return JSONResponse(
        {"success": True, "msg": "All atr Data", "data": data},
        status.HTTP_200_OK,
    )




@router.get(path="/surface_scan", name="screeners:surface_scan")
async def get_surface_scan():
    (intraday_short, 
     intraday_long, 
     eod_short, 
     eod_long,
     intraday_short_avg,
     intraday_long_avg,
     eod_short_avg,
     eod_long_avg) = memory_surface_scan_iv.expiry(-1)
   
    data = {
        "intraday_short": json.loads(intraday_short.to_json(orient="records")),
        "intraday_long": json.loads(intraday_long.to_json(orient="records")),
        "eod_short": json.loads(eod_short.to_json(orient="records")),
        "eod_long": json.loads(eod_long.to_json(orient="records")),
        "intraday_short_avg": json.loads(intraday_short_avg.to_json(orient="records")),
        "intraday_long_avg": json.loads(intraday_long_avg.to_json(orient="records")),
        "eod_short_avg": json.loads(eod_short_avg.to_json(orient="records")),
        "eod_long_avg": json.loads(eod_long_avg.to_json(orient="records")),

    }
    return JSONResponse(
        {"success": True, "msg": "All Surface Data", "data": data},
        status.HTTP_200_OK,
    )





