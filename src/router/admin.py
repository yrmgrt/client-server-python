from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel
import requests
from config.manager import settings
from config.events import scheduler
from apscheduler.triggers.interval import IntervalTrigger

from utils.api import login_inner
from utils.authentication import Credentials, check_creds, set_creds

router = APIRouter(prefix="/admin", tags=["admin"])


class IntervalUpdateRequest(BaseModel):
    interval: int


@router.get(path="/updateInterval", name="admin:updateInterval")
async def update_inteval(request: IntervalUpdateRequest):
    new_interval = request.interval
    if new_interval <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Interval must be positive."
        )

    # Update the settings or configuration with the new interval
    settings.ATM_UPDATE_INTERVAL = new_interval

    if scheduler.get_job("atm_iv_update"):
        scheduler.reschedule_job(
            "atm_iv_update", trigger=IntervalTrigger(seconds=new_interval / 1000)
        )

    return {"success": True, "interval": new_interval}


@router.post("/login", name="admin:userLogin")
def user_login(payload: Credentials):

    try:
        login_inner(payload)
    except (httpx.HTTPStatusError, requests.HTTPError) as exc:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "success": False,
                "msg": "Cannot login with these credentials. Check with your Admin",
                "data": None,
            },
        )

    set_creds(payload)

    return {
        "success": True,
        "msg": "Credentials updated successfully",
        "data": payload.model_dump(mode="json"),
    }


@router.get("/is-logged-in", name="admin:isLoggedIn")
def is_logged_in():
    creds_status, creds = check_creds()
    if not creds_status:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "success": False,
                "msg": "User credentials are not on system",
                "data": {"creds_present": False},
            },
        )

    return {
        "success": True,
        "msg": "User credentials are present",
        "data": {"creds_present": True, **creds.model_dump(mode="json")},
    }

@router.get("/expiries" , name="admin:getExpiries")
def get_expiries():
    from contants.dates import get_cached_monthly_expiry_dates

    try:
        expiries = get_cached_monthly_expiry_dates()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"success": False, "msg": str(e), "data": None},
        )

    return {"success": True, "msg": "Expiries fetched successfully", "data": expiries}