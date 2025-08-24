from datetime import date
from functools import lru_cache
from typing import List

from pydantic import TypeAdapter
from utils.api import get_unauthenticated_session
from utils.dates import format_date, getMonthlyExpiryDate
from config.manager import settings


@lru_cache()
def get_cached_monthly_expiry_dates():
    return load_cached_expiry_dates()


def get_expiries_from_athena():
    endpoint = "/expiries"
    with get_unauthenticated_session() as session:
        response = session.get(endpoint)
        response.raise_for_status()
        data = response.json()
        expiries = data.get("expiries")

    if expiries is None:
        return get_computed_expiries()

    expiries = TypeAdapter(List[date]).validate_python(expiries)

    return expiries


def load_cached_expiry_dates():

    if settings.expiry_dates:
        return settings.expiry_dates

    return get_expiries_from_athena()


def get_computed_expiries():
    expiry1 = getMonthlyExpiryDate(0)
    expiry2 = getMonthlyExpiryDate(1)
    expiry3 = getMonthlyExpiryDate(2)

    cached_expiry_dates = [expiry1, expiry2, expiry3]
    return cached_expiry_dates


MONTHLY_EXPIRY_1, MONTHLY_EXPIRY_2, MONTHLY_EXPIRY_3 = get_cached_monthly_expiry_dates()

EXPIRY = [
    format_date(MONTHLY_EXPIRY_1, "%Y-%m-%d"),
    format_date(MONTHLY_EXPIRY_2, "%Y-%m-%d"),
    format_date(MONTHLY_EXPIRY_3, "%Y-%m-%d"),
]
