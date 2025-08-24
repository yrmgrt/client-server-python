from dataclasses import dataclass
import json
import sys
from typing import List, Literal, Optional
from contextlib import contextmanager
import httpx
import requests
import logging
from cachetools import cached, TTLCache
from config.manager import settings
from .authentication import get_creds, Credentials

BACKEND_URL = settings.BACKEND_URL
API_ENDPOINT = {
    "ATM_IV": "atm_iv",
    "SKEW": "skew",
    "TOKEN_SET": "filtered-token-set",
    "Z_SCORE": "z-score",
}


def raise_for_status(response: httpx.Response | requests.Response):
    try:
        response.raise_for_status()
    except (httpx.HTTPStatusError, requests.HTTPError) as e:
        logging.exception(
            "HTTP error occurred: %r with response body: %r", e, response.text
        )
        raise


@dataclass
class DeltaRange:
    pe: List[str]
    ce: List[str]


@dataclass
class sym_strike:
    sym: {DeltaRange}


@cached(TTLCache(maxsize=1, ttl=settings.RT_VALIDITY))
def login():
    creds = get_creds()
    try:
        response = login_inner(creds)
        return response.json()["data"]
    except requests.RequestException as error:
        logging.error("Error during login: %s", error)
        sys.exit(1)


def login_inner(creds: "Credentials"):
    endpoint = f"{BACKEND_URL}/auth/login"
    payload = {"username": creds.username, "password": creds.password}
    response = requests.post(endpoint, json=payload)
    raise_for_status(response)
    return response


@cached(TTLCache(maxsize=1, ttl=settings.AT_VALIDITY))
def get_auth_token():

    res = login()
    endpoint = f"{BACKEND_URL}/auth/refresh"
    payload = {"refresh_token": res["refresh_token"]}
    try:
        response = requests.post(endpoint, json=payload)
        raise_for_status(response)
        res = response.json()
    except requests.RequestException as error:
        login.cache.clear()
        logging.warning("Error during token refresh: %s", error)
        return get_auth_token()

    return res["data"]["access_token"]


@contextmanager
def get_session():
    authorization = f"Bearer {get_auth_token()}"
    with httpx.Client(
        base_url=BACKEND_URL, headers={"Authorization": authorization}
    ) as session:
        yield session


@contextmanager
def get_unauthenticated_session():
    with httpx.Client(base_url=BACKEND_URL) as session:
        yield session


def get_atm_iv_from_api():
    try:
        with get_session() as session:
            response = session.get(API_ENDPOINT["ATM_IV"])
            raise_for_status(response)
            return response.json()
    except httpx.RequestError as error:
        logging.error("Error fetching atm iv: %s", error)
        return None


def get_z_score_from_api():
    try:
        with get_session() as session:
            response = session.get(API_ENDPOINT["Z_SCORE"])
            raise_for_status(response)
            return response.json()
    except httpx.RequestError as error:
        logging.error("Error fetching z score: %s", error)
        return None


def get_skew_from_api():
    try:
        with get_session() as session:
            response = session.get(API_ENDPOINT["SKEW"])
            raise_for_status(response)
            return response.json()
    except httpx.RequestError as error:
        logging.error("Error fetching skew: %s", error)
        return None


def get_all_token_set(
    oi: Optional[Literal["Max", "Min"]] = None,
    delta: Optional[DeltaRange] = None,
    expiry: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    strike_diff: Optional[List[str]] = None,
    strikes: Optional[sym_strike] = None,
):
    body = {
        "oi": oi,
        "delta": delta,
        "expiry": expiry,
        "symbols": symbols,
        "strike_diff": strike_diff,
        "strikes": strikes,
    }

    try:
        with get_session() as session:
            response = session.post(API_ENDPOINT["TOKEN_SET"], json=body)
            raise_for_status(response)
            return response.json()
    except httpx.RequestError as error:
        logging.error("Error fetching token set: %s", error)
        return None
