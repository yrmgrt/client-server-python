from functools import cached_property
import json
import logging
import os
import pathlib
from typing import Dict, Optional, Union
import typing

import decouple
from dotenv import find_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import date

ROOT_DIR: pathlib.Path = pathlib.Path(
    __file__
).parent.parent.parent.parent.parent.resolve()

PARENT_DIR = pathlib.Path(find_dotenv()).parent.resolve()


class BackendBaseSettings(BaseSettings):
    TITLE: str = "Client Screener Application"
    VERSION: str = "0.1.0"
    TIMEZONE: str = "UTC"
    DESCRIPTION: Optional[str] = None
    DEBUG: bool = False
    ATM_UPDATE_INTERVAL: int = 1000  # in miliseconds

    SERVER_HOST: str = decouple.config("BACKEND_SERVER_HOST", cast=str)  # type: ignore
    SERVER_PORT: int = decouple.config("BACKEND_SERVER_PORT", cast=int)  # type: ignore
    SERVER_WORKERS: int = decouple.config("BACKEND_SERVER_WORKERS", cast=int)  # type: ignore
    API_PREFIX: str = "/api/v1"
    DOCS_URL: str = "/docs"
    OPENAPI_URL: str = "/openapi.json"
    REDOC_URL: str = "/redoc"
    OPENAPI_PREFIX: str = ""

    ALLOWED_ORIGINS: list[str] = decouple.config(
        "BACKEND_ALLOWED_ORIGINS",
        default=["http://localhost:3000", "http://127.0.0.1:3000"],
        cast=lambda x: x if isinstance(x, list) else x.split("|"),
    )

    ALLOWED_METHODS: list[str] = ["*"]
    ALLOWED_HEADERS: list[str] = ["*"]

    LOGGING_LEVEL: int = logging.INFO

    CHECK_HOST: bool = decouple.config("CHECK_HOST", cast=bool, default=True)  # type: ignore

    AT_VALIDITY: float = decouple.config("ATH_VALIDITY", cast=float, default=30 * 60.0)  # type: ignore
    RT_VALIDITY: float = decouple.config("RT_VALIDITY", cast=float, default=12 * 60 * 60.0)  # type: ignore

    BACKEND_URL: str = decouple.config("ATHENA_SERVER_URL", cast=str)  # type: ignore

    AUTH_STORAGE_DIR: pathlib.Path = decouple.config("AUTH_STORAGE_DIR", cast=pathlib.Path, default=PARENT_DIR / "auth_storage")  # type: ignore

    class Config(SettingsConfigDict):
        case_sensitive: bool = True
        env_file: str = f"{str(ROOT_DIR)}/.env"
        validate_assignment: bool = True

    @cached_property
    def expiry_dates(self) -> typing.List[date] | None:
        env_var = "EXPIRY_DATES"
        if env_var in os.environ:
            return [
                date.fromisoformat(date_str.strip())
                for date_str in os.environ[env_var].split(";")
            ]
        return None

    @property
    def set_backend_app_attributes(self) -> Dict[str, Optional[Union[str, bool]]]:
        """
        Set all `FastAPI` class' attributes with the custom values defined in `BackendBaseSettings`.
        """
        return {
            "title": self.TITLE,
            "version": self.VERSION,
            "debug": self.DEBUG,
            "description": self.DESCRIPTION,
            "docs_url": self.DOCS_URL,
            "openapi_url": self.OPENAPI_URL,
            "redoc_url": self.REDOC_URL,
            "openapi_prefix": self.OPENAPI_PREFIX,
            "api_prefix": self.API_PREFIX,
            "atm_update_interval": self.ATM_UPDATE_INTERVAL,
        }
