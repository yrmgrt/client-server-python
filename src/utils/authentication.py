import os
import typing

from pydantic import BaseModel
from config.manager import settings

directory = settings.AUTH_STORAGE_DIR
os.makedirs(directory, exist_ok=True)

SECRET_FILE = directory / "secret.json"


class CredentialsNotFound(FileNotFoundError):
    """Exception raised when credentials are not found in the storage directory."""

    pass


class InvalidCredentials(PermissionError):
    """Exception raised when credentials are invalid"""

    pass


class Credentials(BaseModel):
    username: str
    password: str


def get_creds() -> Credentials:
    try:
        with open(SECRET_FILE, "r", encoding="utf8") as file:
            creds_raw = file.read()
        creds = Credentials.model_validate_json(creds_raw)
    except FileNotFoundError as exc:
        raise CredentialsNotFound(
            f"Credentials file not found at {SECRET_FILE}"
        ) from exc

    return creds


def set_creds(creds: Credentials):
    with open(SECRET_FILE, "w+", encoding="utf8") as handle:
        handle.write(
            creds.model_dump_json(
                indent=4,
            )
        )
    return True


def delete_creds():
    try:
        os.remove(SECRET_FILE)
    except FileNotFoundError as exc:
        return False
    return True


def check_creds() -> typing.Union[
    typing.Tuple[typing.Literal[True], Credentials],
    typing.Tuple[typing.Literal[False], None],
]:
    try:
        creds = get_creds()
    except (CredentialsNotFound, InvalidCredentials) as exc:
        print(f"Invalid credentials: {exc}")
        return False, None
    return True, creds
