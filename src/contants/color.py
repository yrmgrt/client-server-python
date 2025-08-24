from datetime import date
from typing import TypedDict, Literal, Optional
from .dates import MONTHLY_EXPIRY_1, MONTHLY_EXPIRY_2


class Color(TypedDict):
    "light-orange"
    "dark-blue"
    "light-yellow"
    "light-blue"


COLOR = Color(
    {
        "light-orange": "orange",
        "dark-blue": "cyan",
        "light-yellow": "yellow",
        "light-blue": "lightblue",
    }
)

# export key of COLOR
TColor = Literal["light-orange", "dark-blue", "light-yellow", "light-blue"]

# export value of COLOR
TColorValue = str


def get_color(
    confirmed_date: Optional[date], expected_date: Optional[date]
) -> Optional[TColorValue]:
    today = date.today()

    if confirmed_date:
        if confirmed_date < today:
            return COLOR["light-orange"]
        if confirmed_date < MONTHLY_EXPIRY_1:
            return COLOR["dark-blue"]
        if confirmed_date < MONTHLY_EXPIRY_2:
            return COLOR["light-yellow"]

    if expected_date and today < expected_date and expected_date < MONTHLY_EXPIRY_1:
        return COLOR["light-blue"]

    return None
