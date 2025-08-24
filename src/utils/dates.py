import datetime
import calendar


def get_last_thursday(year: int, month: int) -> datetime.date:
    """
    Returns the date of the last Thursday in the given month and year.

    Parameters:
    year (int): The year.
    month (int): The month (1-12).

    Returns:
    datetime.date: The date of the last Thursday in the given month and year.
    """
    # Get the number of weeks in the month
    weeks_in_month = calendar.monthcalendar(year, month)

    # The last week that has a Thursday is the one we are looking for
    # Since weeks are 0-indexed and we start counting from the beginning of the month,
    # the last week will be the last element in the weeks_in_month list
    last_week = weeks_in_month[-1]

    # Get the day of the month for the last Thursday
    # Thursdays are represented by the index 3 in the week list (Monday is 0, Tuesday is 1, etc.)
    last_thursday_day = last_week[3]

    # If the last Thursday is 0, it means there is no Thursday in the last week of the month
    # This can happen if the month ends on a day before Thursday
    # In this case, we take the Thursday from the second last week
    if last_thursday_day == 0:
        last_week = weeks_in_month[-2]
        last_thursday_day = last_week[3]

    # Create and return the date object for the last Thursday
    return datetime.date(year, month, last_thursday_day)


def getMonthlyExpiryDate(offset: int) -> datetime.date:
    """
    Returns the monthly expiry date, which is the last Thursday of each month,
    offset by the given number of months.

    Parameters:
    offset (int): The number of months to offset from the current month.

    Returns:
    datetime.date: The monthly expiry date.
    """
    today = datetime.date.today()
    current_month = today.month
    current_year = today.year

    lastThursday = get_last_thursday(current_year, current_month)

    if today > lastThursday:
        current_month = current_month + 1 + offset

    else:
        current_month += offset

    if current_month > 12:
        current_month -= 12
        current_year += 1

    lastThursday = get_last_thursday(current_year, current_month)

    return lastThursday


def format_date(date: datetime.date, format="%d-%m-%Y"):
    return date.strftime(format)
