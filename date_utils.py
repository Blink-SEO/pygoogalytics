import datetime
from typing import List, Tuple


def date_range_string(dates: List[datetime.date],
                      alternate_text: str = ""):
    if len(dates) == 0:
        date_range_str = alternate_text
    elif len(dates) == 1:
        date_range_str = f"{dates[0].strftime('%Y-%m-%d')}"
    else:
        _min, _max, _days, _range = date_range(dates)
        date_range_str = f"{_min.strftime('%Y-%m-%d')} to " \
                         f"{_max.strftime('%Y-%m-%d')} ({100 * _range:.0f}%)"
    return date_range_str


def date_range(dates: List[datetime.date]) -> Tuple[datetime.date, datetime.date, int, float]:
    _max = max(dates)
    _min = min(dates)
    _days = (_max - _min).days
    _coverage = len(set(dates))/_days
    return _min, _max, _days, _coverage