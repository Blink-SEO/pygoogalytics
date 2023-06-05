import datetime
import re
from typing import List, Tuple, Optional


QUESTION = r"((^what)|(^why)|(^how)|(^can )|(^do )|(^does)|(^where)|(^who(se)? )|(who'?s )|(^which)|(^when)|(^is )|(^are )|(.*\?$))"
TRANSACTION = r"(.*((buy)|(cost)|(price)|(cheap)|(pricing)|(affordable)))"
INVESTIGATION = r"(.*((best)|(most)|(cheapest)|( vs)|( v\.s\.)))"
URL = r"https?://(www\.)?[\w\-_+]*(\.\w{2,4}){0,2}/"

RE_QUESTION = re.compile(QUESTION)
RE_TRANSACTIONAL = re.compile(TRANSACTION)
RE_INVESTIGATION = re.compile(INVESTIGATION)
RE_URL = re.compile(URL)
RE_C2S = re.compile(r"(?<!^)(?=[A-Z])")


def strip_url(url: str) -> str:
    url = url.split('?')[0]
    _match = RE_URL.match(url)
    if _match:
        return url[_match.span()[1] - 1:]
    else:
        return url


def url_extract_parameter(url: str) -> Optional[str]:
    if '?' in url:
        return url.split('?')[1]
    else:
        return None


def url_strip_domain(url: str) -> Optional[str]:
    _match = RE_URL.match(url)
    if _match:
        return url[_match.span()[1] - 1:]
    else:
        return url


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


def test_time(t: datetime.datetime, seconds: float) -> Optional[bool]:
    if not isinstance(t, datetime.datetime):
        return None
    _diff = (datetime.datetime.now() - t).seconds
    if _diff < seconds:
        return True
    else:
        return False
