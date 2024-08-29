import datetime
import re
from typing import List, Tuple, Optional


QUESTION = r"((^what)|(^why)|(^how)|(^can )|(^do )|(^does)|(^where)|(^who(se)? )|(who'?s )|(^which)|(^when)|(^is )|(^are )|(.*\?$))"
TRANSACTION = r"(.*((buy)|(cost)|(price)|(cheap)|(pricing)|(affordable)))"
INVESTIGATION = r"(.*((best)|(most)|(cheapest)|( vs)|( v\.s\.)))"
URL = r"https?://(www\.)?[\w\-_+]*(\.\w{2,4}){0,2}/"
URL_PATH_CAPTURE = r"(?:https?://)?[^/]*(/.*)"

RE_QUESTION = re.compile(QUESTION)
RE_TRANSACTIONAL = re.compile(TRANSACTION)
RE_INVESTIGATION = re.compile(INVESTIGATION)
RE_URL = re.compile(URL)
RE_URL_PATH_CAPTURE = re.compile(URL_PATH_CAPTURE)
RE_C2S = re.compile(r"(?<!^)(?=[A-Z])")


def camel_to_snake(string: str):
    return RE_C2S.sub('_', string).lower()

def strip_url(url: str) -> str:
    url = url.split('?')[0]
    if _match := RE_URL_PATH_CAPTURE.match(url):
        return _match.group(1)
    else:
        return url


def url_extract_parameter(url: str) -> Optional[str]:
    if '?' in url:
        return url.split('?')[1]
    else:
        return None


def url_strip_domain(url: str) -> Optional[str]:
    _match = RE_URL.match(url)
    if _match := RE_URL_PATH_CAPTURE.match(url):
        return _match.group(1)
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

    if _min == _max:
        return _min, _max, 1, 1

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


def dict_merge(dict1, dict2):
    d = dict()
    for k, v in dict1.items():
        d[k] = v
    for k, v in dict2.items():
        d[k] = v
    return d


def expand_list(list_of_lists: list[list]) -> list:
    while any(isinstance(_l, list) for _l in list_of_lists):
        _new_list = []
        for _item in list_of_lists:
            if isinstance(_item, list):
                _new_list.extend(_item)
            else:
                _new_list.append(_item)
        list_of_lists = _new_list
    return list_of_lists

def parse_date(d):
    if m:=re.match(r"\d{8}", d):
        return datetime.datetime.strptime(m.group(0), "%Y%m%d").date()
    elif m:=re.match(r"\d{4}-\d{2}-\d{2}", d):
        return datetime.datetime.strptime(m.group(0), "%Y-%m-%d").date()
    elif m:=re.match(r"\d{4} \d{2} \d{2}", d):
        return datetime.datetime.strptime(m.group(0), "%Y %m %d").date()
    else:
        raise ValueError(f"Cannot parse date string '{d}'")