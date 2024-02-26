import datetime
import re
import operator
from typing import Any

from google.ads.googleads.v15.services.types.google_ads_service import GoogleAdsRow

import pandas as pd



def create_google_ads_sql_query(
        resource: str,
        dimensions: list[str],
        conditions: list | None = None,
        custom_condition_expr: str | None = None,
        limit: int | None = None) -> str:

    q = f"""SELECT {', '.join(dimensions)} \nFROM {resource}"""

    _con_expressions = []

    if not conditions:
        conditions = []
    for _c in conditions:
        _con_expressions.append(tuple_to_sql_cond(_c))
    if custom_condition_expr:
         _con_expressions.append(custom_condition_expr)
    if len(_con_expressions) > 0:
        q += " \nWHERE " + " \nAND ".join(_con_expressions)

    if limit:
        q += f" \nLIMIT {limit}"

    return q



def parse_enum(v):
    if isinstance(v, (str, float, bool)) or type(v) == int:
        return v
    if v is None:
        return v
    try:
        _out = v._name_
    except AttributeError:
        if isinstance(v, int):
            return int(v)
        else:
            _out = str(v)
    return _out


def extract_google_ads_rows(rows: list[GoogleAdsRow], dimensions: list[str]) -> list[dict]:
    return [{_dim: parse_enum(operator.attrgetter(_dim)(r)) for _dim in dimensions} for r in rows]


def parse_date(dt: datetime.date | str) -> str:
    if isinstance(dt, datetime.date):
        return dt.strftime("%Y-%m-%d")
    elif re.match(r'\d{4}-\d{2}-\d{2}', dt):
        return dt
    else:
        raise ValueError("date given in incorrect format")


def make_date_condition(dt: datetime.date | str) -> tuple[str, str, Any]:
    if isinstance(dt, tuple):
        return "segments.date", "BETWEEN", dt
    else:
        return "segments.date", "=", dt




def tuple_to_sql_cond(c: tuple) -> str:
    if len(c) == 2:
        c = (c[0], '=', c[1])

    if c[1] == 'BETWEEN' and isinstance(c[2], (tuple, list)):
        if len(c[2]) != 2:
            raise ValueError(f"BETWEEN condition expects 2 arguments, got {c[2]}")
        return f"{c[0]} {c[1]} {parse_sql_input(c[2][0])} AND {parse_sql_input(c[2][1])}"
    else:
        return f"{c[0]} {c[1]} {parse_sql_input(c[2])}"


def parse_sql_input(v) -> str:
    if isinstance(v, str):
        return '"' + v + '"'
    elif isinstance(v, datetime.date):
        return '"' + dt.strftime("%Y-%m-%d") + '"'
    else:
        return v.__repr__()
    # if _c[1] == 'BETWEEN' and isinstance(_c[2], (tuple, list)):
    #     pass


def tidy_column_names(df: pd.DataFrame) -> pd.DataFrame:
    repl = dict()
    for c in df.columns:
        r = c
        if m := re.match(r'metrics\.(\w+)', c):
            r = m.group(1)
        repl[c] = re.sub(r'\.', '/', r)
    return df.rename(columns=repl)
