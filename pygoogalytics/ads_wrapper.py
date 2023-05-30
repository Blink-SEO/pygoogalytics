import datetime
import re
import operator
from typing import Any

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v12.services.types.google_ads_service import GoogleAdsRow

import pandas as pd

class AdsWrapper:
    def __init__(self,
                 googleads_client: GoogleAdsClient,
                 customer_id: str,):
        self.client = googleads_client
        self.service = self.client.get_service("GoogleAdsService")
        self.customer_id = customer_id

    def get_data(self,
                      resource: str,
                      date: datetime.date | str | tuple[datetime.date | str, datetime.date | str],
                      dimensions: list[str],
                      metrics: list[str],
                      conditions: list | None = None,
                      custom_condition_expr: str | None = None,
                      limit: int | None = None,
                    _return_raw: bool = False,
                 _print_query: bool = False):

        rows = self._make_request(
            resource=resource,
            date=date,
            metrics=metrics,
            dimensions=dimensions,
            conditions=conditions,
            custom_condition_expr=custom_condition_expr,
            limit=limit,
            _return_raw=_return_raw
        )
        if _return_raw:
            return rows
        return pd.DataFrame.from_records(rows)

    def _make_request(self,
                     resource: str,
                     date: datetime.date | str | tuple[datetime.date | str, datetime.date | str],
                     dimensions: list[str],
                     metrics: list[str] = None,
                     conditions: list | None = None,
                     custom_condition_expr: str | None = None,
                     limit: int | None = None,
                      _return_raw: bool = False,
                      _print_query: bool = False) -> list[dict | GoogleAdsRow]:

        if metrics is None:
            metrics = []
        metrics = [f"metrics.{m}" if not re.match(r'metrics\.', m) else m for m in metrics]

        if conditions is None:
            conditions = []
        conditions = [make_date_condition(dt=date)] + conditions

        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.customer_id
        search_request.query = create_google_ads_sql_query(
            resource=resource,
            dimensions=dimensions + metrics,
            conditions=conditions,
            custom_condition_expr=custom_condition_expr,
            limit=limit
        )

        stream = self.service.search_stream(search_request)
        raw_rows = []
        for batch in stream:
            raw_rows.extend([row for row in batch.results])
        if _return_raw:
            return raw_rows
        return extract_google_ads_rows(raw_rows, dimensions=dimensions + metrics)



def create_google_ads_sql_query(
        resource: str,
        dimensions: list[str],
        conditions: list | None = None,
        custom_condition_expr: str | None = None,
        limit: int | None = None) -> str:

    q = f"""SELECT {', '.join(dimensions)} FROM {resource}"""

    _con_expressions = []

    if not conditions:
        conditions = []
    for _c in conditions:
        _con_expressions.append(tuple_to_sql_cond(_c))
    if custom_condition_expr:
         _con_expressions.append(custom_condition_expr)
    if len(_con_expressions) > 0:
        q += " WHERE " + " AND ".join(_con_expressions)

    if limit:
        q += f" LIMIT {limit}"

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


def make_date_condition(dt: datetime.date | str) -> tuple[str, str, Any]:
    if isinstance(dt, tuple):
        return "segments.date", "BETWEEN", dt
    else:
        return "segments.date", "=", dt


def parse_date(dt: datetime.date | str) -> str:
    if isinstance(dt, datetime.date):
        return dt.strftime("%Y-%m-%d")
    elif re.match(r'\d{4}-\d{2}-\d{2}', dt):
        return dt
    else:
        raise ValueError("date given in incorrect format")


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