import datetime
import re

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

    def make_request(self,
                     resource: str,
                     date: datetime.date | str | tuple[datetime.date | str, datetime.date | str],
                     dimensions: list[str],
                     metrics: list[str],
                     conditions: list | None = None,
                     custom_condition_expr: str | None = None,
                     limit: int | None = None):

        metrics = [f"metrics.{m}" for m in metrics]
        custom_condition_expr = make_date_condition(dt=date) + " AND " + custom_condition_expr

        search_request = create_request(
            account_id=self.customer_id,
            resource=resource,
            dimensions=dimensions,
            metrics=metrics,
            conditions=conditions,
            custom_condition_expr=custom_condition_expr,
            limit=limit
        )
        stream = self.service.search_stream(search_request)
        raw_rows = []
        for batch in stream:
            raw_rows.extend([row for row in batch.results])
        rows = extract_google_ads_rows(raw_rows, dimensions=dimensions+metrics)
        return pd.DataFrame.from_records(rows)



def create_google_ads_sql_query(
        resource: str,
        dimensions: list[str],
        metrics: list[str],
        conditions: list | None = None,
        custom_condition_expr: str | None = None,
        limit: int | None = None) -> str:

    dimensions_list = ', '.join(dimensions)
    metrics_list = ', '.join(metrics)

    q = f"""SELECT {dimensions_list}, {metrics_list} FROM {resource}"""

    _con_expressions = []

    if custom_condition_expr:
         _con_expressions.append(custom_condition_expr)

    if not conditions:
        conditions = []
    for _c in conditions:
        if len(_c) == 2:
            _c = (_c[0], '=', _c[1])
        if isinstance(_c[2], str):
            _c = (_c[0], _c[1], f'"{_c[2]}"')
        _con_expressions.append(" ".join([str(_) for _ in _c]) + " ")

    if len(_con_expressions) > 0:
        q += " WHERE " + " AND ".join(_con_expressions)

    if limit:
        q += f" LIMIT {limit}"

    print(q)
    return q


def create_request(account_id: str,
                   resource: str,
                   dimensions: list[str],
                   metrics: list[str],
                    conditions: list | None = None,
                    custom_condition_expr: str | None = None,
                    limit: int | None = None):
    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = account_id
    search_request.query = create_google_ads_sql_query(
        resource=resource,
        dimensions=dimensions,
        metrics=metrics,
        conditions=conditions,
        custom_condition_expr=custom_condition_expr,
        limit=limit
    )
    return search_request


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
    return [{_dim: parse_enum(eval('r.'+_dim)) for _dim in dimensions} for r in rows]


def make_date_condition(dt: datetime.date | str) -> str:
    if isinstance(dt, tuple):
        return f" segments.date BETWEEN '{parse_date(dt[0])}' AND '{parse_date(dt[1])}' "
    else:
        return f" segments.date = '{parse_date(dt)}' "


def parse_date(dt: datetime.date | str) -> str:
    if isinstance(df, datetime.date):
        return dt.strftime("%Y-%m-%d")
    elif re.match(r'\d{4}-\d{2}-\d{2}', dt):
        return dt
    else:
        raise ValueError("date given in incorrect format")