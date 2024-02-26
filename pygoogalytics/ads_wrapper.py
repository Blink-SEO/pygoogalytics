import datetime
import re

from .utils import ads_reports_parsing
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v15.services.types.google_ads_service import GoogleAdsRow

import pandas as pd

class AdsWrapper:
    def __init__(self,
                 googleads_client: GoogleAdsClient,
                 customer_id: str,):
        self.client = googleads_client
        self.service = self.client.get_service("GoogleAdsService", version='v15')
        self.customer_id = customer_id

    def get_data(self,
                      resource: str,
                      date: datetime.date | str | tuple[datetime.date | str, datetime.date | str] | None,
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
        return ads_reports_parsing.tidy_column_names(
            pd.DataFrame.from_records(rows)
        )


    def _make_request(self,
                     resource: str,
                     date: datetime.date | str | tuple[datetime.date | str, datetime.date | str] | None,
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
        if date is not None:
            _dc = ads_reports_parsing.make_date_condition(dt=date)
            conditions = [_dc] + conditions

        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.customer_id
        q = ads_reports_parsing.create_google_ads_sql_query(
            resource=resource,
            dimensions=dimensions + metrics,
            conditions=conditions,
            custom_condition_expr=custom_condition_expr,
            limit=limit
        )
        if _print_query:
            print(q)
        search_request.query = re.sub(r'\n', '', q)

        stream = self.service.search_stream(search_request)
        raw_rows = []
        for batch in stream:
            raw_rows.extend([row for row in batch.results])
        if _return_raw:
            return raw_rows
        return ads_reports_parsing.extract_google_ads_rows(
            raw_rows, dimensions=dimensions + metrics
        )







