import datetime
import logging
import time

import pandas as pd
import numpy as np
import uuid
import re

from math import ceil
from typing import Any

from google.ads.googleads.client import GoogleAdsClient
# from google.ads.googleads.v10.resources import KeywordPlanGeoTarget
from google.ads.googleads.errors import GoogleAdsException
from google.api_core.exceptions import InternalServerError
from google.api_core.exceptions import ResourceExhausted

from . import LOCATION_ID_DICT

DEFAULT_LOCATIONS = "GBR"  # location defaults GBR
DEFAULT_LANGUAGE_ID = "1000"  # language defaults to "1000" (i.e. English)

DEFAULT_KEYWORD_PLAN_CAMPAIGN_CPC_BID = 1000000
DEFAULT_KEYWORD_PLAN_AD_GROUP_CPC_BID = 250000
API_MULTIPLE_REQUEST_WAIT_TIME = 2

RE_URL = re.compile(r"https?://(www\.)?[\w\-_+]*(\.\w{2,4}){0,2}/")


class ClientWrapper:
    def __init__(self,
                 googleads_client: GoogleAdsClient,
                 customer_id: str,
                 location_codes: list[str] | None = None,
                 language_id: str = None,
                 api_version: str | int = 15
                 ):
        self.client: GoogleAdsClient = googleads_client
        self.customer_id = customer_id

        if isinstance(api_version, int):
            api_version = "v" + str(api_version)
        self.googleads_service = self.client.get_service("GoogleAdsService", version=api_version)

        if location_codes is None:
            location_codes = DEFAULT_LOCATIONS
        if isinstance(location_codes, str):
            location_codes = [location_codes]
        self.location_codes = location_codes

        if language_id is None:
            language_id = DEFAULT_LANGUAGE_ID
        self.language_id = language_id

        self.location_rns = _map_location_to_resource_names(client=self.client,
                                                            location_codes=self.location_codes)
        self.language_rn = _map_language_id_to_resource_name(client=self.client,
                                                             language_id=self.language_id)

    def get_campaigns(self) -> pd.DataFrame:
        query = """
            SELECT
              campaign.id,
              campaign.name
            FROM campaign
            ORDER BY campaign.id
        """

        # Issues a search request using streaming.
        stream = self.googleads_service.search_stream(customer_id=self.customer_id, query=query)

        _out = []
        for batch in stream:
            for row in batch.results:
                _out.append({"id": row.campaign.id,
                             "name": row.campaign.name})
        return pd.DataFrame(_out)

    def get_ad_groups(self, campaign_id=None) -> pd.DataFrame:
        query = """
            SELECT
              campaign.id,
              ad_group.id,
              ad_group.name
            FROM ad_group"""

        if campaign_id:
            query += f" WHERE campaign.id = {campaign_id}"

        search_request = self.client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query
        search_request.page_size = 1000

        results = self.googleads_service.search(request=search_request)

        _out = []
        for row in results:
            _out.append({"campaign_id": row.campaign.id,
                         "ad_group_name": row.ad_group.name,
                         "ad_group_id": row.ad_group.id})
        return pd.DataFrame(_out)

    def get_keyword_plan_ad_groups(self) -> pd.DataFrame:
        """Return keyword_plan ad groups -- these don't show up as regular ad groups"""

        query = """
            SELECT
              keyword_plan.id,
              keyword_plan.name,
              keyword_plan_ad_group.id,
              keyword_plan_ad_group.name,
              keyword_plan_campaign.id,
              keyword_plan_campaign.name
            FROM keyword_plan_ad_group"""

        search_request = self.client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query
        search_request.page_size = 1000

        results = self.googleads_service.search(request=search_request)

        _out = []
        for row in results:
            _out.append({"keyword_plan_name": row.keyword_plan.name,
                         "keyword_plan_id": row.keyword_plan.id,
                         "ad_group_name": row.keyword_plan_ad_group.name,
                         "ad_group_id": row.keyword_plan_ad_group.id,
                         "campaign_name": row.keyword_plan_campaign.name,
                         "campaign_id": row.keyword_plan_campaign.id
                         })
        return pd.DataFrame(_out)

    def get_keyword_plan_ad_group_keywords(self,
                                           keyword_plan_resource_name: str = None,
                                           keyword_plan_id: int = None) -> pd.DataFrame:
        query = """
            SELECT
              keyword_plan.id,
              keyword_plan_ad_group_keyword.id,
              keyword_plan_ad_group_keyword.text
            FROM keyword_plan_ad_group_keyword"""
        if keyword_plan_resource_name:
            query += f" WHERE keyword_plan.resource_name = '{keyword_plan_resource_name}'"
        elif keyword_plan_id:
            query += f" WHERE keyword_plan.id = {keyword_plan_id}"

        search_request = self.client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query
        # search_request.page_size = 1000

        results = self.googleads_service.search(request=search_request)

        _out = []
        for row in results:
            _out.append({"keyword_plan_id": row.keyword_plan.id,
                         "keyword_id": row.keyword_plan_ad_group_keyword.id,
                         "keyword_resource_name": row.keyword_plan_ad_group_keyword.resource_name,
                         "keyword_text": row.keyword_plan_ad_group_keyword.text})
        return pd.DataFrame(_out)

    def get_keyword_plans(self) -> pd.DataFrame:
        """Return keyword_plan ad groups -- these don't show up as regular ad groups"""

        query = """
            SELECT
              keyword_plan.id,
              keyword_plan.name
            FROM keyword_plan"""

        search_request = self.client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query
        search_request.page_size = 1000

        results = self.googleads_service.search(request=search_request)

        _out = []
        for row in results:
            _out.append({"keyword_plan_id": row.keyword_plan.id,
                         "keyword_plan_name": row.keyword_plan.name,
                         "resource_name": row.keyword_plan.resource_name})

        return pd.DataFrame(_out)

    def _get_keyword_plan_ids(self) -> list[int]:
        """Return keyword_plan ad groups -- these don't show up as regular ad groups"""

        query = """
            SELECT
              keyword_plan.id,
              keyword_plan.name
            FROM keyword_plan"""

        search_request = self.client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query
        search_request.page_size = 1000

        results = self.googleads_service.search(request=search_request)

        _out = []
        for row in results:
            _out.append(row.keyword_plan.id)

        return _out

    def remove_campaign(self,
                        campaign_id: str = None,
                        campaign_resource_name: str = None):
        # https://developers.google.com/google-ads/api/docs/samples/remove-campaign
        campaign_service = self.client.get_service("CampaignService")
        campaign_operation = self.client.get_type("CampaignOperation")

        if campaign_id is not None:
            resource_name = campaign_service.campaign_path(self.customer_id, campaign_id)
        elif campaign_resource_name is not None:
            resource_name = campaign_resource_name
        else:
            raise ValueError("must supply either campaign_id or campaign_resource_name")

        campaign_operation.remove = resource_name

        campaign_response = campaign_service.mutate_campaigns(
            customer_id=self.customer_id, operations=[campaign_operation]
        )

        print(f"Removed campaign {campaign_response.results[0].resource_name}.")

    def remove_keyword_plan(self,
                            keyword_plan_id: int | str = None,
                            keyword_plan_resource_name: str = None):
        # https://developers.google.com/google-ads/api/docs/samples/remove-campaign
        keyword_plan_service = self.client.get_service("KeywordPlanService")
        keyword_plan_operation = self.client.get_type("KeywordPlanOperation")

        if keyword_plan_id is not None:
            if isinstance(keyword_plan_id, int):
                keyword_plan_id = str(keyword_plan_id)
            resource_name = keyword_plan_service.keyword_plan_path(self.customer_id, keyword_plan_id)
        elif keyword_plan_resource_name is not None:
            resource_name = keyword_plan_resource_name
        else:
            raise ValueError("must supply either campaign_id or campaign_resource_name")

        keyword_plan_operation.remove = resource_name

        keyword_plan_response = keyword_plan_service.mutate_keyword_plans(
            customer_id=self.customer_id, operations=[keyword_plan_operation]
        )

        # print(f"Removed campaign {keyword_plan_response.results[0].resource_name}.")

    def remove_ad_group(self,
                        ad_group_id: str = None,
                        ad_group_resource_name: str = None):
        ad_group_service = self.client.get_service("AdGroupService")
        ad_group_operation = self.client.get_type("AdGroupOperation")

        if ad_group_id is not None:
            resource_name = ad_group_service.ad_group_path(self.customer_id, ad_group_id)
        elif ad_group_resource_name is not None:
            resource_name = ad_group_resource_name
        else:
            raise ValueError("must supply either ad_group_id or ad_group_resource_name")

        ad_group_operation.remove = resource_name

        ad_group_response = ad_group_service.mutate_ad_groups(
            customer_id=self.customer_id, operations=[ad_group_operation]
        )

        # print(f"Removed ad group {ad_group_response.results[0].resource_name}.")

    def _remove_all_keyword_plans(self):
        keyword_plan_ids_list = self._get_keyword_plan_ids()

        for keyword_plan_id in keyword_plan_ids_list:
            self.remove_keyword_plan(keyword_plan_id=keyword_plan_id)


class KeywordPlanService(ClientWrapper):
    def __init__(self,
                 googleads_client: GoogleAdsClient,
                 customer_id: str,
                 location_codes: list[str] = None,
                 language_id: str | None = None
                 ):
        super().__init__(googleads_client=googleads_client,
                         customer_id=customer_id,
                         location_codes=location_codes,
                         language_id=language_id)

        self.keyword_plan_service = self.client.get_service("KeywordPlanService")
        self.keyword_plan_campaign_service = self.client.get_service("KeywordPlanCampaignService")
        self.keyword_plan_ad_group_service = self.client.get_service("KeywordPlanAdGroupService")
        self.keyword_plan_ad_group_keyword_service = self.client.get_service("KeywordPlanAdGroupKeywordService")

        self.default_network = self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
        self.default_keyword_plan_campaign_cpc_bid_micros = DEFAULT_KEYWORD_PLAN_CAMPAIGN_CPC_BID
        self.default_keyword_plan_ad_group_cpc_bid_micros = DEFAULT_KEYWORD_PLAN_AD_GROUP_CPC_BID

    def get_keyword_metrics(self,
                            keywords: list[str] | str,
                            location_codes: list[str] = None,
                            language_id: str = None,
                            print_progress: bool = False,
                            calculated_fields: bool = True,
                            concat_locations: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:

        if isinstance(keywords, str):
            keywords = [keywords]

        if location_codes is None:
            location_codes = self.location_codes

        keyword_list_partition = _partition_list(_list=keywords, n=2_000)
        frames: list[pd.DataFrame] = []
        keyword_plans = []

        for _sublist_index, _keyword_sublist in enumerate(keyword_list_partition):
            if _sublist_index > 0:
                time.sleep(API_MULTIPLE_REQUEST_WAIT_TIME)

            try:
                if concat_locations:
                    historical_metrics_df, _kwps = self.get_historical_metrics_df_concat_locations(
                        keywords=_keyword_sublist,
                        location_codes=location_codes,
                        language_id=language_id,
                        calculated_fields=calculated_fields
                    )
                else:
                    historical_metrics_df, _kwps = self.get_historical_metrics_df(
                        keywords=_keyword_sublist,
                        location_codes=location_codes,
                        language_id=language_id,
                        calculated_fields=calculated_fields
                    )

                if historical_metrics_df is not None:
                    keyword_plans.extend(_kwps)
                    frames.append(historical_metrics_df)
                    for _kwp in _kwps:
                        self.remove_keyword_plan(keyword_plan_resource_name=_kwp)

            except GoogleAdsException:
                print(f"GoogleAdsException encountered after obtaining {len(frames)} frames")
                break

            if print_progress:
                print(f"{len(frames)} - obtained metrics for {len(frames[-1])} keywords")

        if len(frames) > 0:
            _df = pd.concat(frames)
        else:
            return None, None

        metrics_df: pd.DataFrame = _df.drop(columns=['volume_trend_tuples'])

        monthly_volume_df: pd.DataFrame = _df[['country_iso_code',
                                               'date_obtained',
                                               'query',
                                               'keyword',
                                               'status',
                                               'volume_trend_tuples']].explode('volume_trend_tuples')

        monthly_volume_df['volume_trend_tuples'] = monthly_volume_df['volume_trend_tuples'].apply(
            _check_volume_trend_tuples)

        monthly_volume_df['month_name'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[3])
        monthly_volume_df['month_enum'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[2])
        monthly_volume_df['month'] = monthly_volume_df['month_enum'].apply(_get_month_from_enum_value)
        monthly_volume_df['year'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[1])

        monthly_volume_df.dropna(axis='index', subset=['month'], inplace=True)

        monthly_volume_df['record_date'] = monthly_volume_df.apply(
            lambda df: _conv_date(df['year'], df['month'], 15, month_index=0),
            axis=1
        )
        monthly_volume_df['volume'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[0])

        monthly_volume_df.drop(
            columns=['month_enum', 'volume_trend_tuples', 'month', 'year', 'month_name'],
            inplace=True
        )

        metrics_df.drop_duplicates(subset=["country_iso_code", "query", "keyword"], keep="first", inplace=True)
        monthly_volume_df.drop_duplicates(subset=["country_iso_code", "query", "keyword", "record_date"], keep="first", inplace=True)

        metrics_df.reset_index(drop=True, inplace=True)
        monthly_volume_df.reset_index(drop=True, inplace=True)

        _recent_dates = monthly_volume_df[
            ["country_iso_code", 'query', 'keyword', 'record_date']
        ].groupby(by=["country_iso_code", 'query', 'keyword']).max().reset_index()

        _competition_subset = pd.merge(
            left=_recent_dates,
            right=metrics_df[["country_iso_code", 'query', 'keyword', 'competition', 'competition_index']],
            on=["country_iso_code", 'query', 'keyword'],
            how='inner'
        )

        metrics_df = pd.merge(left=metrics_df, right=_recent_dates, on=["country_iso_code", 'query', 'keyword'], how='left')

        monthly_volume_df = pd.merge(
            left=monthly_volume_df,
            right=_competition_subset,
            on=["country_iso_code", 'query', 'keyword', 'record_date'],
            how='left'
        )

        return metrics_df, monthly_volume_df

    def get_forcast_metrics_df(self,
                               keywords: list[str],
                               location_codes: list[str] = None,
                               language_id: str = None) -> tuple[pd.DataFrame, str]:

        keyword_plan, stripped_keyword_dict = self.add_keyword_plan(keywords=keywords,
                                                                    match_type="EXACT",
                                                                    location_codes=location_codes,
                                                                    language_id=language_id)
        metrics = self.generate_forecast_metrics(keyword_plan_resource_name=keyword_plan)
        _df = pd.DataFrame(metrics)

        keywords_df = self.get_keyword_plan_ad_group_keywords(keyword_plan_resource_name=keyword_plan)

        _df["keyword_id"] = _df["keyword_plan_ad_group_keyword"].apply(lambda s: int(s.split('/')[-1]))
        keywords_df["query"] = keywords_df["keyword_text"]
        _df = pd.merge(_df, keywords_df, how="left", on="keyword_id")
        _df = _df[['date_obtained', 'query', 'impressions', 'clicks', 'ctr', 'average_cpc', 'cost_micros']]

        return _df, keyword_plan

    def _get_historical_metrics_df(self,
                                   keywords: list[str],
                                   location_codes: list[str] = None,
                                   language_id: str = None,
                                   calculated_fields: bool = True) -> tuple[pd.DataFrame, str]:
        keyword_plan, stripped_keyword_dict = self.add_keyword_plan(keywords=keywords,
                                                                    match_type="EXACT",
                                                                    location_codes=location_codes,
                                                                    language_id=language_id)
        metrics = self.generate_historical_metrics(keyword_plan_resource_name=keyword_plan,
                                                   stripped_keyword_dict=stripped_keyword_dict)
        # self.remove_keyword_plan(keyword_plan_resource_name=keyword_plan)
        _df = pd.DataFrame(metrics)
        if calculated_fields:
            _df["volume_trend_coef"] = _df["volume_trend"].apply(_three_month_trend_coef)
            _df["volume"] = _df["volume_trend"].apply(_latest_volume)
            _df["volume_last_month"] = _df["volume_trend"].apply(_previous_volume)
            _df["volume_last_year"] = _df["volume_trend"].apply(_last_year_volume)
            _df["volume_3monthavg"] = _df["volume_trend"].apply(_volume_3monthavg)
            _df["volume_6monthavg"] = _df["volume_trend"].apply(_volume_6monthavg)
            _df["volume_12monthavg"] = _df["volume_trend"].apply(_volume_12monthavg)
        return _df, keyword_plan


    def get_historical_metrics_df_concat_locations(self,
                                  keywords: list[str],
                                  location_codes: list[str] = None,
                                  language_id: str = None,
                                  calculated_fields: bool = True,
                                  max_retries: int = 5) -> tuple[pd.DataFrame, list[str]]:

        frames, keyword_plans = [], []

        for _loc_code in location_codes:
            _df, _keyword_plans = self.get_historical_metrics_df(
                keywords=keywords,
                location_codes=[_loc_code],
                language_id=language_id,
                calculated_fields=calculated_fields,
                max_retries=max_retries
            )
            if isinstance(_df, pd.DataFrame):
                frames.append(_df)
            keyword_plans.extend(_keyword_plans)

        if len(frames) > 0:
            dataframe = pd.concat(frames)
        else:
            dataframe = None

        return dataframe, keyword_plans

    def get_historical_metrics_df(self,
                                  keywords: list[str],
                                  location_codes: list[str] = None,
                                  language_id: str = None,
                                  calculated_fields: bool = True,
                                  max_retries: int = 5) -> tuple[pd.DataFrame, list[str]]:

        retries: int = 0
        complete: bool = False
        while retries < max_retries and complete is False:
            try:
                _df, _keyword_plan = self._get_historical_metrics_df(
                    keywords=keywords,
                    location_codes=location_codes,
                    language_id=language_id,
                    calculated_fields=calculated_fields
                )
                complete = True
            except InternalServerError:
                logging.warning("Keyword planner :: InternalServerError :: retrying after 2 seconds")
                time.sleep(2)
                retries += 1

        if complete:
            _plans = [_keyword_plan]
        else:
            _df = None
            _plans = []

        if _df is not None:
            _df.insert(loc=0, column='country_iso_code', value='-'.join(location_codes))

        return _df, _plans

    def generate_forecast_metrics(self,
                                  keyword_plan_resource_name: str):
        response_forcast = self.keyword_plan_service.generate_forecast_metrics(
            keyword_plan=keyword_plan_resource_name,
        )
        _date_today = datetime.date.today()
        metrics = [{"date_obtained": _date_today,
                    "keyword_plan_ad_group_keyword": _m.keyword_plan_ad_group_keyword,
                    "impressions": _m.keyword_forecast.impressions,
                    "clicks": _m.keyword_forecast.clicks,
                    "ctr": _m.keyword_forecast.ctr,
                    "average_cpc": _m.keyword_forecast.average_cpc,
                    "cost_micros": _m.keyword_forecast.cost_micros}
                   for _m in response_forcast.keyword_forecasts]

        return metrics

    def generate_historical_metrics(self,
                                    keyword_plan_resource_name: str,
                                    stripped_keyword_dict: dict):

        response_historical = self.keyword_plan_service.generate_historical_metrics(
            keyword_plan=keyword_plan_resource_name
        )

        _date_today = datetime.date.today()
        metrics = [{"date_obtained": _date_today,
                    "query": _m.search_query,
                    "avg_searches": _m.keyword_metrics.avg_monthly_searches,
                    "competition": _m.keyword_metrics.competition.name,
                    "competition_index": _m.keyword_metrics.competition_index,
                    "low_top_of_page_bid_micros": _m.keyword_metrics.low_top_of_page_bid_micros,
                    "high_top_of_page_bid_micros": _m.keyword_metrics.high_top_of_page_bid_micros,
                    "volume_trend": [v.monthly_searches for v in _m.keyword_metrics.monthly_search_volumes],
                    "volume_trend_tuples": [(v.monthly_searches, v.year, v.month.value, v.month.name)
                                            for v in _m.keyword_metrics.monthly_search_volumes]}
                   for _m in response_historical.metrics]

        empty_metric = {"date_obtained": _date_today,
                        "query": "",
                        "avg_searches": 0,
                        "competition": "",
                        "competition_index": None,
                        "low_top_of_page_bid_micros": 0,
                        "high_top_of_page_bid_micros": 0,
                        "volume_trend": [],
                        "volume_trend_tuples": []}

        metrics_dict = {d.get("query"): d for d in metrics}

        metrics = []
        for keyword, query in stripped_keyword_dict.items():
            d = metrics_dict.get(query)
            if d:
                # These are queries that were a result of stripping an input keyword (usually equal to the keyword)
                # and also found in the metrics returned by `generate_historical_metrics`.
                d["keyword"] = keyword
                d["status"] = "OBTAINED"
            else:
                # These are queries that were a result of stripping an input keyword (usually equal to the keyword)
                # but are NOT found in the metrics returned by `generate_historical_metrics`.
                d = empty_metric
                d["keyword"] = keyword
                d["query"] = query
                d["status"] = "NOT OBTAINED"
            metrics.append(d)

        for query, d in metrics_dict.items():
            if query not in stripped_keyword_dict.values():
                # These are queries that are found in the metrics returned by `generate_historical_metrics`
                # but were not in the stripped keywords. Keyword planner is stripping some of them further
                # based on its own rules that we don't quite understand.
                d["keyword"] = ""
                d["status"] = "ADDITIONAL"
                metrics.append(d)

        return metrics

    def add_keyword_plan(self,
                         keywords: list[str] | str,
                         match_type: str = "EXACT",
                         cpc_bid_micros: list[int] | int = 10000,
                         forecast_interval: str = "NEXT_QUARTER",
                         location_codes: list[str] = None,
                         language_id: str = None) -> tuple[str, dict]:
        """
        Adds a keyword plan, campaign, ad group, etc. to the customer account.

        Returns: keyword plan resource name
        """
        if location_codes is None:
            location_resource_names = self.location_rns
        else:
            location_resource_names = _map_location_to_resource_names(client=self.client,
                                                                      location_codes=location_codes)

        if language_id is None:
            language_rn = self.language_rn
        else:
            language_rn = _map_language_id_to_resource_name(client=self.client,
                                                            language_id=language_id)

        if isinstance(keywords, str):
            keywords = [keywords]

        # It is necessary to strip any punctuation and non-ascii chars from the keyword before adding to a keyword plan
        # This method therefore provides a stripped_keyword_dict which maps keywords to their "stripped" counterparts
        # this is many-to-one mapping since two multiple different keywords may be mapped to the same stripped
        # counterpart, e.g.:
        # stripped_keyword_dict = {"i love chocolate!!":  "i love chocolate",
        #                          "i-love-chocolate??": "i love chocolate"}
        stripped_keyword_dict = {kw: strip_illegal_chars(kw, language_id) for kw in keywords}
        keywords = [kw for kw in set(stripped_keyword_dict.values())
                    if 0 < len(kw) <= 70 and len(kw.split(' ')) <= 10]

        if isinstance(cpc_bid_micros, int):
            cpc_bid_micros = [cpc_bid_micros] * len(keywords)

        keyword_plan: str = self._create_keyword_plan(forecast_interval=forecast_interval)
        keyword_plan_campaign = self._create_keyword_plan_campaign(
            keyword_plan=keyword_plan,
            location_resources=location_resource_names,
            language_resource=language_rn
        )
        keyword_plan_ad_group = self._create_keyword_plan_ad_group(
            keyword_plan_campaign=keyword_plan_campaign
        )
        keyword_plan_keywords = self._create_keyword_plan_ad_group_keywords(
            keyword_plan_ad_group=keyword_plan_ad_group,
            keyword_texts=keywords,
            match_type=match_type,
            cpc_bid_micros=cpc_bid_micros
        )
        return keyword_plan, stripped_keyword_dict

    def _create_keyword_plan(self, forecast_interval: str = "NEXT_QUARTER") -> str:
        """Adds a keyword plan to the customer account.

        Returns:
            A str of the resource_name for the newly created keyword plan.
        """

        operation = self.client.get_type("KeywordPlanOperation")
        keyword_plan = operation.create

        keyword_plan.name = f"Keyword plan for traffic estimate {uuid.uuid4()}"

        if forecast_interval == "NEXT_QUARTER":
            forecast_interval_enum = (
                self.client.enums.KeywordPlanForecastIntervalEnum.NEXT_QUARTER
            )
        else:  # Defaults to NEXT_QUARTER
            forecast_interval_enum = (
                self.client.enums.KeywordPlanForecastIntervalEnum.NEXT_QUARTER
            )
        keyword_plan.forecast_period.date_interval = forecast_interval_enum

        response = self.keyword_plan_service.mutate_keyword_plans(
            customer_id=self.customer_id, operations=[operation]
        )
        resource_name = response.results[0].resource_name
        # print(f"Created keyword plan with resource name: {resource_name}")
        return resource_name

    def _create_keyword_plan_campaign(self,
                                      keyword_plan,
                                      location_resources,
                                      language_resource,
                                      network=None,
                                      cpc_bid_micros: int = None) -> str:
        """Adds a keyword plan campaign to the given keyword plan.

        Args:
            keyword_plan: A str of the keyword plan resource_name this keyword plan
                campaign should be attributed to.create_keyword_plan.

        Returns:
            A str of the resource_name for the newly created keyword plan campaign.
        """
        if network is None:
            network = self.default_network
        if cpc_bid_micros is None:
            cpc_bid_micros = self.default_keyword_plan_campaign_cpc_bid_micros

        operation = self.client.get_type("KeywordPlanCampaignOperation", version="v15")
        keyword_plan_campaign = operation.create

        keyword_plan_campaign.name = f"Keyword plan campaign {uuid.uuid4()}"
        keyword_plan_campaign.cpc_bid_micros = cpc_bid_micros
        keyword_plan_campaign.keyword_plan = keyword_plan
        keyword_plan_campaign.keyword_plan_network = network

        # Other geotarget constants can be referenced here:
        # https://developers.google.com/google-ads/api/reference/data/geotargets
        for _loc in location_resources:
            geo_target = self.client.get_type("KeywordPlanGeoTarget", version="v15")
            # print('geo_target type: ')
            # print(type(geo_target))
            geo_target.geo_target_constant = _loc
            keyword_plan_campaign.geo_targets.append(geo_target)

        keyword_plan_campaign.language_constants.append(language_resource)

        response = self.keyword_plan_campaign_service.mutate_keyword_plan_campaigns(
            customer_id=self.customer_id, operations=[operation]
        )
        resource_name = response.results[0].resource_name

        return resource_name

    def _create_keyword_plan_ad_group(self,
                                      keyword_plan_campaign: str,
                                      cpc_bid_micros: int = None) -> str:
        """Adds a keyword plan ad group to the given keyword plan campaign.

        Args:
            keyword_plan_campaign: A str of the keyword plan campaign resource_name
                this keyword plan ad group should be attributed to.

        Returns:
            A str of the resource_name for the newly created keyword plan ad group.
        """
        if cpc_bid_micros is None:
            cpc_bid_micros = self.default_keyword_plan_ad_group_cpc_bid_micros

        operation = self.client.get_type("KeywordPlanAdGroupOperation")
        keyword_plan_ad_group = operation.create

        keyword_plan_ad_group.name = f"Keyword plan ad group {uuid.uuid4()}"
        keyword_plan_ad_group.cpc_bid_micros = cpc_bid_micros
        keyword_plan_ad_group.keyword_plan_campaign = keyword_plan_campaign

        response = self.keyword_plan_ad_group_service.mutate_keyword_plan_ad_groups(
            customer_id=self.customer_id, operations=[operation]
        )
        resource_name = response.results[0].resource_name

        return resource_name

    def _create_keyword_plan_ad_group_keywords(self,
                                               keyword_plan_ad_group,
                                               match_type: str,
                                               keyword_texts: list[str],
                                               cpc_bid_micros: list[int]):
        """Adds keyword plan ad group keywords to the given keyword plan ad group.

        Args:
            keyword_plan_ad_group: A str of the keyword plan ad group resource_name
                these keyword plan keywords should be attributed to.

        Raises:
            GoogleAdsException: If an error is returned from the API.
        """

        operations = []

        for keyword_text, cpc_bid in zip(keyword_texts, cpc_bid_micros):
            operation = self.client.get_type("KeywordPlanAdGroupKeywordOperation")
            keyword_plan_ad_group_keyword = operation.create
            keyword_plan_ad_group_keyword.text = keyword_text
            keyword_plan_ad_group_keyword.cpc_bid_micros = cpc_bid
            if match_type == "EXACT":
                keyword_plan_ad_group_keyword.match_type = (
                    self.client.enums.KeywordMatchTypeEnum.EXACT
                )
            elif match_type == "BROAD":
                keyword_plan_ad_group_keyword.match_type = (
                    self.client.enums.KeywordMatchTypeEnum.BROAD
                )
            elif match_type == "PHRASE":
                keyword_plan_ad_group_keyword.match_type = (
                    self.client.enums.KeywordMatchTypeEnum.PHRASE
                )
            keyword_plan_ad_group_keyword.keyword_plan_ad_group = keyword_plan_ad_group

            operations.append(operation)

        response = self.keyword_plan_ad_group_keyword_service.mutate_keyword_plan_ad_group_keywords(
            customer_id=self.customer_id, operations=operations
        )

        # return [result.resource_name for result in response.results]
        return [result for result in response.results]


class KeywordPlanIdeaService(ClientWrapper):
    def __init__(self,
                 googleads_client: GoogleAdsClient,
                 customer_id: str,
                 site_url: str = None,
                 location_codes: list[str] = None,
                 language_id: str | None = None
                 ):
        super().__init__(googleads_client=googleads_client,
                         customer_id=customer_id,
                         location_codes=location_codes,
                         language_id=language_id)

        self.site_url = site_url

        self.keyword_plan_idea_service = self.client.get_service(
            "KeywordPlanIdeaService")
        self.keyword_competition_level_enum = (
            self.client.enums.KeywordPlanCompetitionLevelEnum)
        self.keyword_plan_network = (
            self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS
        )

    def generate_keyword_ideas(self,
                               url: str | None = None,
                               phrases: str | list[str] | None = None,
                               include_adult_keywords: bool = True,
                               location_codes: list[str] = None,
                               language_id: str = None) -> pd.DataFrame:
        """
        Use the Google Ads Keyword Planner API to generate keyword ideas based on a URL or a phrase
        """

        if location_codes is None:
            location_resource_names = self.location_rns
        else:
            location_resource_names = _map_location_to_resource_names(client=self.client,
                                                                      location_codes=location_codes)

        if language_id is None:
            language_rn = self.language_rn
        else:
            language_rn = _map_language_id_to_resource_name(client=self.client,
                                                            language_id=language_id)

        request = self.client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = self.customer_id
        request.language = language_rn
        request.geo_target_constants = location_resource_names
        request.include_adult_keywords = include_adult_keywords
        request.keyword_plan_network = self.keyword_plan_network
        # request.page_size = page_size

        if url is not None:
            if RE_URL.match(url):
                page_url = url
            elif url == "/" or url == "":
                page_url = self.site_url
            elif url[0] == "/":
                page_url = self.site_url + url[1:]
            else:
                page_url = self.site_url + url
        else:
            page_url = None

        # To generate keyword ideas with only a page_url and no keywords we need
        # to initialize a UrlSeed object with the page_url as the "url" field.
        if not phrases and page_url:
            request.url_seed.url = page_url

        # To generate keyword ideas with only a list of keywords and no page_url
        # we need to initialize a KeywordSeed object and set the "keywords" field
        # to be a list of StringValue objects.
        if phrases and not page_url:
            request.keyword_seed.keywords.extend(phrases)

        # To generate keyword ideas using both a list of keywords and a page_url we
        # need to initialize a KeywordAndUrlSeed object, setting both the "url" and
        # "keywords" fields.
        if phrases and page_url:
            request.keyword_and_url_seed.url = page_url
            request.keyword_and_url_seed.keywords.extend(phrases)

        if not phrases and not page_url:
            raise ValueError("must supply either `phrases` or `url`")

        keyword_ideas_pager = self.keyword_plan_idea_service.generate_keyword_ideas(request=request)

        # keyword_ideas_list = [idea for idea in keyword_ideas_pager]

        _date_today = datetime.date.today()
        keyword_ideas = [{"date_obtained": _date_today,
                          "phrase": idea.text,
                          "avg_searches": idea.keyword_idea_metrics.avg_monthly_searches,
                          "competition": idea.keyword_idea_metrics.competition.name,
                          "competition_index": idea.keyword_idea_metrics.competition_index,
                          "low_top_of_page_bid_micros": idea.keyword_idea_metrics.low_top_of_page_bid_micros,
                          "high_top_of_page_bid_micros": idea.keyword_idea_metrics.high_top_of_page_bid_micros,
                          "volume_trend": [v.monthly_searches
                                           for v in idea.keyword_idea_metrics.monthly_search_volumes]
                          }
                         for idea in keyword_ideas_pager]

        _df = pd.DataFrame(keyword_ideas)
        _df["volume_trend_coef"] = _df["volume_trend"].apply(_three_month_trend_coef)
        _df["latest_volume"] = _df["volume_trend"].apply(_latest_volume)

        return _df

    def historical_metrics_generator(self,
                                     keywords: str | list[str] | None = None,
                                     include_adult_keywords: bool = True,
                                     location_codes: list[str] = None,
                                     language_id: str = None,
                                     calculated_fields: bool = True,
                                     concat_locations: bool = True,
                                     print_progress: bool = False):

        pass


    def generate_historical_metrics(self,
                                   keywords: str | list[str] | None = None,
                                   include_adult_keywords: bool = True,
                                   location_codes: list[str] = None,
                                   language_id: str = None,
                                    calculated_fields: bool = True,
                                    concat_locations: bool = True,
                                    print_progress: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Use the Google Ads Keyword Planner API to generate keyword ideas based on a URL or a phrase
        """
        if isinstance(keywords, str):
            keywords = [keywords]

        if language_id is None:
            language_rn = self.language_rn
        else:
            language_rn = _map_language_id_to_resource_name(client=self.client,
                                                            language_id=language_id)

        keyword_list_partition = _partition_list(_list=keywords, n=2_000)
        frames: list[pd.DataFrame] = []

        for _sublist_index, _keyword_sublist in enumerate(keyword_list_partition):
            if _sublist_index > 0:
                time.sleep(API_MULTIPLE_REQUEST_WAIT_TIME)

            try:
                historical_metrics_df = self._historical_metrics_dataframe(
                    keywords=_keyword_sublist,
                    location_codes=location_codes,
                    language_resource_name=language_rn,
                    include_adult_keywords=include_adult_keywords,
                    calculated_fields=calculated_fields,
                    concat_locations=concat_locations
                )

                if historical_metrics_df is not None:
                    frames.append(historical_metrics_df)

            except GoogleAdsException:
                print(f"GoogleAdsException encountered after obtaining {len(frames)} frames")
                break

            if print_progress:
                print(f"{len(frames)} - obtained metrics for {len(frames[-1])} keywords")

        if len(frames) > 0:
            _df = pd.concat(frames)
        else:
            return None, None

        metrics_df: pd.DataFrame = _df.drop(columns=['volume_trend_tuples'])

        monthly_volume_df: pd.DataFrame = _df[['country_iso_code',
                                               'date_obtained',
                                               'query',
                                               'keyword',
                                               'status',
                                               'volume_trend_tuples']].explode('volume_trend_tuples')

        monthly_volume_df['volume_trend_tuples'] = monthly_volume_df['volume_trend_tuples'].apply(
            _check_volume_trend_tuples)

        monthly_volume_df['month_name'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[3])
        monthly_volume_df['month_enum'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[2])
        monthly_volume_df['month'] = monthly_volume_df['month_enum'].apply(_get_month_from_enum_value)
        monthly_volume_df['year'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[1])

        monthly_volume_df.dropna(axis='index', subset=['month'], inplace=True)

        monthly_volume_df['record_date'] = monthly_volume_df.apply(
            lambda _mvdf: _conv_date(_mvdf['year'], _mvdf['month'], 15, month_index=0),
            axis=1
        )
        monthly_volume_df['volume'] = monthly_volume_df['volume_trend_tuples'].apply(lambda t: t[0])

        monthly_volume_df.drop(
            columns=['month_enum', 'volume_trend_tuples', 'month', 'year', 'month_name'],
            inplace=True
        )
        monthly_volume_df.fillna(value={'competition': "UNSPECIFIED", 'competition_index': 0.0}, inplace=True)

            # monthly_volume_df = pd.DataFrame(
            #     columns=[
            #         "country_iso_code",
            #         "date_obtained",
            #         "query",
            #         "keyword",
            #         "status",
            #         "record_date",
            #         "volume",
            #         "competition",
            #         "competition_index"
            #     ]
            # )

        metrics_df.drop_duplicates(subset=["country_iso_code", "query", "keyword"], keep="first", inplace=True)
        monthly_volume_df.drop_duplicates(subset=["country_iso_code", "query", "keyword", "record_date"], keep="first", inplace=True)

        metrics_df.reset_index(drop=True, inplace=True)
        monthly_volume_df.reset_index(drop=True, inplace=True)

        _recent_dates = monthly_volume_df[
            ["country_iso_code", 'query', 'keyword', 'record_date']
        ].groupby(by=["country_iso_code", 'query', 'keyword']).max().reset_index()

        _competition_subset = pd.merge(
            left=_recent_dates,
            right=metrics_df[["country_iso_code", 'query', 'keyword', 'competition', 'competition_index']],
            on=["country_iso_code", 'query', 'keyword'],
            how='inner'
        )

        metrics_df = pd.merge(left=metrics_df, right=_recent_dates, on=["country_iso_code", 'query', 'keyword'], how='left')

        monthly_volume_df = pd.merge(
            left=monthly_volume_df,
            right=_competition_subset,
            on=["country_iso_code", 'query', 'keyword', 'record_date'],
            how='left'
        )

        return metrics_df, monthly_volume_df


    def _historical_metrics_response(self,
                                  keywords: list[str],
                                  location_resource_names: list,
                                  language_resource_name: str,
                                  include_adult_keywords: bool = True):
        request = self.client.get_type("GenerateKeywordHistoricalMetricsRequest")
        request.customer_id = self.customer_id
        request.language = language_resource_name
        request.geo_target_constants = location_resource_names
        request.include_adult_keywords = include_adult_keywords
        request.keyword_plan_network = self.keyword_plan_network
        request.keywords = keywords

        response = None
        while response is None:
            try:
                response = self.keyword_plan_idea_service.generate_keyword_historical_metrics(
                    request=request
                )
            except ResourceExhausted as _error:
                response = None
                if m := re.search(r'Retry in (\d+) seconds', _error.__str__()):
                    wait_time = int(m.group(1)) + 1
                else:
                    wait_time = 5

                print(f"ResourceExhausted encountered :: retrying after {wait_time} seconds")

                time.sleep(wait_time)

        return response


    def _historical_metrics_dataframe(self,
                                  keywords: list[str],
                                  location_codes: list,
                                  language_resource_name: str,
                                  include_adult_keywords: bool = True,
                                      calculated_fields: bool = True,
                                      concat_locations: bool = True):

        if location_codes is None:
            location_codes = self.location_codes
            location_resource_names = self.location_rns
        else:
            location_resource_names = _map_location_to_resource_names(client=self.client,
                                                                      location_codes=location_codes)

        if concat_locations:
            return self._historical_metrics_dataframe_concat_locations(
                keywords=keywords,
                location_codes=location_codes,
                language_resource_name=language_resource_name,
                include_adult_keywords=include_adult_keywords,
                calculated_fields=calculated_fields
            )

        response = self._historical_metrics_response(
            keywords=keywords,
            location_resource_names=location_resource_names,
            language_resource_name=language_resource_name,
            include_adult_keywords=include_adult_keywords
        )

        _date_today = datetime.date.today()

        metrics = []
        for _m in response.results:
            _query= _m.text
            _vars = _m.close_variants
            if not _vars:
                _vars = []
            _keywords = [_query] + list(_vars)

            for _k in _keywords:

                _metric = {
                    "date_obtained": _date_today,
                    "country_iso_code": '-'.join(location_codes),
                    "query": _query,
                    "keyword": _k,
                    "status": "OBTAINED",
                    "avg_searches": _m.keyword_metrics.avg_monthly_searches,
                    "competition": _m.keyword_metrics.competition.name,
                    "competition_index": _m.keyword_metrics.competition_index,
                    "low_top_of_page_bid_micros": _m.keyword_metrics.low_top_of_page_bid_micros,
                    "high_top_of_page_bid_micros": _m.keyword_metrics.high_top_of_page_bid_micros,
                    "volume_trend": [v.monthly_searches for v in _m.keyword_metrics.monthly_search_volumes],
                    "volume_trend_tuples": [(v.monthly_searches, v.year, v.month.value, v.month.name)
                                                for v in _m.keyword_metrics.monthly_search_volumes]
                }

                if not _metric["volume_trend"]:
                    _metric["status"] = "NO_VOLUME_DATA"
                    _metric["volume_trend"] = [0]*12
                    _metric["volume_trend_tuples"] = _empty_volume_trend_tuples()

                metrics.append(_metric)

        empty_metric = {
            "date_obtained": _date_today,
            "country_iso_code": '-'.join(location_codes),
            "query": "",
            "status": "NOT_OBTAINED",
            "avg_searches": 0,
            "competition": "",
            "competition_index": None,
            "low_top_of_page_bid_micros": 0,
            "high_top_of_page_bid_micros": 0,
            "volume_trend": [0]*12,
            "volume_trend_tuples": _empty_volume_trend_tuples()
        }

        for _missing_keyword in set(keywords) - set(m.get("keyword") for m in metrics):
            _metric = empty_metric.copy()
            _metric["keyword"] = _missing_keyword
            metrics.append(_metric)

        if len(metrics) == 0:
            df = pd.DataFrame(
                columns=[
                    "date_obtained",
                    "country_iso_code",
                    "query",
                    "keyword",
                    "status",
                    "avg_searches",
                    "competition",
                    "competition_index",
                    "low_top_of_page_bid_micros",
                    "high_top_of_page_bid_micros",
                    "volume_trend",
                    "volume_trend_tuples"
                ]
            )
        else:
            df = pd.DataFrame(metrics)

        if calculated_fields:
            df["volume_trend_coef"] = df["volume_trend"].apply(_three_month_trend_coef)
            df["volume"] = df["volume_trend"].apply(_latest_volume)
            df["volume_last_month"] = df["volume_trend"].apply(_previous_volume)
            df["volume_last_year"] = df["volume_trend"].apply(_last_year_volume)
            df["volume_3monthavg"] = df["volume_trend"].apply(_volume_3monthavg)
            df["volume_6monthavg"] = df["volume_trend"].apply(_volume_6monthavg)
            df["volume_12monthavg"] = df["volume_trend"].apply(_volume_12monthavg)

        return df

    def _historical_metrics_dataframe_concat_locations(self,
                                                      keywords: list[str],
                                                      location_codes: list[str],
                                                      language_resource_name: str,
                                                      include_adult_keywords: bool = True,
                                                       calculated_fields: bool = True):
        frames = []
        for _loc in location_codes:
            _df = self._historical_metrics_dataframe(
                keywords=keywords,
                location_codes=[_loc],
                language_resource_name=language_resource_name,
                include_adult_keywords=include_adult_keywords,
                calculated_fields=calculated_fields,
                concat_locations=False
            )
            if len(_df) > 0:
                frames.append(_df)

        if len(frames) > 0:
            df = pd.concat(frames)
        else:
            df = pd.DataFrame(
                columns=[
                    "date_obtained",
                    "country_iso_code",
                    "query",
                    "keyword",
                    "status",
                    "avg_searches",
                    "competition",
                    "competition_index",
                    "low_top_of_page_bid_micros",
                    "high_top_of_page_bid_micros",
                    "volume_trend",
                    "volume_trend_tuples"
                ]
            )

        return df



def _three_month_trend_coef(volume_trends: list[int]):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) == 0:
        return None
    y = np.array(volume_trends)
    x = np.arange(len(y))
    p = np.polyfit(x[-3:], y[-3:], deg=1)
    return round(p[0] / y.mean(), 3)


def _latest_volume(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) == 0:
        return None
    return volume_trends[-1]


def _previous_volume(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) == 0:
        return None
    return volume_trends[-2]


def _volume_mom(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) < 2:
        return None
    return volume_trends[-1] - volume_trends[-2]


def _last_year_volume(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) < 12:
        return None
    return volume_trends[-12]


def _volume_yoy(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) < 12:
        return None
    return volume_trends[-1] - volume_trends[-12]


def _volume_3monthavg(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) == 0:
        return None
    return np.mean(volume_trends[-3:])


def _volume_6monthavg(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) == 0:
        return None
    return np.mean(volume_trends[-6:])


def _volume_12monthavg(volume_trends):
    if not isinstance(volume_trends, list):
        return None
    if len(volume_trends) == 0:
        return None
    return np.mean(volume_trends[-12:])


def _partition_list(_list, n):
    return [_list[n * i:n * i + n] for i in range(ceil(len(_list) / n))]


def _conv_date(year, month, day, month_index: int = 0):
    if isinstance(year, float):
        year = int(year)
    if not isinstance(year, int):
        return None
    if isinstance(month, float):
        month = int(month)
    if not isinstance(month, int):
        return None
    if month_index == 0:
        month += 1
    return datetime.date(year, month, day)


def _map_location_to_resource_names(client,
                                    location_codes: str | list[str]):
    if isinstance(location_codes, str):
        location_codes = [location_codes]

    location_ids = []
    for _code in location_codes:
        _code = str(_code)
        if re.match("[A-Z]{3}", _code):
            _id = LOCATION_ID_DICT.get(_code, None)
            if _id:
                location_ids.append(_id)
        elif re.match("[0-9]+", _code):
            location_ids.append(_code)

    build_resource_name = client.get_service("GeoTargetConstantService").geo_target_constant_path
    return [build_resource_name(location_id) for location_id in location_ids]


def _map_locations_ids_to_resource_names(client, location_ids):
    """Converts a list of location IDs to resource names.

    Args:
        client: an initialized GoogleAdsClient instance.
        location_ids: a list of location ID strings.

    Returns:
        a list of resource name strings using the given location IDs.
    """
    build_resource_name = client.get_service("GeoTargetConstantService").geo_target_constant_path
    return [build_resource_name(location_id) for location_id in location_ids]


def _map_language_id_to_resource_name(client, language_id):
    return client.get_service("GoogleAdsService").language_constant_path(criterion_id=language_id)


def _check_volume_trend_tuples(t):
    if isinstance(t, tuple) and len(t) == 4:
        return t
    else:
        return 0, 0, 0, ''


def _get_month_from_enum_value(month_enum_value):
    if month_enum_value <= 1:
        return None
    else:
        return month_enum_value - 2


def _add_months(dt: datetime.date, months: int) -> datetime.date:
    m = dt.month - 1 + months
    return datetime.date(
        year=dt.year + m // 12,
        month=m % 12 + 1,
        day=dt.day)


def _empty_volume_trend_tuples(n: int = 12):
    _last_month = _add_months(datetime.date.today(), -1)
    months = [_add_months(datetime.date(_last_month.year, _last_month.month, 15), -i) for i in range(n)]
    return [(0.0,
             d.year,
             d.month + 1,
             d.strftime('%B').upper()
             ) for d in months]


def strip_illegal_chars(s, language_id):
    # lower case obvs
    s = s.lower()
    # replace `‘’ with simple '
    s = re.sub(r"[`‘’]+", "'", s)
    # first remove any non-ascii characters, replace with a space
    if language_id in ['1001']:  # when german language_id don't replace one of [Ä, ä, Ö, ö, Ü, ü, ß]
        s = re.sub(r"[^\x00-\x7F\xC4\xE4\xD6\xF6\xDC\xFC\xDF]+", " ", s)
    else:
        s = re.sub(r"[^\x00-\x7F]+", " ", s)
    # then replace any punctuation with a space
    s = re.sub(r"""[!¡⁄@%^()={}:;,.+\-–—_±~“"#<>?/|*¶•ªº&\[\]\\]+""", " ", s)
    # finally, replace multiple spaces (r"\s+") with a single space
    s = re.sub(r"\s+", " ", s)
    return s.strip()
