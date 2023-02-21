import logging
import re
import csv
import os
import numpy as np
import pandas as pd
import datetime

from typing import List, Optional, Union, Pattern

from . import utils

gpd_logger = logging.getLogger("googlepandas")


def camel_to_snake(string: str):
    return utils.RE_C2S.sub('_', string).lower()


GA_Types = {
    'ga:date': str,
    'ga:dateHourMinute': str,
    'ga:landingPagePath': str,
    'ga:pagePath': str,
    'ga:productName': str,
    'ga:productSku': str,
    'ga:productListName': str,
    'ga:productListPosition': str,
    'ga:source': str,
    'ga:medium': str,
    'ga:sourceMedium': str,
    'ga:channelGrouping': str,
    'ga:referralPath': str,
    'ga:campaign': str,
    'ga:keyword': str,
    'ga:deviceCategory': str,
    'ga:countryIsoCode': str,
    'ga:itemRevenue': float,
    'ga:itemQuantity': float,
    'ga:users': int,
    'ga:timeOnPage': float,
    'ga:newUsers': int,
    'ga:sessions': int,
    'ga:sessionDuration': float,
    'ga:transactionsPerSession': float,  # i.e. conversion_rate
    'ga:bounceRate': float,
    'ga:transactions': float,
    'ga:transactionRevenue': float,
    'ga:pageViews': float,
    'ga:pageviewsPerSession': float,
    'ga:avgTimeOnPage': float,
    'ga:avgPageLoadTime': float,
    'ga:exits': int,
    'ga:productDetailViews': float,
    'ga:productListCTR': float,
    'ga:productListClicks': float,
    'ga:productListViews': float
}


def from_response(response: dict,
                  response_type: str = None,
                  report_index: int = 0,
                  gsc_dimensions: Optional[List[str]] = None,
                  start_date: datetime.date = None,
                  end_date: datetime.date = None):
    # We're going to what kind of response this is by making assumptions about what the responses will look like.
    # If the response dictionary includes a 'report' key, we assume it is a GA response.
    # If the response dictionary includes a 'rows' key with returns
    if response_type is None:
        response_type = get_response_type(response)

    if response_type == 'GA3':
        rows = response.get('reports', [])[report_index]['data']['rows']
        dimensions = response.get('reports')[report_index].get('columnHeader').get('dimensions')
        _metric_header_entries = response.get('reports')[report_index] \
            .get('columnHeader') \
            .get('metricHeader') \
            .get('metricHeaderEntries')
        metrics = [_d['name'] for _d in _metric_header_entries]
        gpd_logger.debug(f"from_response: creating GADataFrame. dimensions = [{dimensions}], metrics = [{metrics}]")
        return GADataFrame(df_input=rows,
                           dimensions=dimensions, metrics=metrics,
                           start_date=start_date, end_date=end_date,
                           from_ga_response=True)
    elif response_type == 'GSC':
        rows = response.get('rows', [])
        response_aggregation = response.get('responseAggregationType', None)
        gpd_logger.debug("from_response: creating GSCDataFrame")
        _gsc_df = GSCDataFrame(df_input=rows,
                               gsc_dimensions=gsc_dimensions,
                               from_gsc_response=True)
        _gsc_df.response_aggregation = response_aggregation
        return _gsc_df


def from_csv(csv_file_path):
    pandas_df = pd.read_csv(csv_file_path)
    r_type = response_type_from_file(csv_file_path)
    if r_type == 'GA':
        gpd_logger.debug("creating GADataFrame from csv")
        return GADataFrame(df_input=pandas_df,
                           from_ga_response=False)
    if r_type == 'GSC' or r_type == 'GSCQ':
        gpd_logger.debug("creating GSCDataFrame from csv")
        return GSCDataFrame(df_input=pandas_df,
                            from_gsc_response=False)


class GADataFrame(pd.DataFrame):
    """
    Subclass of pandas.DataFrame specifically for use with Google Analytics responses.
    The subclass must be initialized with a GA response object and will create a dataframe
    from the report indexed by `report_index` (default 0).

    The DataFrame column headings are the same as the GA dimensions and metrics but without the 'ga:' prefix

    In addition to pandas DataFrame functionality, the GADataFrame has additional metadata attributes:
        dimensions:      list of the GA dimensions returned in the response
        metrics:         list of the GA metrics returned in the response
        brand_regex_dict:     dictionary of regex patterns supplied to __init__()
        category_regex_dict: dictionary of regex patterns supplied to __init__()
    """

    _metadata = ["dimensions",
                 "metrics",
                 "join_dimensions",
                 "date_range",
                 "date_range_days"]

    def __init__(self, df_input,
                 dimensions: Optional[List[str]] = None,
                 metrics: Optional[List[str]] = None,
                 from_ga_response: bool = False,
                 join_dimensions: List[str] = None,
                 start_date: datetime.date = None,
                 end_date: datetime.date = None
                 ):

        self.dimensions = dimensions
        self.metrics = metrics
        self.join_dimensions = join_dimensions
        self.date_range = {'start': start_date, 'end': end_date}
        if start_date and end_date:
            self.date_range_days = (end_date - start_date).days + 1
        else:
            self.date_range_days = None

        if df_input is None:
            join_dimensions = [strip_ga_prefix(_) for _ in self.dimensions]
            super().__init__(None, columns=[strip_ga_prefix(_) for _ in dimensions + metrics])
            if 'landingPagePath' in self.columns:
                # rename 'landingPagePath' to 'landingPageFull', then add dummy columns for
                # 'landingPage' and 'landingPageParameter'
                self.rename(columns={'landingPagePath': 'landingPageFull'}, inplace=True)
                self['landingPage'] = None
                self['landingPageParameter'] = None
                join_dimensions = remove_list_item(join_dimensions, 'landingPagePath')
                join_dimensions.extend(['landingPage', 'landingPageFull', 'landingPageParameter'])

            if 'deviceCategory' in self.columns:
                self.rename(columns={'deviceCategory': 'device'}, inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'deviceCategory')
                join_dimensions.extend(['device'])

            if 'sourceMedium' in self.columns:
                self['source'] = None
                self['medium'] = None
                self.drop(columns='sourceMedium', inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'sourceMedium')
                join_dimensions.extend(['source', 'medium'])

            if 'transactionsPerSession' in self.columns:
                self.rename(columns={'transactionsPerSession': 'conversionRate'}, inplace=True)

            if 'date' in self.columns:
                self.rename(columns={'date': 'recordDate'}, inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'date')
                join_dimensions.extend(['recordDate'])

            if 'dateHourMinute' in self.columns:
                self['recordDate'] = None
                self['recordTime'] = None
                self.drop(columns='dateHourMinute', inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'dateHourMinute')
                join_dimensions.extend(['recordDate', 'recordTime'])

            self.join_dimensions = [camel_to_snake(_) for _ in join_dimensions]

        else:
            super().__init__(df_input)

        if from_ga_response:
            for _i in range(len(self.metrics) - 1, -1, -1):
                self.insert(loc=0, column=strip_ga_prefix(self.metrics[_i]),
                            value=self['metrics'].apply(lambda x: x[0].get('values')[_i]
                                                        ).astype(GA_Types.get(self.metrics[_i], str))
                            )
            self.drop(columns='metrics', inplace=True)

            for _i in range(len(self.dimensions) - 1, -1, -1):
                self.insert(loc=0, column=strip_ga_prefix(self.dimensions[_i]),
                            value=self['dimensions'].apply(lambda x: x[_i]
                                                           ).astype(GA_Types.get(self.dimensions[_i], str))
                            )
            self.drop(columns='dimensions', inplace=True)
            join_dimensions = [strip_ga_prefix(_) for _ in self.dimensions]

            if 'productName' in self.columns:
                self['productName'] = self['productName'].apply(lambda s: s.strip().lower()
                                                                .replace('-', ' ')
                                                                .replace('_', ' ')
                                                                .replace('&amp;', '&')
                                                                )
                self['productName'] = self['productName'].apply(lambda s: re.sub(r"\\u[a-f\d]{4}", " ", s))
                self['productName'] = self['productName'].apply(lambda s: re.sub(r"\s+", " ", s).strip())

            if 'landingPagePath' in self.columns:
                self['landingPage'] = self['landingPagePath'].apply(utils.strip_url)
                self['landingPageParameter'] = self['landingPagePath'].apply(utils.url_extract_parameter)
                self.rename(columns={'landingPagePath': 'landingPageFull'}, inplace=True)

                join_dimensions = remove_list_item(join_dimensions, 'landingPagePath')
                join_dimensions.extend(['landingPageFull', 'landingPageParameter', 'landingPage'])

            if 'transactionsPerSession' in self.columns:
                self.rename(columns={'transactionsPerSession': 'conversionRate'}, inplace=True)

            if 'deviceCategory' in self.columns:
                self.rename(columns={'deviceCategory': 'device'}, inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'deviceCategory')
                join_dimensions.extend(['device'])

            if 'sourceMedium' in self.columns:
                self['source'] = self.sourceMedium.apply(lambda s: s.split('/')[0].strip())
                self['medium'] = self.sourceMedium.apply(lambda s: s.split('/')[1].strip() if '/' in s else None)
                self.drop(columns='sourceMedium', inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'sourceMedium')
                join_dimensions.extend(['source', 'medium'])

            if 'date' in self.columns:
                self.drop(
                    self[self.date.apply(lambda _date: True if _date == '(other)' else False)].index,
                    inplace=True
                )
                self.date = pd.to_datetime(self.date, format="%Y%m%d")
                self.date = self.date.apply(lambda date_time: datetime.datetime.date(date_time))
                self.rename(columns={'date': 'recordDate'}, inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'date')
                join_dimensions.extend(['recordDate'])

            if 'dateHourMinute' in self.columns:
                self.drop(
                    self[self.dateHourMinute.apply(lambda _datetime: True if _datetime == '(other)' else False)].index,
                    inplace=True
                )
                date_time_series = pd.to_datetime(self.dateHourMinute, format="%Y%m%d%H%M")
                self['recordDate'] = date_time_series.apply(lambda date_time: datetime.datetime.date(date_time))
                self['recordTime'] = date_time_series.apply(lambda date_time: date_time.time())
                self.drop(columns='dateHourMinute', inplace=True)
                join_dimensions = remove_list_item(join_dimensions, 'dateHourMinute')
                join_dimensions.extend(['recordDate', 'recordTime'])

            if 'countryIsoCode' in self.columns:
                self.countryIsoCode = self.countryIsoCode.apply(iso_code_2_to_3)

            self.join_dimensions = [camel_to_snake(_) for _ in join_dimensions]

        self.snake_case_columns()

        if "record_date" not in self.columns:
            if start_date and end_date:
                self.insert(loc=0, column="record_date", value=end_date)
                self.insert(loc=0, column="record_date_start", value=start_date)
                self.join_dimensions.append("record_date")
                self.join_dimensions.append("record_date_start")
            elif end_date:
                self.insert(loc=0, column="record_date", value=end_date)
                self.join_dimensions.append("record_date")
            elif start_date:
                self.insert(loc=0, column="record_date", value=start_date)
                self.join_dimensions.append("record_date")

    def snake_case_columns(self):
        self.rename(columns={old_col_name: camel_to_snake(old_col_name)
                             for old_col_name in self.columns},
                    inplace=True)

    def add_brand_column(self,
                         brand_function: Optional[str],
                         brands_dict: Optional[dict] = None):
        if 'product_name' in self.columns:
            _loc = self.columns.get_loc('product_name') + 1
            if brand_function == "first word":
                self.insert(loc=_loc, column='brand',
                            value=self.product_name.apply(lambda s: s.split(' ')[0]))
            elif brand_function == "regex":
                def regex_brand_function_local(_product_name):
                    return regex_brand_function(product_name=_product_name, brands_dict=brands_dict)

                self.insert(loc=_loc, column='brand',
                            value=self.product_name.apply(regex_brand_function_local))
            else:
                self.insert(loc=_loc, column='brand',
                            value='')

    def add_category_column(self, category_function: Optional[str], categories_dict: Optional[dict] = None):
        if 'product_name' in self.columns:
            _loc = self.columns.get_loc('product_name') + 1
            if category_function == "first word":
                self.insert(loc=_loc, column='category',
                            value=self.product_name.apply(lambda s: s.split(' ')[0]))
                self.insert(loc=_loc + 1, column='subcategory', value='')
            elif category_function == "regex" and categories_dict is not None:

                def regex_category_function_local(_product_name):
                    return regex_category_function(product_name=_product_name, categories_dict=categories_dict)

                category_df = self[['product_name']].apply(regex_category_function_local,
                                                           axis=1, result_type='expand')
                self.insert(loc=_loc, column='category', value=category_df[0])
                self.insert(loc=_loc + 1, column='subcategory', value=category_df[1])
            elif category_function == "separate_regex" and categories_dict is not None:

                def separate_regex_category_function1_local(_product_name):
                    return separate_regex_category_function1(product_name=_product_name,
                                                             categories_dict=categories_dict)

                def separate_regex_category_function2_local(_product_name):
                    return separate_regex_category_function2(product_name=_product_name,
                                                             categories_dict=categories_dict)

                self.insert(loc=_loc, column='category',
                            value=self['product_name'].apply(separate_regex_category_function1_local))
                self.insert(loc=_loc + 1, column='subcategory',
                            value=self['product_name'].apply(separate_regex_category_function2_local))
            else:
                self.insert(loc=_loc, column='category', value='')
                self.insert(loc=_loc + 1, column='subcategory', value='')

    def add_google_organic_column(self):
        if 'source' in self.columns and 'medium' in self.columns:
            _loc = self.columns.get_loc('medium') + 1
            if len(self) == 0:
                self['is_google_organic'] = None
            else:
                self.insert(loc=_loc,
                            column='is_google_organic',
                            value=self.apply(lambda _df: is_google_organic(source=_df['source'],
                                                                           medium=_df['medium']),
                                             axis=1)
                            )
        else:
            pass

    def add_new_user_column(self):
        if 'user_type' in self.columns:
            _loc = self.columns.get_loc('user_type') + 1
            if len(self) == 0:
                self['is_new_user'] = None
            else:
                self.insert(loc=_loc,
                            column='is_new_user',
                            value=self.apply(lambda _df: is_new_user(user_type=_df['user_type']),
                                             axis=1)
                            )
        else:
            pass

    def add_shopping_stage_all_column(self):
        if 'shopping_stage' in self.columns:
            _loc = self.columns.get_loc('shopping_stage') + 1
            if len(self) == 0:
                self['shopping_stage_all'] = None
            else:
                self.insert(loc=_loc,
                            column='shopping_stage_all',
                            value=self.apply(lambda _df: is_shopping_stage_all(shopping_stage=_df['shopping_stage']),
                                             axis=1)
                            )
        else:
            pass

    def add_has_site_search_column(self):
        if 'search_used' in self.columns:
            _loc = self.columns.get_loc('search_used') + 1
            if len(self) == 0:
                self['has_site_search'] = None
            else:
                self.insert(loc=_loc,
                            column='has_site_search',
                            value=self.apply(lambda _df: has_site_search(search_used=_df['search_used']),
                                             axis=1)
                            )
        else:
            pass

    def add_has_item_column(self):
        if 'item_quantity' in self.columns:
            _loc = self.columns.get_loc('item_quantity') + 1
            if len(self) == 0:
                self['has_item'] = None
            else:
                self.insert(loc=_loc,
                            column='has_item',
                            value=self.item_quantity.apply(lambda quantity: False if quantity == 0 else True)
                            )
        else:
            pass

    def add_landing_page_subdomains(self):
        if 'landing_page' in self.columns:
            _loc = self.columns.get_loc('landing_page') + 1
            self.insert(loc=_loc,
                        column='lp_sub1',
                        value=self.landing_page.apply(get_sub1)
                        )
            self.insert(loc=_loc + 1,
                        column='lp_sub2',
                        value=self.landing_page.apply(get_sub2)
                        )
        else:
            pass

    def filter_go(self, inplace=True):
        """filters for results that are only "Google organic".
        """
        obj = self[(self['source'] == 'google') & (self['medium'] == 'organic')].drop(columns=['source', 'medium'])
        if inplace:
            self._update_inplace(obj)
        else:
            return obj

    def bool_column_to_int(self, column_name):
        if column_name in self.columns:
            self[column_name] = self[column_name].apply(lambda b: 1 if b else 0)


class GSCDataFrame(pd.DataFrame):
    """
    Subclass of pandas.DataFrame specifically for use with Google Search Console responses.
    The subclass must be initialized with a GSC response object and will create a dataframe
    from the report.

    The DataFrame column headings are the same as the GA dimensions and metrics but without the 'ga:' prefix

    In addition to pandas DataFrame functionality, the GADataFrame has additional metadata attributes:
        dimensions: list of the GSC dimensions
    """

    _metadata = ["dimensions",
                 "metrics",
                 "response_aggregation",
                 "date_range"]

    def __init__(self, df_input,
                 gsc_dimensions: Optional[List[str]] = None,
                 from_gsc_response: bool = False):
        """

        :param df_input:
        :param gsc_dimensions:
        :param from_gsc_response: The init can either come from some date structure that ordinary pandas DataFrame
        can initialize, or a GSC response dictionary
        """

        # self.logger = logging.getLogger("GADataFrame")
        # self.logger.debug("initializing GADataFrame")

        if gsc_dimensions is None:
            gsc_dimensions = ['country', 'device', 'page', 'query']

        self.dimensions = gsc_dimensions

        if df_input is None:
            self.metrics = ['clicks', 'impressions', 'ctr', 'position']
            super().__init__(None, columns=[strip_ga_prefix(_) for _ in self.dimensions + self.metrics])
            if 'date' in self.columns:
                self.rename(columns={'date': 'record_date'}, inplace=True)
            if 'page' in self.columns:
                self.rename(columns={'page': 'landing_page'}, inplace=True)
        else:
            super().__init__(df_input)

        if from_gsc_response is True and df_input is not None:
            self.metrics = []
            for metric in ['clicks', 'impressions', 'ctr', 'position']:
                if metric in list(self.columns):
                    self.metrics.append(metric)

            for _i in range(len(self.dimensions) - 1, -1, -1):
                self.insert(loc=0, column=self.dimensions[_i],
                            value=self['keys'].apply(lambda x: x[_i])
                            )
            self.drop(columns='keys', inplace=True)

            if 'query' in self.columns:
                self['query'] = self['query'].apply(lambda s: s.lower())

            if 'page' in self.columns:
                self['landing_page_parameter'] = self['page'].apply(utils.url_extract_parameter)
                self['landing_page'] = self['page'].apply(utils.strip_url)
                self['landing_page_nodomain'] = self['page'].apply(utils.url_strip_domain)
                self.rename(columns={'page': 'landing_page_full'}, inplace=True)

        if from_gsc_response is False and df_input is not None:
            self.dimensions = list(self.columns)
            self.metrics = []
            for metric in ['clicks', 'impressions', 'ctr', 'position']:
                self.dimensions.remove(metric)
                if metric in list(self.columns):
                    self.metrics.append(metric)

        if 'device' in self.columns:
            self.device = self.device.apply(lambda s: s.lower())

        if 'date' in self.columns:
            self.date = pd.to_datetime(self.date, format="%Y-%m-%d")
            self.date = self.date.apply(lambda date_time: datetime.datetime.date(date_time))
            self.rename(columns={'date': 'record_date'}, inplace=True)

        if 'country' in self.columns:
            self.country = self.country.apply(lambda _s: _s.upper())
            self.rename(columns={'country': 'country_iso_code'}, inplace=True)

    def add_question_column(self):
        if 'query' in self.columns:
            self.insert(loc=self.columns.get_loc('query') + 1,
                        column='is_question',
                        value=self['query'].apply(is_question))
        else:
            pass

    def add_transactional_column(self):
        if 'query' in self.columns:
            self.insert(loc=self.columns.get_loc('query') + 1,
                        column='is_transactional',
                        value=self['query'].apply(is_transactional))
        else:
            pass

    def add_investigation_column(self):
        if 'query' in self.columns:
            self.insert(loc=self.columns.get_loc('query') + 1,
                        column='is_investigation',
                        value=self['query'].apply(is_investigation))
        else:
            pass

    def add_branded_column(self, branded_regex: Union[str, Pattern]):
        if 'query' in self.columns and self.branded_regex is not None:

            def is_branded_function(_query):
                if re.match(branded_regex, _query):
                    return True
                else:
                    return False

            self.insert(loc=self.columns.get_loc('query') + 1,
                        column='is_branded',
                        value=self['query'].apply(is_branded_function))
        else:
            pass

    def add_landing_page_subdomains(self):
        if 'landing_page' in self.columns:
            _loc = self.columns.get_loc('landing_page') + 1
            self.insert(loc=_loc,
                        column='lp_sub1',
                        value=self.landing_page.apply(get_sub1)
                        )
            self.insert(loc=_loc + 1,
                        column='lp_sub2',
                        value=self.landing_page.apply(get_sub2)
                        )
        else:
            pass

    def add_category_column(self, category_function: Optional[str], categories_dict: Optional[dict] = None):
        if 'query' in self.columns:
            _loc = self.columns.get_loc('query') + 1
            if category_function == "first word":
                self.insert(loc=_loc, column='category',
                            value=self.product_name.apply(lambda s: s.split(' ')[0]))
                self.insert(loc=_loc + 1, column='subcategory', value='')

            elif category_function == "regex" and categories_dict is not None:

                def regex_category_function_local(_query):
                    return regex_category_function(product_name=_query, categories_dict=categories_dict)

                category_df = self[['query']].apply(regex_category_function_local,
                                                    axis=1, result_type='expand')
                self.insert(loc=_loc, column='category', value=category_df[0])
                self.insert(loc=_loc + 1, column='subcategory', value=category_df[1])

            elif category_function == "separate_regex" and categories_dict is not None:

                def separate_regex_category_function1_local(_query):
                    return separate_regex_category_function1(product_name=_query, categories_dict=categories_dict)

                def separate_regex_category_function2_local(_query):
                    return separate_regex_category_function2(product_name=_query, categories_dict=categories_dict)

                self.insert(loc=_loc, column='category',
                            value=self['query'].apply(separate_regex_category_function1_local))
                self.insert(loc=_loc + 1, column='subcategory',
                            value=self['query'].apply(separate_regex_category_function2_local))
            else:
                self.insert(loc=_loc, column='category', value='')
                self.insert(loc=_loc + 1, column='subcategory', value='')

    def bin_by_position(self,
                        bins=None,
                        reset_index=True) -> pd.DataFrame:
        if bins is None:
            bins = [0, 5, 10, 20, 40, 80, np.Inf]
        _df = self.copy()
        _df['position'] = pd.cut(_df['position'], bins)
        binned_df = _df.groupby('position').sum()[['clicks', 'impressions']]
        binned_df.insert(loc=0, column='queries', value=_df.groupby('position').size())
        if reset_index:
            return binned_df.reset_index()
        else:
            return binned_df


def is_question(query: str) -> bool:
    if utils.RE_QUESTION.match(query):
        return True
    else:
        return False


def is_transactional(query: str) -> bool:
    if utils.RE_TRANSACTIONAL.match(query):
        return True
    else:
        return False


def is_investigation(query: str) -> bool:
    if utils.RE_INVESTIGATION.match(query):
        return True
    else:
        return False


def is_google_organic(source: str,
                      medium: str) -> bool:
    return (source == 'google') and (medium == 'organic')


def is_new_user(user_type: str) -> bool:
    return user_type == 'New Visitor'


def is_shopping_stage_all(shopping_stage: str) -> bool:
    return shopping_stage == 'ALL_VISITS'


def has_site_search(search_used: str) -> bool:
    return search_used == 'Visits With Site Search'


def regex_brand_function(product_name: str, brands_dict: dict):
    for _brand in brands_dict.keys():
        if re.match(brands_dict[_brand], product_name.lower()):
            return _brand
    return 'other'


def regex_category_function(product_name: str, categories_dict: dict):
    for cat in categories_dict.keys():
        for subcat in categories_dict.get(cat).keys():
            if re.match(categories_dict.get(cat).get(subcat),
                        product_name[0].lower()):
                return cat, subcat
    return 'other', 'other'


def separate_regex_category_function1(product_name: str, categories_dict: dict):
    cat_dict = categories_dict.get('categories')
    for cat in cat_dict.keys():
        if re.match(cat_dict.get(cat), product_name.lower()):
            return cat
    return 'other'


def separate_regex_category_function2(product_name: str, categories_dict: dict):
    subcat_dict = categories_dict.get('subcategories')
    for sub_cat in subcat_dict.keys():
        if re.match(subcat_dict.get(sub_cat), product_name.lower()):
            return sub_cat
    return 'other'


def strip_ga_prefix(s: str) -> str:
    if len(s) < 3:
        return s
    elif s[:3] == 'ga:':
        return s[3:]
    else:
        return s


def get_response_type(response: dict):
    # Check for GA response
    try:
        rows = response.get('reports', [])[0]['data']['rows']
        dimensions = response.get('reports')[0].get('columnHeader')['dimensions']
        return 'GA'
    except (TypeError, IndexError, KeyError, AttributeError):
        pass

    # Check for GSC response
    try:
        if {'clicks', 'impressions', 'ctr', 'position'} < set(response.get('rows', [])[0].keys()):
            return 'GSC'
    except (TypeError, IndexError, KeyError, AttributeError):
        pass

    return None


def response_type_from_file(file_path):
    return file_path.split('/')[-3].strip('_results')


def gsc_bin_by_position(df, bins: List[int] = None):
    if bins is None:
        bins = [0, 10, 20, 50, 100]
    _df = df.copy()
    _df['position'] = pd.cut(_df['position'], bins)
    binned_df = _df.groupby('position').sum()[['clicks', 'impressions']]
    binned_df.insert(loc=0, column='queries', value=_df.groupby('position').size())
    return binned_df.reset_index()


def iso_code_2_to_3(iso_code: str):
    if iso_code == "GB":
        return "GBR"
    elif iso_code == "US":
        return "USA"
    else:
        with open(os.path.join(os.path.dirname(__file__), 'data/country_iso_codes.csv'), mode='r') as infile:
            reader = csv.reader(infile)
            isodict = {rows[1]: rows[2] for rows in reader}
        return isodict.get(iso_code, 'ZZZ')


def get_sub_domain(page_path: str, n: int = 1):
    split = page_path.split('/')
    if len(split) > n:
        return page_path.split('/')[n]
    else:
        return ''


def get_sub1(page_path: str):
    return get_sub_domain(page_path=page_path, n=1)


def get_sub2(page_path: str):
    return get_sub_domain(page_path=page_path, n=2)


def remove_list_item(_list, _item) -> list:
    return [_ for _ in _list if _ != _item]
