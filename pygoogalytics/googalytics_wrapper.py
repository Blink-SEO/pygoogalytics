import pandas as pd
import logging
import re
import datetime
import json
from typing import List, Union, Optional, Any

from googleapiclient.errors import HttpError as GoogleApiHttpError
from google.api_core.exceptions import ResourceExhausted, InvalidArgument, PermissionDenied
import google.analytics.data_v1beta.types as ga_data_types

from . import googlepandas as gpd
from .utils import general_utils
from .utils.ga4_parser import parse_ga4_response, parse_ga3_response, join_ga4_responses
from . import pga_logger


class MissingID(AttributeError):
    """Error raised when Property ID is not specified"""
    def __init__(self, message="Missing ID"):
        self.message = message
        super().__init__(self.message)


class GoogalyticsWrapper:
    """
    The GoogalyticsWrapper requires the following arguments to access data:
    - for GSC data: sc_domain. This is the url-like string you see in the Google Search Console web application
    when selecting the site. It is either a full url (e.g. `https://www.example.com/`) or something like `sc_domain:example.com`
    - for GA3 data: the "view_id" you see in "settings" on the GA web application. This is usually an 8- or 9-digit number, passed as a string
    - for GA4 data: the ga4 property id.
    """
    def __init__(self,
                 gsc_resource,
                 ga3_resource,
                 ga4_resource,
                 sc_domain: str = None,
                 view_id: str = None,
                 ga4_property_id: str = None):

        self.sc_domain: str = sc_domain
        self.view_id: str = view_id
        self.ga4_property_id: str = ga4_property_id

        self._api_test_gsc: dict = dict()
        self._api_test_ga3: dict = dict()
        self._api_test_ga4: dict = dict()

        self.gsc_resource = gsc_resource
        self.ga3_resource = ga3_resource
        self.ga4_resource = ga4_resource

        pga_logger.debug(f"initialising GoogalyticsWrapper object")

    # *****************************************************************
    # *** GAPI_WRAPPER STATS ******************************************

    def __dict__(self) -> dict:
        _dates_test = self.available_dates
        gsc_date_range_str = general_utils.date_range_string(dates=_dates_test.get("GSC"),
                                                     alternate_text="No dates available from GSC")
        ga3_date_range_str = general_utils.date_range_string(dates=_dates_test.get("GA3"),
                                                     alternate_text="No dates available from GA3")

        return {
            "API config": {
                "GSC sc-domain": self.sc_domain,
                "GA3 View ID": self.view_id,
                "GA4 Property ID": self.ga4_property_id
            },
            "API status": {
                "GSC status": self.api_test_gsc.get('status'),
                "GA3 status": self.api_test_ga3.get('status'),
                "GSC error": self.api_test_gsc.get('error'),
                "GA3 error": self.api_test_ga3.get('error')
            },
            "Available datas": {
                "GSC": gsc_date_range_str,
                "GA3": ga3_date_range_str
            }
        }

    def __repr__(self):
        _s = "GoogalyticsWrapper object:\n"
        _s += json.dumps(self.__dict__(), indent=2)
        return _s

    @property
    def api_summary(self) -> dict:
        _dates_test = self.available_dates
        _sc_domain = ""
        if re.match("sc-domain:.+", self.sc_domain):
            _sc_domain = self.sc_domain
        return {"GA3 view id": self.view_id,
                "sc-domain": _sc_domain,
                "GA3 API": self.api_test_ga3.get('status'),
                "GSC API": self.api_test_gsc.get('status'),
                "GA3 dates": len(_dates_test.get("GA3")),
                "GSC dates": len(_dates_test.get("GSC")),
                }

    def _perform_api_test_gsc(self) -> dict:
        """test GSC API"""
        pga_logger.debug(f"{self.__class__.__name__}.api_test() :: testing GSC api")

        _api_error = None
        _dates_list = []

        if not self.sc_domain:
            return dict(
                status="SC Domain not set",
                error="SC Domain not set",
                date_count=0,
                timestamp=datetime.datetime.utcnow()
            )

        try:
            _ = self.get_gsc_response(start_date=datetime.date.today() + datetime.timedelta(days=-7),
                                      raise_http_error=True)
            _api_status = "Success"
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GSC api successful")
            _dates_list = self.get_dates(result="GSC")
        except GoogleApiHttpError as http_e:
            _api_status = "HttpError"
            _api_error = http_e.reason.split('See also')[0]
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GSC api failed")
        except Exception as http_e:
            _api_status = "Other Error"
            _api_error = repr(http_e)
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GSC api failed")
            # The HttpError for GSC contains this unhelpful "See also this answer to a question..."
            # which is just a link to an FAQ with a 404 error

        return dict(
            status=_api_status,
            error=_api_error,
            date_count=len(_dates_list),
            timestamp=datetime.datetime.utcnow()
        )

    @property
    def api_test_gsc(self) -> dict:
        if self._api_test_gsc.get('status') is None:
            self._api_test_gsc = self._perform_api_test_gsc()
        return self._api_test_gsc

    @property
    def api_test_ga3(self) -> dict:
        if self._api_test_ga3.get('status') is None or not general_utils.test_time(self._api_test_ga3.get('timestamp'), 3600):
            self._api_test_ga3 = self._perform_api_test_ga3()
        return self._api_test_ga3

    def _perform_api_test_ga3(self) -> dict:
        """test GA API"""
        pga_logger.debug(f"{self.__class__.__name__}.api_test() :: testing GA api")
        _api_error = None
        if not self.view_id:
            return dict(
                status="No View ID",
                error="GA3 View ID not set",
                date_count=0,
                timestamp=datetime.datetime.utcnow()
            )

        _dates_list = []
        try:
            _date = datetime.date.today() + datetime.timedelta(days=-7)
            _ = self.get_ga3_response(
                start_date=_date,
                end_date=_date,
                dimensions=['date'],
                metrics=['users'],
                raise_http_error=True,
                log_error=False
            )
            _api_status = "Success"
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GA api successful")
            _dates_list = self.get_dates(result="GA3")
        except GoogleApiHttpError as http_e:
            _api_status = "HttpError"
            _api_error = repr(http_e)
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GA api failed")
        except Exception as http_e:
            _api_status = "Other Error"
            _api_error = repr(http_e)
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GA api failed")
        return dict(
            status=_api_status,
            error=_api_error,
            date_count=len(_dates_list),
            timestamp=datetime.datetime.utcnow()
        )

    @property
    def api_test_ga4(self) -> dict:
        if self._api_test_ga4.get('status') is None or not general_utils.test_time(self._api_test_ga4.get('timestamp'), 3600):
            self._api_test_ga4 = self._perform_api_test_ga4()
        return self._api_test_ga4

    def _perform_api_test_ga4(self) -> dict:
        """test GA4 API"""
        pga_logger.debug(f"{self.__class__.__name__}.api_test() :: testing GA4 api")

        if not self.ga4_property_id:
            return dict(
                status="No property id",
                error="Ga4 Property ID not set",
                date_count=0,
                timestamp=datetime.datetime.utcnow()
            )

        _dates_list = []
        _error = None
        try:
            _date = datetime.date.today() + datetime.timedelta(days=-7)
            _response = self.get_ga4_response(
                start_date=_date,
                end_date=_date,
                dimensions=['date'],
                metrics=['totalUsers']
            )
            _error = _response.get('error')
            if _error:
                _api_status = _response.get('error_type')
                _api_error = str(_error)
            else:
                _api_status = "Success"
                pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GA4 api successful")
                _dates_list = self.get_dates(result="GA4")
                _api_error = None
        except GoogleApiHttpError as http_e:
            _api_status = "HttpError"
            _api_error = repr(http_e)
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GA4 api failed")
        except Exception as http_e:
            _api_status = "Other Error"
            _api_error = repr(http_e)
            pga_logger.debug(f"{self.__class__.__name__}.api_test() :: GA4 api failed")


        return dict(
            status=_api_status,
            error=_api_error,
            date_count=len(_dates_list),
            timestamp=datetime.datetime.utcnow()
        )

    # *****************************************************************************************

    @property
    def available_dates(self) -> dict:
        gsc_date_list: List[datetime.date] = self.get_dates(result="GSC")
        ga3_date_list: List[datetime.date] = self.get_dates(result="GA3")
        ga4_date_list: List[datetime.date] = self.get_dates(result="GA4")
        return {"GA3": ga3_date_list, "GA4": ga4_date_list, "GSC": gsc_date_list}

    # *** Calls to Google API *****************************************************************

    def get_inspection_response(self,
                                inspection_url: str):
        gsc_request = {
            'siteUrl': self.sc_domain,
            'inspectionUrl': inspection_url,
        }
        gsc_response = self.gsc_resource.urlInspection().index().inspect(body=gsc_request).execute()
        return gsc_response

    def get_gsc_response(self,
                         start_date: Union[str, datetime.date],
                         end_date: Optional[Union[str, datetime.date]] = None,
                         gsc_dimensions: Optional[Union[List[str], str]] = None,
                         row_limit: int = 25000,
                         start_row: int = 0,
                         raise_http_error: bool = False,
                         _print_log: bool = False):

        # dimension "searchAppearance" cannot be used alongside any other dimension

        if end_date is None:
            end_date = start_date

        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

        start_date_string = start_date.strftime("%Y-%m-%d")
        end_date_string = end_date.strftime("%Y-%m-%d")

        if gsc_dimensions is None:
            gsc_dimensions = ['country', 'device', 'page', 'query']
        elif isinstance(gsc_dimensions, str):
            gsc_dimensions = [gsc_dimensions]

        gsc_request = {
            'startDate': start_date_string,
            'endDate': end_date_string,
            'dimensions': gsc_dimensions,
            # 'dimensionFilterGroups': [{
            #     'filters': [
            #         {'dimension': 'country', 'expression': 'GBR'}
            #     ]
            # }],
            # 'aggregationType': aggregation_type,
            'rowLimit': row_limit,
            'startRow': start_row
        }

        try:
            gsc_response = self.gsc_resource.searchanalytics().query(siteUrl=self.sc_domain,
                                                                     body=gsc_request).execute()
        except GoogleApiHttpError as http_error:
            if re.match(".*user does not have sufficient permissions", repr(http_error).lower()):
                pga_logger.error(
                    f"{self.__class__.__name__}.get_gsc_response() :: user does not have sufficient permissions")
            if raise_http_error:
                raise http_error
            else:
                if _print_log:
                    print(f"{self.__class__.__name__}.get_gsc_response() :: GoogleApiHttpError")
                    print(http_error)
                return None

        try:
            _rows = gsc_response.get("rows", None)
        except AttributeError:
            _rows = None

        if _rows is None:
            pga_logger.debug(f"{self.__class__.__name__}.get_gsc_response() :: empty gsc response")
            if _print_log:
                print(f"{self.__class__.__name__}.get_gsc_response() :: empty gsc response")
            # raise EmptyResponseError("GSC", start_date=start_date, end_date=end_date)
            return None

        return gsc_response

    def get_ga3_response(self,
                         start_date: datetime.date,
                         end_date: datetime.date,
                         dimensions: list[str] = None,
                         metrics: list[str] = None,
                         ga_filters: dict | None = None,
                         raise_http_error: bool = False,
                         log_error: bool = True,
                         filter_google_organic: bool = False,
                         _print_log: bool = False) -> Optional[dict]:

        ga_dimensions = ['ga:' + _s if not re.match(r'ga:', _s) else _s for _s in dimensions]
        ga_metrics = ['ga:'+_s if not re.match(r'ga:', _s) else _s for _s in metrics]

        r = self._ga3_response_raw(
            start_date=start_date,
            end_date=end_date,
            ga_dimensions=ga_dimensions,
            ga_metrics=ga_metrics,
            ga_filters=ga_filters,
            filter_google_organic=filter_google_organic,
            page_token=None
        )

        data = r.get('reports', [{}])[0].get('data', {}).get('rows', [])
        column_header = r.get('reports', [{}])[0].get('columnHeader')
        next_page_token = r.get('reports', [{}])[0].get('nextPageToken')
        error = r.get('error', None)
        error_type = r.get('error_type', None)

        while next_page_token and not error:
            r = self._ga3_response_raw(
                start_date=start_date,
                end_date=end_date,
                ga_dimensions=ga_dimensions,
                ga_metrics=ga_metrics,
                ga_filters=ga_filters,
                filter_google_organic=filter_google_organic,
                page_token=next_page_token
            )
            error = r.get('error', None)
            error_type = r.get('error_type', None)
            _d = r.get('reports', [dict()])[0].get('data', dict()).get('rows', [])
            data.extend(_d)
            next_page_token = r.get('reports', [{}])[0].get('nextPageToken')

        # print(f"\ndimensions: {ga_dimensions} \n"
        #       f"metrics: {ga_metrics} \n "
        #       f"data: len={len(data)}, \n"
        #       f"error_type: {error_type} \n"
        #       f"column_header: {column_header} \n")

        if column_header is not None:
            response = parse_ga3_response(column_header=column_header, response_rows=data)
        else:
            response = {
                'dimension_headers': dimensions,
                'metric_headers': metrics,
                'row_count': 0,
                'rows': []
            }

        response['response_type'] = 'GA3'
        response['start_date'] = start_date
        response['end_date'] = end_date

        response['error'] = error
        response['error_type'] = error_type

        return response

    def _ga3_response_raw(self,
                          start_date: datetime.date,
                          end_date: datetime.date,
                          ga_dimensions: list[str],
                          ga_metrics: List[str],
                          ga_filters: dict | None = None,
                          filter_google_organic: bool = False,
                          return_raw_response: bool = False,
                          page_token: str = None,
                          _print_log: bool = False):

        if not self.view_id:
            _r = {
                'error': PermissionError("view_id is not set"),
                'error_type': 'missing_view_id'
            }
            return _r

        start_date_string = start_date.strftime("%Y-%m-%d")
        end_date_string = end_date.strftime("%Y-%m-%d")

        _dfc = []  # dimension filter clauses
        _mfc = []  # metric filter clauses
        _orderby = []

        if ga_filters:
            for filter_dict in ga_filters:
                if not isinstance(filter_dict.get('filters'), list):
                    continue
                if len(filter_dict.get('filters')) != 0:
                    if filter_dict.get('filters')[0].get('dimensionName'):
                        _dfc.append(filter_dict)
                    elif filter_dict.get('filters')[0].get('metricName'):
                        _mfc.append(filter_dict)

        if filter_google_organic is True:
            _dfc.append({"operator": 'OR',
                         "filters": [{"dimensionName": 'ga:sourceMedium',
                                      "not": 'false',
                                      "operator": 'EXACT',
                                      "expressions": ['google / organic'],
                                      "caseSensitive": 'false'
                                      }]
                         })

        if 'ga:itemRevenue' in ga_metrics:
            _orderby.append({
                "fieldName": 'ga:itemRevenue',
                "orderType": 'VALUE',
                "sortOrder": 'DESCENDING'
            })

        _request_dict = {
            'viewId': self.view_id,
            'dateRanges': [{'startDate': start_date_string, 'endDate': end_date_string}],
            # 'dimensions': [{'name': 'ga:productName'}],
            # 'metrics': [{'expression': 'ga:itemRevenue'}]
            'dimensions': [{'name': _d} for _d in ga_dimensions],
            "dimensionFilterClauses": _dfc,
            "metricFilterClauses": _mfc,
            "orderBys": _orderby,
            'metrics': [{'expression': _m} for _m in ga_metrics],
            'pageSize': 100_000
        }

        if page_token:
            _request_dict.update({'pageToken': page_token})

        ga3_request = {'reportRequests': [_request_dict]}

        _error = None
        _error_type = None
        try:
            ga3_response = self.ga3_resource.reports().batchGet(body=ga3_request).execute()
            if return_raw_response:
                pga_logger.info(f"{self.__class__.__name__}.get_ga3_response() :: returning raw response")
                return ga3_response
        except GoogleApiHttpError as http_error:
            _error = http_error
            _msg = ''
            if re.match(".*user does not have sufficient permissions", repr(http_error).lower()):
                _error_type = 'insufficient_permissions'
                _msg = f"{self.__class__.__name__}.get_ga3_response() :: user does not have sufficient permissions"
            if re.match(".*viewid must be set", repr(http_error).lower()):
                _error_type = 'missing_view_id'
                _msg = f"{self.__class__.__name__}.get_ga3_response() :: view id is not set"
            ga3_response = None
        except Exception as _e:
            _error = _e
            _error_type = 'other'
            ga3_response = None

        if ga3_response is None:
            ga3_response = dict()
        else:
            try:
                _rows = ga3_response.get('reports', [])[0].get('data').get('rows', None)
            except (AttributeError, KeyError) as _e:
                _rows = None

            if _rows is None:
                _error = AttributeError('ga3_response in incorrect format.')
                _error_type = 'empty_response'
                pga_logger.debug(f"{self.__class__.__name__}.get_ga3_response() :: empty ga response")
                # raise EmptyResponseError("GA3", start_date=start_date, end_date=end_date)

        ga3_response['error'] = _error
        ga3_response['error_type'] = _error_type

        return ga3_response

    def _ga4_response_raw(self,
                         start_date: datetime.date,
                         end_date: datetime.date,
                         ga4_dimensions: list[ga_data_types.Dimension],
                         ga4_metrics: list[ga_data_types.Metric],
                         limit: int,
                         offset: int):

        if not self.ga4_property_id:
            raise MissingID("ga4_property_id is not set")

        request = ga_data_types.RunReportRequest(
            property=f"properties/{self.ga4_property_id}",
            dimensions=ga4_dimensions,
            metrics=ga4_metrics,
            date_ranges=[
                ga_data_types.DateRange(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d")
                )
            ],
            limit=limit,
            offset=offset,
            return_property_quota=True
        )
        ga4_response = self.ga4_resource.run_report(request)

        return ga4_response

    def get_ga4_response(self,
                         start_date: datetime.date,
                         end_date: datetime.date,
                         dimensions: list[str],
                         metrics: list[str],
                         limit: int | None = None) -> (list, dict, Any):

        ga_dimensions = [ga_data_types.Dimension(name=_) for _ in dimensions]
        ga_metrics = [ga_data_types.Metric(name=_) for _ in metrics]

        request_limit = 100_000
        if limit is None:
            limit = 1_000_000_000
        elif limit < 100_000:
            request_limit = limit

        complete: bool = False
        error_type: str | None = None
        responses: list[dict] = []
        error = None
        offset: int = 0

        while not complete:
            num_tries = 0
            success = False
            response = dict()
            while num_tries < 3 and not success:
                error = None
                try:
                    ga4_response = self._ga4_response_raw(
                        start_date=start_date,
                        end_date=end_date,
                        ga4_dimensions=ga_dimensions,
                        ga4_metrics=ga_metrics,
                        limit=request_limit,
                        offset=offset
                    )
                    response = parse_ga4_response(ga4_response)
                    success = True
                except PermissionDenied as _permission_denied_error:
                    complete = True
                    error_type = 'PermissionDenied'
                    error = _permission_denied_error
                    num_tries = 1_000
                except ResourceExhausted as _resource_exhausted_error:
                    error_type = 'ResourceExhausted'
                    complete = True
                    error = _resource_exhausted_error
                    num_tries = 1_000
                except MissingID as _id_error:
                    complete = True
                    error_type = 'MissingID'
                    error = _id_error
                    num_tries = 1_000
                except InvalidArgument as _invalid_argument_error:
                    error_type = 'InvalidArgument'
                    if re.search(r"metrics are incompatible", _invalid_argument_error.message):
                        error_type = 'IncompatibleMetrics'
                    if re.search(r"Invalid property ID", _invalid_argument_error.message):
                        error_type = 'InvalidPropertyID'
                    complete = True
                    error = _invalid_argument_error
                    num_tries = 1_000
                except Exception as _e:
                    error_type = "Error"
                    error = _e
                    num_tries += 1

            if success:
                offset += response.get('row_count', 0)
                if offset >= limit:
                    complete = True
                if response.get('row_count', 0) == 0 or offset >= response.get('meta_row_count', 1_000_000_000):
                    complete = True

                responses.append(response)

        if len(responses) > 1:
            response = join_ga4_responses(responses)
            if response.get('row_count', 0) == 0:
                error = AttributeError("Empty response")
                error_type = "EmptyResponse"
        elif len(responses) == 1:
            response = responses[0]
            if response.get('row_count', 0) == 0:
                error = AttributeError("Empty response")
                error_type = "EmptyResponse"
        else:
            response = {
                'response_type': 'GA4',
                'dimension_headers': dimensions,
                'metric_headers': metrics,
                'rows': []
            }

        response['start_date'] = start_date
        response['end_date'] = end_date

        response['error'] = error
        response['error_type'] = error_type

        return response

    def get_dates(self,
                  result: str,
                  start_date: Optional[Union[datetime.date, str, int]] = None,
                  end_date: Optional[Union[datetime.date, str]] = None,
                  reverse: bool = False) -> List[datetime.date]:

        # set the end_date to yesterday by default.
        # GA data is "available" for today but it is not the whole day.
        if end_date is None:
            end_date = datetime.date.today() + datetime.timedelta(days=-1)

        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        if isinstance(start_date, int):
            start_date = end_date + datetime.timedelta(days=-1 * start_date)

        if re.match(r"GA3", result):
            if start_date is None:
                start_date = datetime.date.today() + datetime.timedelta(days=-1500)
            dimensions = ['ga:date']
            metrics = ['ga:sessions']
        elif re.match(r"GA4", result):
            if start_date is None:
                start_date = datetime.date.today() + datetime.timedelta(days=-1500)
            dimensions = ['date']
            metrics = ['sessions']
        elif re.match(r"GSC", result):
            if start_date is None:
                start_date = datetime.date.today() + datetime.timedelta(days=-500)
            dimensions = ['date']
            metrics = None
        else:
            raise KeyError(f"invalid result {result}")

        _df = self.get_df(result=result,
                          start_date=start_date,
                          end_date=end_date,
                          dimensions=dimensions,
                          metrics=metrics,
                          add_boolean_metrics=False)

        if _df is None or "record_date" not in _df.columns or len(_df) == 0:
            return []

        if "record_date" not in _df.columns or len(_df) == 0:
            return []
        else:
            return sorted(list(_df["record_date"]), reverse=reverse)

    # *****************************************************************************************
    # *** Return dataframe *****************************************************************

    def get_df(self,
               result: str,
               start_date: Union[str, datetime.date] = None,
               end_date: Optional[Union[str, datetime.date]] = None,
               dimensions: Optional[Union[str, List[str]]] = None,
               metrics: Optional[Union[str, List[str]]] = None,
               row_limit: Optional[int] = None,
               url_list: Optional[Union[str, List[str]]] = None,
               filter_google_organic: bool = False,
               filters: List[dict] = None,
               add_boolean_metrics: bool = False,
               _return_response: bool = False,
               raise_errors: bool = True
               ) -> Union[gpd.GADataFrame, gpd.GSCDataFrame, pd.DataFrame]:
        """
        The `get_df` method accepts the following values for the `result` argument:
        - "GSC": for Google Search Console data
        - "GA3": for Google Analytics 3 (UA) data
        - "URL": for Google Search Console URL inspection data
        - "GA4": for Google Analytics 4 data (note, this is not yet available in production)
        """

        if start_date is None:
            if result == "GSC":
                start_date = datetime.date.today() + datetime.timedelta(days=-3)
            else:
                start_date = datetime.date.today()

        if end_date is None:
            end_date = start_date

        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        if isinstance(dimensions, str):
            dimensions = [dimensions]
        if isinstance(metrics, str):
            metrics = [metrics]
        if isinstance(url_list, str):
            url_list = [url_list]

        if re.match(r"GA4", result):
            return self._get_analytics_df(
                response_type='GA4',
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                metrics=metrics,
                add_boolean_metrics=add_boolean_metrics,
                limit=row_limit,
                filters=filters,
                return_response=_return_response,
            )
        elif re.match(r"GA3", result):
            return self._get_analytics_df(
                response_type='GA3',
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                metrics=metrics,
                filters=filters,
                add_boolean_metrics=add_boolean_metrics,
                return_response=_return_response
            )
        elif re.match(r"GSC", result) and result != "GSCQ":
            if row_limit is None:
                row_limit = 100000
            return self._get_gsc_df(start_date=start_date,
                                    end_date=end_date,
                                    gsc_dimensions=dimensions,
                                    row_limit=row_limit,
                                    add_boolean_metrics=add_boolean_metrics)
        elif result == 'GSCQ':
            if row_limit is None:
                row_limit = 100000
            return self._get_gsc_df(start_date=start_date,
                                    end_date=end_date,
                                    gsc_dimensions=['query'],
                                    row_limit=row_limit,
                                    add_boolean_metrics=add_boolean_metrics)
        elif result == 'URL':
            return self._get_urlinspection_df(url_list=url_list)
        else:
            raise KeyError(f"invalid result {result}")

    def _get_gsc_df_raw(self,
                        start_date: datetime.date,
                        end_date: datetime.date,
                        row_limit: int,
                        gsc_dimensions: List[str]) -> Optional[gpd.GSCDataFrame]:

        gsc_response = self.get_gsc_response(start_date=start_date, end_date=end_date,
                                             gsc_dimensions=gsc_dimensions,
                                             row_limit=min(row_limit, 25000))

        if gsc_response is None:
            # Make an empty GSCDataFrame
            gsc_df = gpd.GSCDataFrame(df_input=None,
                                      gsc_dimensions=gsc_dimensions)
            _response_aggregation = None
        else:
            # Make a dataframe of the gsc response
            gsc_df = gpd.from_response(response=gsc_response,
                                       response_type="GSC",
                                       gsc_dimensions=gsc_dimensions)
            _response_aggregation = gsc_df.response_aggregation

        if (row_limit > 25000) and (len(gsc_df) == 25000):
            temp_row_limit = row_limit - 25000
            temp_start_row = 25000  # not 25001: "Zero-based index of the first row in the response"
            frames = [gsc_df]
            while temp_row_limit > 0:

                gsc_response2 = self.get_gsc_response(start_date=start_date, end_date=end_date,
                                                      gsc_dimensions=gsc_dimensions,
                                                      row_limit=min(temp_row_limit, 25000),
                                                      start_row=temp_start_row)
                if gsc_response2 is None:
                    break  # exit the while loop if we get an empty response

                # Make a dataframe of the second gsc response
                new_gsc_df = gpd.from_response(response=gsc_response2,
                                               response_type="GSC",
                                               gsc_dimensions=gsc_dimensions)
                frames.append(new_gsc_df)

                temp_row_limit -= 25000
                temp_start_row += 25000

            if len(frames) > 1:
                gsc_df = gpd.GSCDataFrame(pd.concat(frames, ignore_index=True),
                                          gsc_dimensions=gsc_dimensions)

        gsc_df.response_aggregation = _response_aggregation

        return gsc_df

    def _get_gsc_df(self,
                    start_date: datetime.date,
                    end_date: datetime.date,
                    row_limit: int = 200000,
                    gsc_dimensions: Optional[List[str]] = None,
                    add_boolean_metrics: bool = True) -> Optional[gpd.GSCDataFrame]:

        if not self.sc_domain:
            return None

        if gsc_dimensions is None:
            gsc_dimensions = ['date', 'country', 'device', 'page', 'query']

        gsc_df = self._get_gsc_df_raw(start_date=start_date,
                                      end_date=end_date,
                                      row_limit=row_limit,
                                      gsc_dimensions=gsc_dimensions)

        if gsc_df is None:
            return None

        if add_boolean_metrics:
            gsc_df.add_question_column()
            gsc_df.add_transactional_column()
            gsc_df.add_investigation_column()

        return gsc_df

    def _get_analytics_df(self,
                    response_type: str,
                    start_date: datetime.date | str,
                    end_date: datetime.date | str | None = None,
                    dimensions: Optional[List[str]] = None,
                    metrics: list[str] | list[list[str]] = None,
                    add_boolean_metrics: bool = True,
                    filters: Optional[dict] = None,
                    limit: int | None = 100_000_000,
                    return_response: bool = False,
                    raise_errors: bool = False,
                    _print: bool = False) -> gpd.GADataFrame:

        if dimensions is None:
            dimensions = ['dateHour']
        elif isinstance(dimensions, str):
            dimensions = dimensions.split(';')
        dimensions = [re.sub(r"^ga:", "", _) for _ in dimensions]

        if metrics is None:
            metrics = ['itemRevenue']

        if not end_date:
            end_date = start_date
        if isinstance(start_date, str):
            start_date = general_utils.parse_date(start_date)
        if isinstance(end_date, str):
            end_date = general_utils.parse_date(end_date)
        if end_date < start_date:
            raise ValueError("date range incompatible: end_date < start_date")
        if start_date > datetime.date.today():
            raise ValueError("date range incompatible: start_date in the future")

        if isinstance(metrics, str):
            metrics = metrics.split(';')
        if all(isinstance(_, str) for _ in metrics):
            metrics = [metrics]

        metrics_list: list[list[str]] = []
        for _list in metrics:
            metrics_list.extend([_list[10 * i:10 * i + 10] for i in range((len(_list) - 1) // 10 + 1)])
        metrics_list = [[re.sub(r"^ga:", "", _m) for _m in _sub_list] for _sub_list in metrics_list]

        responses: list = []
        breaking_error: bool = False
        breaking_error_type: str | None = None
        for _metrics in metrics_list:
            if response_type == 'GA3':
                _r = self.get_ga3_response(
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=dimensions,
                    metrics=_metrics,
                    ga_filters=filters,
                    raise_http_error=False
                )
            elif response_type == 'GA4':
                _r = self.get_ga4_response(
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=dimensions,
                    metrics=_metrics,
                    limit=limit
                )
            else:
                raise KeyError("response_type not recognised")

            responses.append(_r)
            if _r.get('error_type') is not None:
                if _r.get('error_type') in ('empty_response', 'EmptyResponse', 'emptyResponse'):
                    _r['error_type'] = 'empty_response'
                else:
                    breaking_error = True
                    breaking_error_type = _r.get('error_type')
                    if _print:
                        print(f"Breaking error: {breaking_error_type}")
                    break


        if return_response:
            return responses

        if raise_errors:
            _errors = [_r.get('error') for _r in responses if _r.get('error') is not None]
            if len(_errors) > 0:
                raise _errors[0]

        if breaking_error:
            frames = []
        else:
            frames = [gpd.from_response(response=_r) for _r in responses]

        # if len(frames)>0:
        #     return frames

        if _print:
            print(f"Returned {len(frames)} frames:")
            for _f in frames:
                print(f"{len(_f)}: {_f.columns.to_list()}")

        if len(frames) == 0:
            dataframe = gpd.GADataFrame(df_input=None,
                                     dimensions=dimensions,
                                     metrics=general_utils.expand_list(metrics_list),
                                     start_date=start_date,
                                     end_date=end_date,
                                     error = breaking_error_type)

        elif all(len(_frame) == 0 for _frame in frames):
            dataframe = gpd.GADataFrame(df_input=None,
                                     dimensions=dimensions,
                                     metrics=general_utils.expand_list(metrics_list),
                                     start_date=start_date,
                                     end_date=end_date,
                                     error = breaking_error_type)
        elif len(frames) == 1:
            dataframe = frames[0]
        else:
            dataframe = frames[0]
            for i in range(1, len(frames)):
                dataframe = dataframe.join_on_dimensions(frames[i], how="outer")
            if len(dataframe) == 0 and not dataframe.error:
                dataframe.error = 'empty_response'

        if add_boolean_metrics:
            dataframe.add_google_organic_column()
            dataframe.add_has_item_column()
            dataframe.add_new_user_column()
            dataframe.add_shopping_stage_all_column()
            dataframe.add_has_site_search_column()

        dataframe.fill_nan_with_zeros()

        return dataframe


    def urlinspection_dict(self, url: str, inspection_index: int = None) -> dict:
        _now = datetime.datetime.utcnow()
        _d = {"record_date": _now.date(),
              "record_time": _now.time(),
              "url": url}

        try:
            response = self.get_inspection_response(url)
        except Exception as _e:
            _d.update({"response": repr(_e)})
            return _d

        _inspectionResult = response.get("inspectionResult")
        if _inspectionResult is None:
            _d.update({"response": "empty"})
            return _d

        _d.update({"response": "success"})

        _indexStatusResult = _inspectionResult.get("indexStatusResult")
        if _indexStatusResult is not None:
            _last_crawl_time = _indexStatusResult.get("lastCrawlTime")
            if _last_crawl_time is not None:
                _last_crawl_time = datetime.datetime.strptime(_last_crawl_time, "%Y-%m-%dT%H:%M:%SZ")
            _d.update({"index_status_result_verdict": _indexStatusResult.get("verdict"),
                       "coverage_state": _indexStatusResult.get("coverageState"),
                       "robotstxt_state": _indexStatusResult.get("robotsTxtState"),
                       "indexing_state": _indexStatusResult.get("indexingState"),
                       "last_crawl_time": _last_crawl_time,
                       "page_fetch_state": _indexStatusResult.get("pageFetchState"),
                       "google_canonical": _indexStatusResult.get("googleCanonical"),
                       "user_canonical": _indexStatusResult.get("userCanonical"),
                       "sitemap": _indexStatusResult.get("sitemap"),
                       "referring_urls": _indexStatusResult.get("referringUrls"),
                       "crawled_as": _indexStatusResult.get("crawledAs")})

        _mobileUsabilityResult = _inspectionResult.get("mobileUsabilityResult")
        if _mobileUsabilityResult is not None:
            _d.update({"mobile_usability_result_verdict": _mobileUsabilityResult.get("verdict")
                       })

        _mobileUsabilityIssue = _inspectionResult.get("mobileUsabilityIssue")
        if _mobileUsabilityIssue is not None:
            _d.update({"mobile_usability_result_verdict": _mobileUsabilityResult.get("verdict")
                       })
        return _d

    def _get_urlinspection_df(self,
                              url_list: List[str]) -> pd.DataFrame:
        if isinstance(url_list, str):
            url_list = [url_list]
        pga_logger.info(f"{self.__class__.__name__}.get_urlinspection_df() :: "
                        f"requesting url inspection for {len(url_list)} urls")
        _frames = []
        for _i, url in enumerate(url_list):
            _frames.append(self.urlinspection_dict(url, inspection_index=_i))

        if len(_frames) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(_frames)

        df.rename(columns={'url': 'url_full'}, inplace=True)

        df['url_parameter'] = df['url_full'].apply(general_utils.url_extract_parameter)
        df['url'] = df['url_full'].apply(general_utils.strip_url)
        df['url_nodomain'] = df['url_full'].apply(general_utils.url_strip_domain)

        return df
