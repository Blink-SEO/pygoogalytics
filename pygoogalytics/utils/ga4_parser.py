import re

from google.analytics.data_v1beta.types.analytics_data_api import RunReportResponse

from .general_utils import dict_merge

def parse_ga4_response(response: RunReportResponse):
    dimension_headers = [_.name for _ in response.dimension_headers]
    metric_headers = [_.name for _ in response.metric_headers]
    if response.row_count > 0:
        rows = [
            (
                {dim_k: dim_v for dim_k, dim_v in zip(dimension_headers, [_dv.value for _dv in _r.dimension_values])},
                {met_k: float(met_v) for met_k, met_v in zip(metric_headers, [_dv.value for _dv in _r.metric_values])}
            ) for _r in response.rows
        ]
        rows = [dict_merge(t[0], t[1]) for t in rows]
    else:
        rows = []

    _quota = response.property_quota
    _quota_dict = {
        'tokens_per_day': {
            'consumed': _quota.tokens_per_day.consumed,
            'remaining': _quota.tokens_per_day.remaining
        },
        'tokens_per_hour':{
            'consumed': _quota.tokens_per_hour.consumed,
            'remaining': _quota.tokens_per_hour.remaining
        }
    }

    response = {
        'response_type': 'GA4',
        'dimension_headers': dimension_headers,
        'metric_headers': metric_headers,
        'meta_row_count': response.row_count,
        'currency_code': response.metadata.currency_code,
        'time_zone': response.metadata.time_zone,
        'row_count': len(rows),
        'quota': _quota_dict,
        'rows': rows
    }

    return response


def join_ga4_responses(responses: list[dict]) -> dict:
    _rows = []
    for _r in responses:
        _rows.extend(_r.get('rows', []))
    response = {
        'response_type': 'GA4',
        'dimension_headers': responses[0].get('dimension_headers'),
        'metric_headers': responses[0].get('metric_headers'),
        'meta_row_count': responses[0].get('meta_row_count'),
        'currency_code': responses[0].get('currency_code'),
        'time_zone': responses[0].get('time_zone'),
        'row_count': len(_rows),
        'quota': responses[-1].get('quota'),
        'rows': _rows
    }
    return response

def parse_ga3_response(response = None,
              column_header = None,
              response_rows = None,
              report_index: int = 0):

    if response is not None:
        column_header = response.get('reports', [dict()])[report_index].get('columnHeader')
        response_rows = response.get('reports', [dict()])[report_index].get('data', dict()).get('rows')

    if column_header is None or response_rows is None:
        raise KeyError("Incompatible response")

    dimensions = column_header.get('dimensions', [])
    _metrics_headers = column_header.get('metricHeader', dict()).get('metricHeaderEntries', [dict()])
    metrics = [_.get('name') for _ in _metrics_headers]

    dimensions = [remove_ga_prefix(_) for _ in dimensions]
    metrics = [remove_ga_prefix(_) for _ in metrics]

    _dm = dimensions+metrics
    _rows = [_.get('dimensions') + _.get('metrics', [dict()])[0].get('values') for _ in response_rows]
    rows = [{_k: _v for _k, _v in zip(_dm, _row_values)} for _row_values in _rows]

    response = {
        'response_type': 'GA3',
        'dimension_headers': dimensions,
        'metric_headers': metrics,
        'row_count': len(rows),
        'rows': rows
    }

    return response


def remove_ga_prefix(key: str) -> str:
    return re.sub(r'^ga:', '', key)
