from google.analytics.data_v1beta.types.analytics_data_api import RunReportResponse

from .utils import dict_merge

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

    metadata = {
        'response_type': 'GA4',
        'dimension_headers': dimension_headers,
        'metric_headers': metric_headers,
        'meta_row_count': response.row_count,
        'currency_code': response.metadata.currency_code,
        'time_zone': response.metadata.time_zone,
        'row_count': len(rows),
        'quota': _quota_dict
    }

    return rows, metadata
