import json

from google.oauth2 import service_account  # pip install --upgrade google-auth
from googleapiclient import discovery  # pip install --upgrade google-api-python-client
from google.analytics.data_v1beta import BetaAnalyticsDataClient  # pip install google-analytics-data


def resources_from_key_file(path: str):
    with open(path, 'rb') as _file:
        _json_string = _file.read().decode('utf8')
    return resources_from_json(json_string=_json_string)


def resources_from_json(json_string: str):
    _secrets = json.loads(json_string)
    _project_credentials = get_analytics_project_credentials(secrets=_secrets)
    return get_analytics_resources(analytics_project_credentials=_project_credentials)


def get_analytics_project_credentials(secrets: dict):
    analytics_project_scopes = ['https://www.googleapis.com/auth/analytics.readonly',
                                'https://www.googleapis.com/auth/webmasters.readonly',
                                'https://www.googleapis.com/auth/webmasters',
                                'https://www.googleapis.com/auth/cloud-platform']

    project_credentials = service_account.Credentials.from_service_account_info(
        info=secrets).with_scopes(scopes=analytics_project_scopes)
    return project_credentials


def get_analytics_resources(analytics_project_credentials):
    ga3_resource = discovery.build('analyticsreporting', 'v4', credentials=analytics_project_credentials)
    gsc_resource = discovery.build('searchconsole', 'v1', credentials=analytics_project_credentials)
    ga4_data_client = BetaAnalyticsDataClient(credentials=analytics_project_credentials)
    return ga3_resource, ga4_data_client, gsc_resource
