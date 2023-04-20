import json
import yaml

from typing import Tuple

from google.oauth2 import service_account  # pip install --upgrade google-auth
from googleapiclient import discovery  # pip install --upgrade google-api-python-client
from google.analytics.data_v1beta import BetaAnalyticsDataClient  # pip install google-analytics-data
from google.ads.googleads.client import GoogleAdsClient


def googleads_client_from_key_file(path: str):
    with open(path, 'rb') as _file:
        googleads_yaml_string = _file.read().decode('utf8')
    return googleads_client_from_yaml(googleads_yaml_string=googleads_yaml_string)


def googleads_client_from_yaml(googleads_yaml_string: str) -> Tuple[GoogleAdsClient, str]:
    googleads_yaml_dict = yaml.safe_load(googleads_yaml_string)
    default_customer_id = str(googleads_yaml_dict.get('default_customer_id'))
    googleads_client = GoogleAdsClient.load_from_string(yaml_str=googleads_yaml_string, version="v12")
    return googleads_client, default_customer_id


def get_analytics_resources(json_api_key: str = None, key_file_path: str = None):
    if key_file_path and not json_api_key:
        with open(path, 'rb') as _file:
            json_api_key = _file.read().decode('utf8')
    if json_api_key:
        project_credentials = get_analytics_project_credentials(secrets=json.loads(json_string))
    else:
        project_credentials = None
    return build_analytics_resources(analytics_project_credentials=project_credentials)


def get_analytics_project_credentials(secrets: dict):
    analytics_project_scopes = ['https://www.googleapis.com/auth/analytics.readonly',
                                'https://www.googleapis.com/auth/webmasters.readonly',
                                'https://www.googleapis.com/auth/webmasters',
                                'https://www.googleapis.com/auth/cloud-platform']

    project_credentials = service_account.Credentials.from_service_account_info(
        info=secrets).with_scopes(scopes=analytics_project_scopes)
    return project_credentials


def build_analytics_resources(analytics_project_credentials: service_account.Credentials = None):
    ga3_resource = discovery.build('analyticsreporting', 'v4', credentials=analytics_project_credentials)
    gsc_resource = discovery.build('searchconsole', 'v1', credentials=analytics_project_credentials)
    ga4_data_client = BetaAnalyticsDataClient(credentials=analytics_project_credentials)
    return ga3_resource, ga4_data_client, gsc_resource
