import json
import yaml
import re

from typing import Tuple

from google.oauth2 import service_account, credentials  # pip install --upgrade google-auth

from googleapiclient import discovery  # pip install --upgrade google-api-python-client
from google.analytics.data_v1beta import BetaAnalyticsDataClient  # pip install google-analytics-data
from google.ads.googleads.client import GoogleAdsClient

_default_googleads_api_version = 15

_analytics_project_scopes = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/webmasters.readonly',
    'https://www.googleapis.com/auth/webmasters',
    'https://www.googleapis.com/auth/cloud-platform'
]

def googleads_client_from_key_file(path: str):
    with open(path, 'rb') as _file:
        googleads_yaml_string = _file.read().decode('utf8')
    return googleads_client_from_yaml(googleads_yaml_string=googleads_yaml_string)


def googleads_client_from_yaml(googleads_yaml_string: str) -> Tuple[GoogleAdsClient, str, dict]:
    googleads_yaml_dict = yaml.safe_load(googleads_yaml_string)
    default_customer_id = parse_ads_id(googleads_yaml_dict.get('default_customer_id', ''))
    api_version = parse_api_version(googleads_yaml_dict.get('api_version', _default_googleads_api_version))
    googleads_client = GoogleAdsClient.load_from_string(yaml_str=googleads_yaml_string, version=api_version)
    return googleads_client, default_customer_id, googleads_yaml_dict


def parse_ads_id(customer_id: str | int) -> str:
    if isinstance(customer_id, int):
        customer_id = str(customer_id)
    return re.sub(r'[^\d]', '', customer_id)


def parse_api_version(api_version: str | int) -> str:
    if isinstance(api_version, int):
        _api_version = "v" + str(api_version)
    elif isinstance(api_version, str) and len(api_version) > 0:
        if default_api_version[0] == 'v':
            _api_version = api_version
        else:
            _api_version = "v" + api_version
    else:
        _api_version = "v15"
    return _api_version


def get_analytics_resources(json_api_key: str | bytes | dict = None, key_file_path: str = None):
    if key_file_path and not json_api_key:
        with open(key_file_path, 'rb') as _file:
            json_api_key = _file.read().decode('utf8')

    if isinstance(json_api_key, (bytes, str)):
        secrets = json.loads(json_api_key)
    elif isinstance(json_api_key, dict):
        secrets = json_api_key
    else:
        secrets = None

    if secrets:
        project_credentials = service_account.Credentials.from_service_account_info(
            info=secrets
        ).with_scopes(
            scopes=_analytics_project_scopes
        )
    else:
        project_credentials = None

    ga3_resource = build_resource("GA3", project_credentials)
    ga4_resource = build_resource("GA4", project_credentials)
    gsc_resource = build_resource("GSC", project_credentials)

    return ga3_resource, ga4_resource, gsc_resource

def get_analytics_resources_oauth(oauth_config: dict, client_id: str, client_secret: str):
    project_credentials = credentials.Credentials(
        token=oauth_config.get("accessToken"),
        refresh_token=oauth_config.get("refreshToken"),
        token_uri='https://oauth2.googleapis.com/token',
        client_id=client_id,
        client_secret=client_secret,
        # scopes=_analytics_project_scopes  # Scopes are not needed when using oauth
    )

    ga3_resource = build_resource("GA3", project_credentials)
    ga4_resource = build_resource("GA4", project_credentials)
    gsc_resource = build_resource("GSC", project_credentials)

    return ga3_resource, ga4_resource, gsc_resource


def build_resource(_type: str, creds: service_account.Credentials = None):
    if _type == "GA3":
        return discovery.build('analyticsreporting', 'v4', credentials=creds)
    elif _type == "GSC":
        return discovery.build('searchconsole', 'v1', credentials=creds)
    elif _type == "GA4":
        return BetaAnalyticsDataClient(credentials=creds)
    else:
        raise ValueError("Unsupported resource type")
