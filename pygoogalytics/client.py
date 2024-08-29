from typing import List

from google.ads.googleads.client import GoogleAdsClient

from pygoogalytics.utils.resource_utils import get_analytics_resources, \
    googleads_client_from_yaml, googleads_client_from_key_file, parse_ads_id, get_analytics_resources_oauth
from .googalytics_wrapper import GoogalyticsWrapper
from .kwp_wrappers import KeywordPlanIdeaService, KeywordPlanService
from .ads_wrapper import AdsWrapper
from . import pga_logger


class GoogalyticsClient:
    """
    The Client class holds the credentials for a project
    which can then be used to create a GoogalyticsWrapper object.

    example implementation:
    from pygoogalytics.client import Client
    googalytics_client = Client(key_file_path='<path-to-your-key-file>')
    """

    def __init__(self,
                 gsc_resource=None,
                 ga3_resource=None,
                 ga4_resource=None
                 ):

        self.gsc_resource = gsc_resource
        self.ga3_resource = ga3_resource
        self.ga4_resource = ga4_resource

        self.sc_domain = None
        self.view_id = None
        self.ga4_property_id = None

    @classmethod
    def build(cls, api_key: str | bytes | dict = None, key_file_path: str = None):
        _ga3_resource, _ga4_resource, _gsc_resource = get_analytics_resources(
            json_api_key=api_key, key_file_path=key_file_path
        )
        return cls(gsc_resource=_gsc_resource, ga3_resource=_ga3_resource,  ga4_resource=_ga4_resource)

    @classmethod
    def build_oauth(cls, config: str | bytes | dict, client_id: str, client_secret: str):
        if isinstance(config, (bytes, str)):
            config = json.loads(config)
        if not isinstance(config, dict):
            raise TypeError("config must be a dict or json str")

        if "google" in config.keys():
            config = config.get("google")

        if not "oauth" in config.keys():
            raise ValueError("config must contain 'oauth' key")

        _ga3_resource, _ga4_resource, _gsc_resource = get_analytics_resources_oauth(
            oauth_config=config.get("oauth"), client_id=client_id, client_secret=client_secret
        )

        client = cls(gsc_resource=_gsc_resource, ga3_resource=_ga3_resource,  ga4_resource=_ga4_resource)

        client.sc_domain = config.get("searchConsole", {}).get("siteUrl")
        client.ga4_property_id = config.get("analytics4", {}).get("propertyId")

        return client

    def wrapper(self,
                sc_domain: str = None,
                view_id: str = None,
                ga4_property_id: str = None,
                ) -> GoogalyticsWrapper:
        """
        The GoogalyticsWrapper requires different arguments to access data depending on the source.
        @param sc_domain: required for GSC data. This is the url-like string you see in the Google Search Console web application when selecting the site. It is either a full url (e.g. `https://www.example.com/`) or something like `sc_domain:example.com`
        @param view_id: required for GA3 data: the "view_id" you see in "settings" on the GA web application. This is usually an 8- or 9-digit number, passed as a string
        @param ga4_property_id: required for GA4 data. Note that GA4 is currently not fully supported by PyGoogalytics
        @returns: GoogalyticsWrapper object.
        """
        return GoogalyticsWrapper(
            gsc_resource=self.gsc_resource,
            ga3_resource=self.ga3_resource,
            ga4_resource=self.ga4_resource,
            sc_domain=sc_domain or self.sc_domain,
            view_id=view_id or self.view_id,
            ga4_property_id=ga4_property_id or self.ga4_property_id
        )

    def __bool__(self):
        if self.gsc_resource is not None or self.ga3_resource is not None or self.ga4_resource is not None:
            return True
        else:
            return False

    def __dict__(self):
        return {
            'gsc_resource': self.gsc_resource.__repr__(),
            'ga3_resource': self.ga3_resource.__repr__(),
            'ga4_resource': self.ga4_resource.__repr__()
        }

    def __repr__(self):
        _s = 'PyGoogalytics Client object:\n'
        for _k, _v in self.__dict__().items():
            _s += f" - {_k}: {_v}\n"
        return _s


class AdsClient:
    def __init__(self,
                 googleads_client: GoogleAdsClient,
                 default_customer_id: str = None,
                 googleads_yaml_dict: dict = None):
        self.googleads_client = googleads_client
        self.default_customer_id = default_customer_id
        self.googleads_yaml_dict = googleads_yaml_dict

    @classmethod
    def build(cls, yaml_key: str = None, key_file_path: str = None):
        if yaml_key:
            googleads_client, default_customer_id, googleads_yaml_dict = googleads_client_from_yaml(googleads_yaml_string=yaml_key)
            pga_logger.info(f"initialised KwpClient object from yaml string")
        elif key_file_path:
            googleads_client, default_customer_id, googleads_yaml_dict = googleads_client_from_key_file(path=key_file_path)
            pga_logger.info(f"initialised KwpClient object from key file")
        else:
            raise KeyError("either yaml_key or key_file_path must be supplied")

        return cls(googleads_client=googleads_client,
                   default_customer_id=default_customer_id,
                   googleads_yaml_dict=googleads_yaml_dict)

    def __bool__(self):
        if self.googleads_client is not None:
            return True
        else:
            return False

    def plan_service(self,
                     customer_id: str = None,
                     location_codes: List[str] = None,
                     language_id: str = None
                     ):
        if customer_id is None:
            customer_id = self.default_customer_id

        return KeywordPlanService(googleads_client=self.googleads_client,
                                  customer_id=parse_ads_id(customer_id),
                                  location_codes=location_codes,
                                  language_id=language_id)

    def ideas_service(self,
                      customer_id: str = None,
                      site_url: str = None,
                      location_codes: List[str] = None,
                      language_id: str = None
                      ):
        if customer_id is None:
            customer_id = self.default_customer_id

        return KeywordPlanIdeaService(googleads_client=self.googleads_client,
                                      customer_id=parse_ads_id(customer_id),
                                      site_url=site_url,
                                      location_codes=location_codes,
                                      language_id=language_id)

    def report_service(self, customer_id: str):
        if customer_id is None:
            customer_id = self.default_customer_id
        return AdsWrapper(googleads_client=self.googleads_client,
                          customer_id=parse_ads_id(customer_id))


