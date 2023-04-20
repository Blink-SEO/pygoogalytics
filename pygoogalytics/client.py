from typing import List

from .resource_utils import get_analytics_resources, \
    googleads_client_from_yaml, googleads_client_from_key_file
from .googalytics_wrapper import GoogalyticsWrapper
from .kwp_wrappers import KeywordPlanIdeaService, KeywordPlanService
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

    @classmethod
    def build(cls, api_key: str = None, key_file_path: str = None):
        _ga3_resource, _ga4_resource, _gsc_resource = get_analytics_resources(
            json_api_key=api_key, key_file_path=key_file_path
        )
        return cls(gsc_resource=_gsc_resource, ga3_resource=_ga3_resource,  ga4_resource=_ga4_resource)

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
            sc_domain=sc_domain,
            view_id=view_id,
            ga4_property_id=ga4_property_id
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


class KwpClient:
    def __init__(self,
                 key_file_yaml: str = None,
                 key_file_path: str = None
                 ):
        self.googleads_client = None
        self.default_customer_id = None

        if key_file_yaml:
            self._build_from_yaml(yaml_string=key_file_yaml)
        elif key_file_path:
            self._build_from_key_file(path=key_file_path)

    def _build_from_yaml(self, yaml_string: str):
        _googleads_client, _default_customer_id = googleads_client_from_yaml(googleads_yaml_string=yaml_string)
        self.googleads_client = _googleads_client
        self.default_customer_id = _default_customer_id
        pga_logger.info(f"initialised KwpClient object from yaml string")

    def _build_from_key_file(self, path: str):
        _googleads_client, _default_customer_id = googleads_client_from_key_file(path=path)
        self.googleads_client = _googleads_client
        self.default_customer_id = _default_customer_id
        pga_logger.info(f"initialised KwpClient object from key file")

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
                                  customer_id=customer_id,
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
                                      customer_id=customer_id,
                                      site_url=site_url,
                                      location_codes=location_codes,
                                      language_id=language_id)
