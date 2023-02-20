from .resource_utils import resources_from_json, resources_from_key_file
from .wrapper import GoogalyticsWrapper
from . import pga_logger


class Client:
    def __init__(self,
                 gsc_resource=None,
                 ga3_resource=None,
                 ga4_data_client=None,
                 ):

        self.gsc_resource = gsc_resource
        self.ga3_resource = ga3_resource
        self.ga4_data_client = ga4_data_client

        pga_logger.info(f"initialising Client object from PyGoogalytics.client")

    def build_from_json(self, json_string: str):
        _ga3_resource, _ga4_data_client, _gsc_resource = resources_from_json(json_string=json_string)
        self.gsc_resource = _gsc_resource
        self.ga3_resource = _ga3_resource
        self.ga4_data_client = _ga4_data_client

    def build_from_key_file(self, path: str):
        _ga3_resource, _ga4_data_client, _gsc_resource = resources_from_key_file(path=path)
        self.gsc_resource = _gsc_resource
        self.ga3_resource = _ga3_resource
        self.ga4_data_client = _ga4_data_client

    def wrapper(self,
                sc_domain: str = None,
                view_id: str = None,
                ga4_property_id: str = None,
                ) -> GoogalyticsWrapper:
        return GoogalyticsWrapper(
            gsc_resource=self.gsc_resource,
            ga3_resource=self.ga3_resource,
            ga4_data_client=self.ga4_data_client,
            sc_domain=sc_domain,
            view_id=view_id,
            ga4_property_id=ga4_property_id
        )


def client_from_json(json_string: str) -> Client:
    c = Client()
    c.build_from_json(json_string=json_string)
    return c


def client_from_key_file(path: str) -> Client:
    c = Client()
    c.build_from_key_file(path=path)
    return c
