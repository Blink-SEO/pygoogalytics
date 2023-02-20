import logging

from .resource_utils import resources_from_json, resources_from_key_file
from .wrapper import GoogalyticsWrapper


class Client:
    def __init__(self,
                 gsc_resource=None,
                 ga3_resource=None,
                 ga4_data_client=None,
                 logging_handler_console: logging.FileHandler = None,
                 logging_handler_error_log: logging.FileHandler = None
                 ):

        self.gsc_resource = gsc_resource
        self.ga3_resource = ga3_resource
        self.ga4_data_client = ga4_data_client

        self.logging_handler_console = logging_handler_console
        if logging_handler_error_log:
            self.logging_handler_error_log = logging_handler_error_log
        else:
            self.logging_handler_error_log = logging_handler_console

        self.logger = logging.getLogger("gapi_wrapper")
        if logging_handler_console:
            self.logger.addHandler(self.logging_handler_console)
        if logging_handler_error_log:
            self.logger.addHandler(self.logging_handler_error_log)
        self.logger.debug(f"{self.__class__.__name__}.__init__() :: "
                          f"initialising Client object from PyGoogalytics.client")

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
            ga4_property_id=ga4_property_id,
            logging_handler_console=self.logging_handler_console,
            logging_handler_error_log=self.logging_handler_error_log
        )


def client_from_json(json_string: str,
                     logging_handler_console: logging.FileHandler = None,
                     logging_handler_error_log: logging.FileHandler = None
                     ) -> Client:
    c = Client(logging_handler_console=logging_handler_console,
               logging_handler_error_log=logging_handler_error_log)
    c.build_from_json(json_string=json_string)
    return c


def client_from_key_file(path: str,
                         logging_handler_console: logging.FileHandler = None,
                         logging_handler_error_log: logging.FileHandler = None
                         ) -> Client:
    c = Client(logging_handler_console=logging_handler_console,
               logging_handler_error_log=logging_handler_error_log)
    c.build_from_key_file(path=path)
    return c
