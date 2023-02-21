from .resource_utils import resources_from_json, resources_from_key_file
from .wrapper import GoogalyticsWrapper
from . import pga_logger


class Client:
    """
    The Client class holds the credentials for a project
    which can then be used to create a GoogalyticsWrapper object.

    example implementation:
    from pygoogalytics.client import Client
    googalytics_client = Client(key_file_path='<path-to-your-key-file>')
    """
    def __init__(self,
                 key_file_path: str = None,
                 key_file_json: str = None
                 ):

        self.gsc_resource = None
        self.ga3_resource = None
        self.ga4_resource = None

        if key_file_json:
            self._build_from_json(json_string=key_file_json)
            pga_logger.info(f"initialised PyGoogalytics Client object from key file")
        elif key_file_path:
            self._build_from_key_file(path=key_file_path)
            pga_logger.info(f"initialised PyGoogalytics Client object from JSON string")

    def _build_from_json(self, json_string: str):
        _ga3_resource, _ga4_resource, _gsc_resource = resources_from_json(json_string=json_string)
        self.gsc_resource = _gsc_resource
        self.ga3_resource = _ga3_resource
        self.ga4_resource = _ga4_resource

    def _build_from_key_file(self, path: str):
        _ga3_resource, _ga4_resource, _gsc_resource = resources_from_key_file(path=path)
        self.gsc_resource = _gsc_resource
        self.ga3_resource = _ga3_resource
        self.ga4_resource = _ga4_resource

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