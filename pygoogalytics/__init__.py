"""
PyGoogalytics allows a user to quickly and simply download Google Analytics and Google Search Console data
in the form of a pandas dataframe.

To start, you must first create a service account and save the JSON key file locally:
https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py#1_enable_the_api
Also, give the service account email address access (at least "read" level) to the Search Console or Google Analytics
accounts you want to access. Now you can use the PyGoogalytics API wrapper.

Follow the implementation example below:

```
from pygoogalytics.client import Client
googalytics_client = Client(key_file_path='<path-to-your-key-file>')
g_wrapper = googalytics_client.wrapper(
  sc_domain='<search-console-domain>',
  view_id='<ga3-view-id>',
  ga4_property_id='<ga4-property-id>'
)
```
"""

__version__ = "0.3.0"
__author__ = 'Joshua Prettyman'
__credits__ = 'Blink SEO'

import os
import csv
import logging

pga_logger = logging.getLogger(__name__)


with open(os.path.join(os.path.dirname(__file__), 'data', 'google_ads_location_ids.csv'), mode='r') as infile:
    reader = csv.reader(infile)
    LOCATION_ID_DICT = {rows[1]: rows[2] for rows in reader}


