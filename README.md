# - Google Analytics API wrapper

`client.py` defines the class `Client` which builds `googleapiclient.discovery` resources for accessing 
Google Search Console and Google Analytics 3 (UA) data from the respective APIs, 
and also `google.analytics.data_v1beta.BetaAnalyticsDataClient` resource for accessing GA4 data, although 
currently this is only for testing.

Before using this python package you must [create a service account and download a JSON key file](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py#1_enable_the_api)
following the instructions from Google. This process will also give you a service account email address,
you need to add this email address to the user list for the Analytics or Search Console account for which you 
want to obtain data — you only need to give the service account "read" access. 

A `Client` class can create a `GoogalyticsWrapper` object which has methods to access data 
the Google services and return a pandas dataframe. 

A typical implementation will look like:
```python
from pygoogalytics.client import Client

googalytics_client = Client(key_file_path='<path-to-your-key-file>')
g_wrapper = googalytics_client.wrapper(
  sc_domain='<search-console-domain>', 
  view_id='<ga3-view-id>', 
  ga4_property_id='<ga4-property-id>'
)

ga3_dataframe = g_wrapper.get_df(
  result='GA3', 
  start_date='2023-01-01', 
  end_date='2023-01-07', 
  metrics=['ga:itemRevenue', 'ga:itemQuantity', 'ga:users'],
  dimensions=['ga:dateHourMinute', 'ga:landingPagePath', 'ga:sourceMedium', 'ga:countryIsoCode']
)
```

The `get_df` method accepts the following values for the `result` argument:
- "GSC": for Google Search Console data
- "GA3": for Google Analytics 3 (UA) data
- "URL": for Google Search Console URL inspection data
- "GA4": for Google Analytics 4 data (note, this is not yet available in production)


## - Google Pandas

`googlepandas.py` provides the classes `GADataFrame` and `GSCDataFrame`, 
both children of pandas `DataFrame` specific to storing 
GA and GSC data with additional metadata fields 
(e.g. `dimensions` and `metrics`) and methods for adding columns 
and filtering by particular metrics.
