<div id="badges">
   <img src="https://img.shields.io/badge/3.10+-yellow?label=python&style=plastic&logo=python&logoColor=yellow" alt="Python version: 3.10 | 3.11">
   <img src="https://img.shields.io/badge/UA (GA3) | GA4 | GSC | GoogleAds-green?label=API support&style=plastic&logo=googlecloud&logoColor=green" alt="Application support: GA3 | GSC | GoogleAds | SemRush | ScreamingFrog">
   <img src="https://img.shields.io/badge/beta-red?label=stage&style=plastic&logoColor=blue" alt="Stage: beta">
</div>

### Installation

PyGoogalytics can be installed using [pip](https://pypi.org/project/pygoogalytics/):
```shell
pip install -U pygoogalytics
```

### Set-up

Although PyGoogalytics is intended to be user-friendly it is still 
necessary to set up various accounts and credentials with Google. 
If you haven't yet setup API access for Universal Analytics, GA4, Search Console or Google Ads, follow [our instructions](setup_instruction.md) to get started.


# Google Analytics API wrapper

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


## Advantages of PyGoogalytics

1. **Simple.** When doing SEO exploration using python we really want to use a Pandas dataframe. We can either 
download lots of CSV files separately then read them in, or we can use the API. The first is a little fiddly and can
mean you're not getting the full picture: the GSC web app, for example, has a maximum number of rows you can download
so you won't see all those long-tail case. The API option can be difficult to interpret for a beginner programmer and 
comes with its own caveats. PyGoogalytics is a user-friendly wrapper for the API that does GSC and GA3 in one, 
it returns a Pandas dataframe without any faff about pagination. Having a dataframe allows you to use all the familiar 
`sort_values`, `groupby`, etc. to analyse your data.
2. **Compatible.** Pygoogalytics provides a child-class of Pandas (GooglePandas), which interprets the responses from
the GSC and GA3 APIs. This also standardises column names (all snake_case) so that they match between GA and GSC. It
also converts the 2-character country ISO codes (e.g. 'US') used by GA3 into 3-character codes (e.g. 'USA') as used by GSC,
to make it easy to join or merge the two. In this way you could get a dataframe for `country_iso_code`, `landing_page`,
`ctr` and `position` from GSC, and another from GA3 with `country_iso_code`, `landing_page` and `transaction_revenue`,
then join on the dimensions (`country_iso_code`, `landing_page`) to get a single data frame with data from both GA3 and GSC.
3. **More metrics.** When using the API for GA3 (UA) data, you are allowed to request only 10 metrics at once. 
The PyGoogalytics wrapper allows the passing of any number of compatible dimensions and metrics:
when more than ten metrics are passed, the list is partitioned into sub-lists of length 10
and separate API calls are made for each partition, the resulting dataframes are then joined 
on the dimensions to create a seamless dataframe with all requested metrics.
4. **More data.** When using the GSC or GA web applications, the data you can output is severely restricted, and
relies on downloading multiple CSV files. Going the API route is better for small sites, but both the Search Console 
and GA3 APIs can return a maximum of 100k rows of data, so even if you request one day's data at a time, you might 
run up to the limit for larger sites and have to paginate your requests. The PyGoogalytics wrapper automatically 
paginates the requests and concatenates the results to return a single dataframe of arbitrary length. 

## Google Pandas

`googlepandas.py` provides the classes `GADataFrame` and `GSCDataFrame`, 
both children of pandas `DataFrame` specific to storing 
GA and GSC data with additional metadata fields 
(e.g. `dimensions` and `metrics`) and methods for adding columns 
and filtering by particular metrics.
